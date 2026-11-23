using System.Buffers;
using System.Buffers.Binary;

namespace MqttZero.Protocol;

internal static class MqttPublishPacketCodec
{
    private const byte MessageExpiryIntervalId = 0x02;
    private const byte SubscriptionIdentifierId = 0x0B;
    private const byte TopicAliasId = 0x23;

    public static bool TryDecode(
        in ReadOnlySequence<byte> sequence,
        out MqttZeroPublishView view,
        out long consumed)
    {
        view = default;
        consumed = 0;
        if (sequence.IsEmpty)
        {
            return false;
        }

        SequenceReader<byte> reader = new(sequence);
        if (!reader.TryRead(out byte firstByte) || (firstByte >> 4) != (byte)MqttPacketType.Publish)
        {
            return false;
        }

        if (!TryReadVariableByteInteger(ref reader, out int remainingLength) ||
            reader.Remaining < remainingLength)
        {
            return false;
        }

        ReadOnlySequence<byte> body = sequence.Slice(reader.Position, remainingLength);
        SequenceReader<byte> bodyReader = new(body);
        if (!TryReadTopic(ref bodyReader, out ReadOnlySequence<byte> topic))
        {
            return false;
        }

        MqttQualityOfServiceLevel qos = (MqttQualityOfServiceLevel)((firstByte >> 1) & 0x03);
        if (qos == MqttQualityOfServiceLevel.ExactlyOnce)
        {
            return false;
        }

        ushort packetIdentifier = 0;
        if (qos > MqttQualityOfServiceLevel.AtMostOnce &&
            !TryReadUInt16(ref bodyReader, out packetIdentifier))
        {
            return false;
        }

        if (!TryReadProperties(
            ref bodyReader,
            out ReadOnlySequence<byte> properties,
            out uint? messageExpiryInterval,
            out uint? subscriptionIdentifier,
            out ushort? topicAlias))
        {
            return false;
        }

        ReadOnlySequence<byte> payload = bodyReader.Sequence.Slice(bodyReader.Position);
        view = new MqttZeroPublishView(
            topic,
            properties,
            payload,
            qos,
            packetIdentifier,
            (firstByte & 0x08) != 0,
            (firstByte & 0x01) != 0,
            messageExpiryInterval,
            subscriptionIdentifier,
            topicAlias);
        consumed = reader.Consumed + remainingLength;
        return true;
    }

    public static void Encode(in MqttZeroPublishView view, IBufferWriter<byte> writer)
    {
        int bodyLength = GetBodyLength(view);
        int propertyLength = GetPropertyLength(view);

        Span<byte> fixedHeader = writer.GetSpan(5);
        byte firstByte = 0x30;
        if (view.Duplicate)
        {
            firstByte |= 0x08;
        }

        firstByte |= (byte)((byte)view.QualityOfServiceLevel << 1);
        if (view.Retain)
        {
            firstByte |= 0x01;
        }

        fixedHeader[0] = firstByte;
        int fixedHeaderLength = 1 + WriteVariableByteInteger(fixedHeader[1..], bodyLength);
        writer.Advance(fixedHeaderLength);
        WriteUInt16(writer, (ushort)view.Topic.Length);
        WriteSequence(writer, view.Topic);
        if (view.QualityOfServiceLevel != MqttQualityOfServiceLevel.AtMostOnce)
        {
            WriteUInt16(writer, view.PacketIdentifier);
        }

        Span<byte> propertyLengthSpan = writer.GetSpan(4);
        int propertyLengthBytes = WriteVariableByteInteger(propertyLengthSpan, propertyLength);
        writer.Advance(propertyLengthBytes);
        if (view.Properties.IsEmpty)
        {
            WriteScalarProperties(writer, view);
        }
        else
        {
            WriteSequence(writer, view.Properties);
        }

        WriteSequence(writer, view.Payload);
    }

    public static int GetEncodedLength(in MqttZeroPublishView view)
    {
        int bodyLength = GetBodyLength(view);
        return 1 + GetVariableByteIntegerSize((uint)bodyLength) + bodyLength;
    }

    private static bool TryReadProperties(
        ref SequenceReader<byte> reader,
        out ReadOnlySequence<byte> properties,
        out uint? messageExpiryInterval,
        out uint? subscriptionIdentifier,
        out ushort? topicAlias)
    {
        properties = ReadOnlySequence<byte>.Empty;
        messageExpiryInterval = null;
        subscriptionIdentifier = null;
        topicAlias = null;
        if (!TryReadVariableByteInteger(ref reader, out int length) ||
            reader.Remaining < length)
        {
            return false;
        }

        if (length == 0)
        {
            return true;
        }

        properties = reader.Sequence.Slice(reader.Position, length);
        SequenceReader<byte> propertyReader = new(properties);
        while (propertyReader.Remaining > 0)
        {
            if (!propertyReader.TryRead(out byte identifier))
            {
                return false;
            }

            switch (identifier)
            {
                case MessageExpiryIntervalId:
                    if (messageExpiryInterval.HasValue ||
                        !TryReadUInt32(ref propertyReader, out uint expiry))
                    {
                        return false;
                    }

                    messageExpiryInterval = expiry;
                    break;
                case SubscriptionIdentifierId:
                    if (subscriptionIdentifier.HasValue ||
                        !TryReadVariableByteInteger(ref propertyReader, out int subscriptionId) ||
                        subscriptionId == 0)
                    {
                        return false;
                    }

                    subscriptionIdentifier = (uint)subscriptionId;
                    break;
                case TopicAliasId:
                    if (topicAlias.HasValue ||
                        !TryReadUInt16(ref propertyReader, out ushort alias) ||
                        alias == 0)
                    {
                        return false;
                    }

                    topicAlias = alias;
                    break;
                default:
                    if (!TrySkipProperty(identifier, ref propertyReader))
                    {
                        return false;
                    }

                    break;
            }
        }

        reader.Advance(length);
        return true;
    }

    private static bool TryReadTopic(ref SequenceReader<byte> reader, out ReadOnlySequence<byte> topic)
    {
        if (!TryReadUInt16(ref reader, out ushort length) || reader.Remaining < length)
        {
            topic = ReadOnlySequence<byte>.Empty;
            return false;
        }

        topic = length == 0
            ? ReadOnlySequence<byte>.Empty
            : reader.Sequence.Slice(reader.Position, length);
        reader.Advance(length);
        return true;
    }

    private static bool TryReadUInt16(ref SequenceReader<byte> reader, out ushort value)
    {
        value = 0;
        if (!reader.TryRead(out byte first) || !reader.TryRead(out byte second))
        {
            return false;
        }

        value = BinaryPrimitives.ReadUInt16BigEndian([first, second]);
        return true;
    }

    private static bool TryReadUInt32(ref SequenceReader<byte> reader, out uint value)
    {
        value = 0;
        Span<byte> bytes = stackalloc byte[4];
        for (int index = 0; index < bytes.Length; index++)
        {
            if (!reader.TryRead(out bytes[index]))
            {
                return false;
            }
        }

        value = BinaryPrimitives.ReadUInt32BigEndian(bytes);
        return true;
    }

    private static bool TryReadVariableByteInteger(ref SequenceReader<byte> reader, out int value)
    {
        value = 0;
        int multiplier = 1;
        for (int index = 0; index < 4; index++)
        {
            if (!reader.TryRead(out byte encoded))
            {
                return false;
            }

            value += (encoded & 0x7F) * multiplier;
            if ((encoded & 0x80) == 0)
            {
                return true;
            }

            multiplier *= 128;
        }

        return false;
    }

    private static bool TrySkipProperty(byte identifier, ref SequenceReader<byte> reader)
    {
        return identifier switch
        {
            0x01 or 0x17 or 0x19 or 0x24 or 0x25 or 0x28 or 0x29 or 0x2A => TryAdvance(ref reader, 1),
            0x11 or 0x18 or 0x27 => TryAdvance(ref reader, 4),
            0x13 or 0x21 or 0x22 or 0x23 => TryAdvance(ref reader, 2),
            0x09 or 0x16 => TrySkipBinary(ref reader),
            0x03 or 0x08 or 0x12 or 0x15 or 0x1A or 0x1C or 0x1F => TrySkipBinary(ref reader),
            0x26 => TrySkipBinary(ref reader) && TrySkipBinary(ref reader),
            _ => false
        };
    }

    private static bool TryAdvance(ref SequenceReader<byte> reader, long length)
    {
        if (reader.Remaining < length)
        {
            return false;
        }

        reader.Advance(length);
        return true;
    }

    private static bool TrySkipBinary(ref SequenceReader<byte> reader)
    {
        if (!TryReadUInt16(ref reader, out ushort length) || reader.Remaining < length)
        {
            return false;
        }

        reader.Advance(length);
        return true;
    }

    private static int GetScalarPropertyLength(in MqttZeroPublishView view)
    {
        int length = 0;
        if (view.MessageExpiryInterval.HasValue)
        {
            length += 5;
        }

        if (view.SubscriptionIdentifier.HasValue)
        {
            length += 1 + GetVariableByteIntegerSize(view.SubscriptionIdentifier.Value);
        }

        if (view.TopicAlias.HasValue)
        {
            length += 3;
        }

        return length;
    }

    private static int GetBodyLength(in MqttZeroPublishView view)
    {
        int propertyLength = GetPropertyLength(view);
        return
            2 +
            (int)view.Topic.Length +
            (view.QualityOfServiceLevel == MqttQualityOfServiceLevel.AtMostOnce ? 0 : 2) +
            GetVariableByteIntegerSize((uint)propertyLength) +
            propertyLength +
            (int)view.Payload.Length;
    }

    private static int GetPropertyLength(in MqttZeroPublishView view)
    {
        return view.Properties.IsEmpty
            ? GetScalarPropertyLength(view)
            : (int)view.Properties.Length;
    }

    private static void WriteScalarProperties(IBufferWriter<byte> writer, in MqttZeroPublishView view)
    {
        if (view.MessageExpiryInterval.HasValue)
        {
            WriteByte(writer, MessageExpiryIntervalId);
            WriteUInt32(writer, view.MessageExpiryInterval.Value);
        }

        if (view.SubscriptionIdentifier.HasValue)
        {
            WriteByte(writer, SubscriptionIdentifierId);
            WriteVariableByteInteger(writer, view.SubscriptionIdentifier.Value);
        }

        if (view.TopicAlias.HasValue)
        {
            WriteByte(writer, TopicAliasId);
            WriteUInt16(writer, view.TopicAlias.Value);
        }
    }

    private static void WriteSequence(IBufferWriter<byte> writer, ReadOnlySequence<byte> value)
    {
        foreach (ReadOnlyMemory<byte> segment in value)
        {
            Span<byte> destination = writer.GetSpan(segment.Length);
            segment.Span.CopyTo(destination);
            writer.Advance(segment.Length);
        }
    }

    private static void WriteByte(IBufferWriter<byte> writer, byte value)
    {
        Span<byte> destination = writer.GetSpan(1);
        destination[0] = value;
        writer.Advance(1);
    }

    private static void WriteUInt16(IBufferWriter<byte> writer, ushort value)
    {
        Span<byte> destination = writer.GetSpan(2);
        BinaryPrimitives.WriteUInt16BigEndian(destination, value);
        writer.Advance(2);
    }

    private static void WriteUInt32(IBufferWriter<byte> writer, uint value)
    {
        Span<byte> destination = writer.GetSpan(4);
        BinaryPrimitives.WriteUInt32BigEndian(destination, value);
        writer.Advance(4);
    }

    private static int WriteVariableByteInteger(Span<byte> destination, int value)
    {
        int index = 0;
        do
        {
            byte encoded = (byte)(value % 128);
            value /= 128;
            if (value > 0)
            {
                encoded |= 0x80;
            }

            destination[index++] = encoded;
        }
        while (value > 0);

        return index;
    }

    private static void WriteVariableByteInteger(IBufferWriter<byte> writer, uint value)
    {
        Span<byte> destination = writer.GetSpan(4);
        int length = WriteVariableByteInteger(destination, (int)value);
        writer.Advance(length);
    }

    private static int GetVariableByteIntegerSize(uint value)
    {
        if (value < 128)
        {
            return 1;
        }

        if (value < 16_384)
        {
            return 2;
        }

        if (value < 2_097_152)
        {
            return 3;
        }

        return 4;
    }
}
