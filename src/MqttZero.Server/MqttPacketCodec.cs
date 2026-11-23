using System.Buffers;
using System.Buffers.Binary;

namespace MqttZero;

/// <summary>
/// Encodes and decodes the MQTT 5 packets required by the broker.
/// </summary>
public static class MqttPacketCodec
{
    /// <summary>
    /// Attempts to decode one MQTT packet from a sequence.
    /// </summary>
    /// <param name="sequence">The input byte sequence.</param>
    /// <param name="packet">The decoded packet when successful.</param>
    /// <param name="consumed">The number of bytes consumed by the packet.</param>
    /// <returns><see langword="true"/> when a complete supported packet was decoded.</returns>
    public static bool TryDecode(
        in ReadOnlySequence<byte> sequence,
        out MqttPacket? packet,
        out long consumed)
    {
        packet = null;
        consumed = 0;

        SequenceReader<byte> reader = new(sequence);
        if (!reader.TryRead(out byte firstByte))
        {
            return false;
        }

        if (!TryReadVariableByteInteger(ref reader, out int remainingLength) || remainingLength < 0)
        {
            return false;
        }

        if (reader.Remaining < remainingLength)
        {
            return false;
        }

        ReadOnlySequence<byte> body = sequence.Slice(reader.Position, remainingLength);
        SequenceReader<byte> bodyReader = new(body);
        MqttPacketType packetType = (MqttPacketType)(firstByte >> 4);
        bool success = packetType switch
        {
            MqttPacketType.Connect => TryDecodeConnect(firstByte, ref bodyReader, out packet),
            MqttPacketType.Publish => TryDecodePublish(firstByte, ref bodyReader, out packet),
            _ => false
        };

        if (!success)
        {
            packet = null;
            return false;
        }

        consumed = reader.Consumed + remainingLength;
        return true;
    }

    /// <summary>
    /// Encodes one MQTT packet into a buffer writer.
    /// </summary>
    /// <param name="packet">The packet to encode.</param>
    /// <param name="writer">The destination buffer writer.</param>
    /// <exception cref="ArgumentNullException">Thrown when arguments are null.</exception>
    /// <exception cref="NotSupportedException">Thrown for unsupported packet types.</exception>
    public static void Encode(MqttPacket packet, IBufferWriter<byte> writer)
    {
        ArgumentNullException.ThrowIfNull(packet);
        ArgumentNullException.ThrowIfNull(writer);

        switch (packet)
        {
            case MqttConnectPacket connectPacket:
                EncodeConnect(connectPacket, writer);
                break;
            case MqttPublishPacket publishPacket:
                EncodePublish(publishPacket, writer);
                break;
            default:
                throw new NotSupportedException($"Packet type '{packet.PacketType}' is not supported.");
        }
    }

    private static bool TryDecodeConnect(
        byte firstByte,
        ref SequenceReader<byte> reader,
        out MqttPacket? packet)
    {
        packet = null;
        if ((firstByte & 0x0F) != 0 ||
            !TryReadUtf8(ref reader, out ReadOnlySequence<byte> protocolName) ||
            protocolName.Length != 4 ||
            !protocolName.FirstSpan.SequenceEqual("MQTT"u8) ||
            !reader.TryRead(out byte protocolVersion) ||
            protocolVersion != 5 ||
            !reader.TryRead(out byte connectFlags) ||
            (connectFlags & 0x01) != 0 ||
            !TryReadUInt16(ref reader, out ushort keepAliveSeconds) ||
            !TryReadProperties(ref reader, out ReadOnlySequence<byte> properties) ||
            !TryReadUtf8(ref reader, out ReadOnlySequence<byte> clientId))
        {
            return false;
        }

        bool hasUserName = (connectFlags & 0x80) != 0;
        bool hasPassword = (connectFlags & 0x40) != 0;
        bool willRetain = (connectFlags & 0x20) != 0;
        MqttQualityOfServiceLevel willQos = (MqttQualityOfServiceLevel)((connectFlags >> 3) & 0x03);
        bool hasWill = (connectFlags & 0x04) != 0;

        if ((!hasWill && (willRetain || willQos != MqttQualityOfServiceLevel.AtMostOnce)) ||
            willQos == MqttQualityOfServiceLevel.ExactlyOnce)
        {
            return false;
        }

        ReadOnlySequence<byte> willProperties = ReadOnlySequence<byte>.Empty;
        ReadOnlySequence<byte> willTopic = ReadOnlySequence<byte>.Empty;
        ReadOnlySequence<byte> willPayload = ReadOnlySequence<byte>.Empty;
        if (hasWill)
        {
            if (!TryReadProperties(ref reader, out willProperties) ||
                !TryReadUtf8(ref reader, out willTopic) ||
                !TryReadBinary(ref reader, out willPayload))
            {
                return false;
            }
        }

        ReadOnlySequence<byte> userName = ReadOnlySequence<byte>.Empty;
        if (hasUserName && !TryReadUtf8(ref reader, out userName))
        {
            return false;
        }

        ReadOnlySequence<byte> password = ReadOnlySequence<byte>.Empty;
        if (hasPassword && !TryReadBinary(ref reader, out password))
        {
            return false;
        }

        packet = new MqttConnectPacket
        {
            ProtocolVersion = protocolVersion,
            CleanStart = (connectFlags & 0x02) != 0,
            KeepAliveSeconds = keepAliveSeconds,
            ClientId = clientId,
            Properties = properties,
            HasUserName = hasUserName,
            UserName = userName,
            HasPassword = hasPassword,
            Password = password,
            HasWill = hasWill,
            WillQualityOfServiceLevel = willQos,
            WillRetain = willRetain,
            WillProperties = willProperties,
            WillTopic = willTopic,
            WillPayload = willPayload
        };

        return reader.Remaining == 0;
    }

    private static bool TryDecodePublish(
        byte firstByte,
        ref SequenceReader<byte> reader,
        out MqttPacket? packet)
    {
        packet = null;
        MqttQualityOfServiceLevel qos = (MqttQualityOfServiceLevel)((firstByte >> 1) & 0x03);
        if (qos == MqttQualityOfServiceLevel.ExactlyOnce)
        {
            return false;
        }

        if (!TryReadUtf8(ref reader, out ReadOnlySequence<byte> topicName))
        {
            return false;
        }

        ushort packetIdentifier = 0;
        if (qos > MqttQualityOfServiceLevel.AtMostOnce &&
            !TryReadUInt16(ref reader, out packetIdentifier))
        {
            return false;
        }

        if (!TryReadProperties(ref reader, out ReadOnlySequence<byte> properties))
        {
            return false;
        }

        ReadOnlySequence<byte> payload = reader.Sequence.Slice(reader.Position);
        reader.Advance(reader.Remaining);
        packet = new MqttPublishPacket
        {
            TopicName = topicName,
            Properties = properties,
            Payload = payload,
            QualityOfServiceLevel = qos,
            PacketIdentifier = packetIdentifier,
            Duplicate = (firstByte & 0x08) != 0,
            Retain = (firstByte & 0x01) != 0
        };

        return true;
    }

    private static void EncodeConnect(MqttConnectPacket packet, IBufferWriter<byte> writer)
    {
        int bodyLength = 10 +
            GetVariableByteIntegerSize(packet.Properties.Length) +
            (int)packet.Properties.Length +
            2 +
            (int)packet.ClientId.Length;

        if (packet.HasWill)
        {
            bodyLength += GetVariableByteIntegerSize(packet.WillProperties.Length) +
                (int)packet.WillProperties.Length +
                2 +
                (int)packet.WillTopic.Length +
                2 +
                (int)packet.WillPayload.Length;
        }

        if (packet.HasUserName)
        {
            bodyLength += 2 + (int)packet.UserName.Length;
        }

        if (packet.HasPassword)
        {
            bodyLength += 2 + (int)packet.Password.Length;
        }

        Span<byte> fixedHeader = writer.GetSpan(1 + 4);
        fixedHeader[0] = 0x10;
        int lengthBytes = WriteVariableByteInteger(fixedHeader[1..], bodyLength);
        writer.Advance(1 + lengthBytes);

        WriteUtf8(writer, "MQTT"u8);
        WriteByte(writer, packet.ProtocolVersion);
        WriteByte(writer, BuildConnectFlags(packet));
        WriteUInt16(writer, packet.KeepAliveSeconds);
        WriteProperties(writer, packet.Properties);
        WriteUtf8(writer, packet.ClientId.IsEmpty ? ReadOnlySpan<byte>.Empty : packet.ClientId.FirstSpan);

        if (packet.HasWill)
        {
            WriteProperties(writer, packet.WillProperties);
            WriteUtf8(writer, packet.WillTopic.IsEmpty ? ReadOnlySpan<byte>.Empty : packet.WillTopic.FirstSpan);
            WriteBinary(writer, packet.WillPayload);
        }

        if (packet.HasUserName)
        {
            WriteUtf8(writer, packet.UserName.IsEmpty ? ReadOnlySpan<byte>.Empty : packet.UserName.FirstSpan);
        }

        if (packet.HasPassword)
        {
            WriteBinary(writer, packet.Password);
        }
    }

    private static void EncodePublish(MqttPublishPacket packet, IBufferWriter<byte> writer)
    {
        int bodyLength =
            2 +
            (int)packet.TopicName.Length +
            (packet.QualityOfServiceLevel == MqttQualityOfServiceLevel.AtMostOnce ? 0 : 2) +
            GetVariableByteIntegerSize(packet.Properties.Length) +
            (int)packet.Properties.Length +
            (int)packet.Payload.Length;

        Span<byte> fixedHeader = writer.GetSpan(1 + 4);
        byte firstByte = 0x30;
        if (packet.Duplicate)
        {
            firstByte |= 0x08;
        }

        firstByte |= (byte)((byte)packet.QualityOfServiceLevel << 1);
        if (packet.Retain)
        {
            firstByte |= 0x01;
        }

        fixedHeader[0] = firstByte;
        int lengthBytes = WriteVariableByteInteger(fixedHeader[1..], bodyLength);
        writer.Advance(1 + lengthBytes);

        WriteUtf8(writer, packet.TopicName.IsEmpty ? ReadOnlySpan<byte>.Empty : packet.TopicName.FirstSpan);
        if (packet.QualityOfServiceLevel != MqttQualityOfServiceLevel.AtMostOnce)
        {
            WriteUInt16(writer, packet.PacketIdentifier);
        }

        WriteProperties(writer, packet.Properties);
        WriteSequence(writer, packet.Payload);
    }

    private static byte BuildConnectFlags(MqttConnectPacket packet)
    {
        byte flags = 0;
        if (packet.HasUserName)
        {
            flags |= 0x80;
        }

        if (packet.HasPassword)
        {
            flags |= 0x40;
        }

        if (packet.HasWill)
        {
            flags |= 0x04;
            flags |= (byte)((byte)packet.WillQualityOfServiceLevel << 3);
            if (packet.WillRetain)
            {
                flags |= 0x20;
            }
        }

        if (packet.CleanStart)
        {
            flags |= 0x02;
        }

        return flags;
    }

    private static bool TryReadProperties(
        ref SequenceReader<byte> reader,
        out ReadOnlySequence<byte> properties)
    {
        properties = ReadOnlySequence<byte>.Empty;
        if (!TryReadVariableByteInteger(ref reader, out int propertyLength) ||
            propertyLength < 0 ||
            reader.Remaining < propertyLength)
        {
            return false;
        }

        if (propertyLength > 0)
        {
            properties = reader.Sequence.Slice(reader.Position, propertyLength);
        }

        reader.Advance(propertyLength);
        return true;
    }

    private static bool TryReadUtf8(ref SequenceReader<byte> reader, out ReadOnlySequence<byte> value)
    {
        if (!TryReadUInt16(ref reader, out ushort length) || reader.Remaining < length)
        {
            value = ReadOnlySequence<byte>.Empty;
            return false;
        }

        value = length == 0
            ? ReadOnlySequence<byte>.Empty
            : reader.Sequence.Slice(reader.Position, length);
        reader.Advance(length);
        return true;
    }

    private static bool TryReadBinary(ref SequenceReader<byte> reader, out ReadOnlySequence<byte> value)
    {
        return TryReadUtf8(ref reader, out value);
    }

    private static bool TryReadUInt16(ref SequenceReader<byte> reader, out ushort value)
    {
        value = 0;
        if (!reader.TryRead(out byte high) || !reader.TryRead(out byte low))
        {
            return false;
        }

        value = BinaryPrimitives.ReadUInt16BigEndian([high, low]);
        return true;
    }

    private static bool TryReadVariableByteInteger(ref SequenceReader<byte> reader, out int value)
    {
        value = 0;
        int multiplier = 1;
        for (int index = 0; index < 4; index++)
        {
            if (!reader.TryRead(out byte encodedByte))
            {
                return false;
            }

            value += (encodedByte & 0x7F) * multiplier;
            if ((encodedByte & 0x80) == 0)
            {
                return true;
            }

            multiplier *= 128;
        }

        return false;
    }

    private static void WriteProperties(IBufferWriter<byte> writer, ReadOnlySequence<byte> properties)
    {
        int length = (int)properties.Length;
        Span<byte> lengthSpan = writer.GetSpan(4);
        int lengthBytes = WriteVariableByteInteger(lengthSpan, length);
        writer.Advance(lengthBytes);
        WriteSequence(writer, properties);
    }

    private static void WriteUtf8(IBufferWriter<byte> writer, ReadOnlySpan<byte> value)
    {
        if (value.Length > ushort.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(value), "MQTT UTF-8 values may not exceed 65535 bytes.");
        }

        WriteUInt16(writer, (ushort)value.Length);
        Span<byte> destination = writer.GetSpan(value.Length);
        value.CopyTo(destination);
        writer.Advance(value.Length);
    }

    private static void WriteBinary(IBufferWriter<byte> writer, ReadOnlySequence<byte> value)
    {
        if (value.Length > ushort.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(value), "MQTT binary values may not exceed 65535 bytes.");
        }

        WriteUInt16(writer, (ushort)value.Length);
        WriteSequence(writer, value);
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

    private static int WriteVariableByteInteger(Span<byte> destination, int value)
    {
        int index = 0;
        do
        {
            byte encodedByte = (byte)(value % 128);
            value /= 128;
            if (value > 0)
            {
                encodedByte |= 0x80;
            }

            destination[index++] = encodedByte;
        }
        while (value > 0);

        return index;
    }

    private static int GetVariableByteIntegerSize(long value)
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
