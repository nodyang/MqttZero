using System.Buffers;
using System.Buffers.Binary;

namespace MqttZero.Protocol;

/// <summary>
/// Implements MQTT subscription packet codecs.
/// </summary>
public static partial class MqttPacketCodec
{
    private static bool TryDecodeSubscribe(
        byte firstByte,
        ref SequenceReader<byte> reader,
        out MqttPacket? packet)
    {
        packet = null;
        if ((firstByte & 0x0F) != 0x02 ||
            !TryReadUInt16(ref reader, out ushort packetIdentifier) ||
            !TryReadProperties(ref reader, out ReadOnlySequence<byte> properties))
        {
            return false;
        }

        List<MqttSubscriptionRequest> subscriptions = [];
        while (reader.Remaining > 0)
        {
            if (!TryReadUtf8(ref reader, out ReadOnlySequence<byte> topicFilter) ||
                !reader.TryRead(out byte options))
            {
                return false;
            }

            MqttQualityOfServiceLevel qos = (MqttQualityOfServiceLevel)(options & 0x03);
            byte retainHandling = (byte)((options >> 4) & 0x03);
            if (qos == MqttQualityOfServiceLevel.ExactlyOnce || retainHandling == 3)
            {
                return false;
            }

            subscriptions.Add(new MqttSubscriptionRequest
            {
                TopicFilter = topicFilter,
                QualityOfServiceLevel = qos,
                NoLocal = (options & 0x04) != 0,
                RetainAsPublished = (options & 0x08) != 0,
                RetainHandling = retainHandling
            });
        }

        packet = new MqttSubscribePacket
        {
            PacketIdentifier = packetIdentifier,
            Properties = properties,
            Subscriptions = subscriptions
        };
        if (!Mqtt5PropertyReader.TryReadSubscribe(properties, (MqttSubscribePacket)packet))
        {
            packet = null;
            return false;
        }

        return subscriptions.Count > 0;
    }

    private static bool TryDecodeUnsubscribe(
        byte firstByte,
        ref SequenceReader<byte> reader,
        out MqttPacket? packet)
    {
        packet = null;
        if ((firstByte & 0x0F) != 0x02 ||
            !TryReadUInt16(ref reader, out ushort packetIdentifier) ||
            !TryReadProperties(ref reader, out ReadOnlySequence<byte> properties))
        {
            return false;
        }

        List<ReadOnlySequence<byte>> topicFilters = [];
        while (reader.Remaining > 0)
        {
            if (!TryReadUtf8(ref reader, out ReadOnlySequence<byte> topicFilter))
            {
                return false;
            }

            topicFilters.Add(topicFilter);
        }

        packet = new MqttUnsubscribePacket
        {
            PacketIdentifier = packetIdentifier,
            Properties = properties,
            TopicFilters = topicFilters
        };
        return topicFilters.Count > 0;
    }

    private static bool TryDecodeSubAck(
        byte firstByte,
        ref SequenceReader<byte> reader,
        out MqttPacket? packet)
    {
        packet = null;
        if ((firstByte & 0x0F) != 0 ||
            !TryReadUInt16(ref reader, out ushort packetIdentifier) ||
            !TryReadProperties(ref reader, out ReadOnlySequence<byte> properties))
        {
            return false;
        }

        ReadOnlyMemory<byte> reasonCodes = reader.Sequence
            .Slice(reader.Position, reader.Remaining)
            .ToArray();
        reader.Advance(reader.Remaining);
        packet = new MqttSubAckPacket
        {
            PacketIdentifier = packetIdentifier,
            Properties = properties,
            ReasonCodes = reasonCodes
        };
        return true;
    }

    private static bool TryDecodeUnsubAck(
        byte firstByte,
        ref SequenceReader<byte> reader,
        out MqttPacket? packet)
    {
        packet = null;
        if ((firstByte & 0x0F) != 0 ||
            !TryReadUInt16(ref reader, out ushort packetIdentifier) ||
            !TryReadProperties(ref reader, out ReadOnlySequence<byte> properties))
        {
            return false;
        }

        ReadOnlyMemory<byte> reasonCodes = reader.UnreadSequence.ToArray();
        reader.Advance(reader.Remaining);
        packet = new MqttUnsubAckPacket
        {
            PacketIdentifier = packetIdentifier,
            Properties = properties,
            ReasonCodes = reasonCodes
        };
        return true;
    }

    private static void EncodeSubscribe(MqttSubscribePacket packet, IBufferWriter<byte> writer)
    {
        ReadOnlySequence<byte> properties = Mqtt5PropertyWriter.BuildSubscribeProperties(packet);
        int bodyLength = 2 +
            GetVariableByteIntegerSize(properties.Length) +
            (int)properties.Length;
        foreach (MqttSubscriptionRequest subscription in packet.Subscriptions)
        {
            bodyLength += 2 + (int)subscription.TopicFilter.Length + 1;
        }

        Span<byte> fixedHeader = writer.GetSpan(1 + 4);
        fixedHeader[0] = 0x82;
        int lengthBytes = WriteVariableByteInteger(fixedHeader[1..], bodyLength);
        writer.Advance(1 + lengthBytes);
        WriteUInt16(writer, packet.PacketIdentifier);
        WriteProperties(writer, properties);

        foreach (MqttSubscriptionRequest subscription in packet.Subscriptions)
        {
            WriteUtf8(writer, subscription.TopicFilter.IsEmpty
                ? ReadOnlySpan<byte>.Empty
                : subscription.TopicFilter.FirstSpan);
            byte options = (byte)subscription.QualityOfServiceLevel;
            if (subscription.NoLocal)
            {
                options |= 0x04;
            }

            if (subscription.RetainAsPublished)
            {
                options |= 0x08;
            }

            options |= (byte)(subscription.RetainHandling << 4);
            WriteByte(writer, options);
        }
    }

    private static void EncodeSubAck(MqttSubAckPacket packet, IBufferWriter<byte> writer)
    {
        int bodyLength = 2 +
            GetVariableByteIntegerSize(packet.Properties.Length) +
            (int)packet.Properties.Length +
            packet.ReasonCodes.Length;

        Span<byte> fixedHeader = writer.GetSpan(1 + 4);
        fixedHeader[0] = 0x90;
        int lengthBytes = WriteVariableByteInteger(fixedHeader[1..], bodyLength);
        writer.Advance(1 + lengthBytes);
        WriteUInt16(writer, packet.PacketIdentifier);
        WriteProperties(writer, packet.Properties);

        Span<byte> reasonCodes = writer.GetSpan(packet.ReasonCodes.Length);
        packet.ReasonCodes.Span.CopyTo(reasonCodes);
        writer.Advance(packet.ReasonCodes.Length);
    }

    private static void EncodeUnsubscribe(MqttUnsubscribePacket packet, IBufferWriter<byte> writer)
    {
        int bodyLength = 2 +
            GetVariableByteIntegerSize(packet.Properties.Length) +
            (int)packet.Properties.Length;
        foreach (ReadOnlySequence<byte> topicFilter in packet.TopicFilters)
        {
            bodyLength += 2 + (int)topicFilter.Length;
        }

        Span<byte> fixedHeader = writer.GetSpan(1 + 4);
        fixedHeader[0] = 0xA2;
        int lengthBytes = WriteVariableByteInteger(fixedHeader[1..], bodyLength);
        writer.Advance(1 + lengthBytes);
        WriteUInt16(writer, packet.PacketIdentifier);
        WriteProperties(writer, packet.Properties);
        foreach (ReadOnlySequence<byte> topicFilter in packet.TopicFilters)
        {
            WriteUtf8(
                writer,
                topicFilter.IsEmpty ? ReadOnlySpan<byte>.Empty : topicFilter.FirstSpan);
        }
    }

    private static void EncodeUnsubAck(MqttUnsubAckPacket packet, IBufferWriter<byte> writer)
    {
        int bodyLength = 2 +
            GetVariableByteIntegerSize(packet.Properties.Length) +
            (int)packet.Properties.Length +
            packet.ReasonCodes.Length;

        Span<byte> fixedHeader = writer.GetSpan(1 + 4);
        fixedHeader[0] = 0xB0;
        int lengthBytes = WriteVariableByteInteger(fixedHeader[1..], bodyLength);
        writer.Advance(1 + lengthBytes);
        WriteUInt16(writer, packet.PacketIdentifier);
        WriteProperties(writer, packet.Properties);
        Span<byte> reasonCodes = writer.GetSpan(packet.ReasonCodes.Length);
        packet.ReasonCodes.Span.CopyTo(reasonCodes);
        writer.Advance(packet.ReasonCodes.Length);
    }

}
