using System.Buffers;
using System.Buffers.Binary;

namespace MqttZero.Protocol;

/// <summary>
/// Implements MQTT PUBLISH packet codecs.
/// </summary>
public static partial class MqttPacketCodec
{
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

        if (!Mqtt5PropertyReader.TryReadPublish(properties, (MqttPublishPacket)packet))
        {
            packet = null;
            return false;
        }

        return true;
    }

    private static void EncodePublish(MqttPublishPacket packet, IBufferWriter<byte> writer)
    {
        ReadOnlySequence<byte> properties = Mqtt5PropertyWriter.BuildPublishProperties(packet);
        int bodyLength =
            2 +
            (int)packet.TopicName.Length +
            (packet.QualityOfServiceLevel == MqttQualityOfServiceLevel.AtMostOnce ? 0 : 2) +
            GetVariableByteIntegerSize(properties.Length) +
            (int)properties.Length +
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

        WriteProperties(writer, properties);
        WriteSequence(writer, packet.Payload);
    }

}
