using System.Buffers;
using System.Buffers.Binary;

namespace MqttZero.Protocol;

/// <summary>
/// Implements MQTT acknowledgement and control packet codecs.
/// </summary>
public static partial class MqttPacketCodec
{
    private static bool TryDecodeConnAck(
        byte firstByte,
        ref SequenceReader<byte> reader,
        out MqttPacket? packet)
    {
        packet = null;
        if ((firstByte & 0x0F) != 0 ||
            !reader.TryRead(out byte acknowledgeFlags) ||
            !reader.TryRead(out byte reasonCode) ||
            !TryReadProperties(ref reader, out ReadOnlySequence<byte> properties) ||
            reader.Remaining != 0)
        {
            return false;
        }

        packet = new MqttConnAckPacket
        {
            SessionPresent = (acknowledgeFlags & 0x01) != 0,
            ReasonCode = (MqttReasonCode)reasonCode,
            Properties = properties
        };
        if (!Mqtt5PropertyReader.TryReadConnAck(properties, (MqttConnAckPacket)packet))
        {
            packet = null;
            return false;
        }

        return true;
    }

    private static bool TryDecodePubAck(
        byte firstByte,
        ref SequenceReader<byte> reader,
        out MqttPacket? packet)
    {
        packet = null;
        if ((firstByte & 0x0F) != 0 ||
            !TryReadUInt16(ref reader, out ushort packetIdentifier))
        {
            return false;
        }

        MqttReasonCode reasonCode = MqttReasonCode.Success;
        ReadOnlySequence<byte> properties = ReadOnlySequence<byte>.Empty;
        if (reader.Remaining > 0)
        {
            if (!reader.TryRead(out byte encodedReasonCode))
            {
                return false;
            }

            reasonCode = (MqttReasonCode)encodedReasonCode;
            if (reader.Remaining > 0 && !TryReadProperties(ref reader, out properties))
            {
                return false;
            }
        }

        packet = new MqttPubAckPacket
        {
            PacketIdentifier = packetIdentifier,
            ReasonCode = reasonCode,
            Properties = properties
        };
        return reader.Remaining == 0;
    }

    private static bool TryDecodePubRec(
        byte firstByte,
        ref SequenceReader<byte> reader,
        out MqttPacket? packet)
    {
        packet = null;
        if ((firstByte & 0x0F) != 0 ||
            !TryReadAcknowledgement(
                ref reader,
                out ushort packetIdentifier,
                out MqttReasonCode reasonCode,
                out ReadOnlySequence<byte> properties))
        {
            return false;
        }

        packet = new MqttPubRecPacket
        {
            PacketIdentifier = packetIdentifier,
            ReasonCode = reasonCode,
            Properties = properties
        };
        return true;
    }

    private static bool TryDecodePubRel(
        byte firstByte,
        ref SequenceReader<byte> reader,
        out MqttPacket? packet)
    {
        packet = null;
        if ((firstByte & 0x0F) != 0x02 ||
            !TryReadAcknowledgement(
                ref reader,
                out ushort packetIdentifier,
                out MqttReasonCode reasonCode,
                out ReadOnlySequence<byte> properties))
        {
            return false;
        }

        packet = new MqttPubRelPacket
        {
            PacketIdentifier = packetIdentifier,
            ReasonCode = reasonCode,
            Properties = properties
        };
        return true;
    }

    private static bool TryDecodePubComp(
        byte firstByte,
        ref SequenceReader<byte> reader,
        out MqttPacket? packet)
    {
        packet = null;
        if ((firstByte & 0x0F) != 0 ||
            !TryReadAcknowledgement(
                ref reader,
                out ushort packetIdentifier,
                out MqttReasonCode reasonCode,
                out ReadOnlySequence<byte> properties))
        {
            return false;
        }

        packet = new MqttPubCompPacket
        {
            PacketIdentifier = packetIdentifier,
            ReasonCode = reasonCode,
            Properties = properties
        };
        return true;
    }

    private static bool TryReadAcknowledgement(
        ref SequenceReader<byte> reader,
        out ushort packetIdentifier,
        out MqttReasonCode reasonCode,
        out ReadOnlySequence<byte> properties)
    {
        reasonCode = MqttReasonCode.Success;
        properties = ReadOnlySequence<byte>.Empty;
        if (!TryReadUInt16(ref reader, out packetIdentifier))
        {
            return false;
        }

        if (reader.Remaining > 0)
        {
            if (!reader.TryRead(out byte encodedReasonCode))
            {
                return false;
            }

            reasonCode = (MqttReasonCode)encodedReasonCode;
            if (reader.Remaining > 0 &&
                !TryReadProperties(ref reader, out properties))
            {
                return false;
            }
        }

        return reader.Remaining == 0;
    }

    private static void EncodeAcknowledgement(
        byte firstByte,
        ushort packetIdentifier,
        MqttReasonCode reasonCode,
        ReadOnlySequence<byte> properties,
        IBufferWriter<byte> writer)
    {
        bool includeReason =
            reasonCode != MqttReasonCode.Success || !properties.IsEmpty;
        int bodyLength = includeReason
            ? 3 +
                GetVariableByteIntegerSize(properties.Length) +
                (int)properties.Length
            : 2;
        Span<byte> fixedHeader = writer.GetSpan(5);
        fixedHeader[0] = firstByte;
        int lengthBytes = WriteVariableByteInteger(fixedHeader[1..], bodyLength);
        writer.Advance(1 + lengthBytes);
        WriteUInt16(writer, packetIdentifier);
        if (includeReason)
        {
            WriteByte(writer, (byte)reasonCode);
            WriteProperties(writer, properties);
        }
    }

    private static bool TryDecodePingReq(
        byte firstByte,
        ref SequenceReader<byte> reader,
        out MqttPacket? packet)
    {
        packet = null;
        if ((firstByte & 0x0F) != 0)
        {
            return false;
        }

        packet = new MqttPingReqPacket();
        return reader.Remaining == 0;
    }

    private static bool TryDecodeDisconnect(
        byte firstByte,
        ref SequenceReader<byte> reader,
        out MqttPacket? packet)
    {
        packet = null;
        if ((firstByte & 0x0F) != 0)
        {
            return false;
        }

        MqttReasonCode reasonCode = MqttReasonCode.Success;
        ReadOnlySequence<byte> properties = ReadOnlySequence<byte>.Empty;
        if (reader.Remaining > 0)
        {
            if (!reader.TryRead(out byte encodedReasonCode))
            {
                return false;
            }

            reasonCode = (MqttReasonCode)encodedReasonCode;
            if (reader.Remaining > 0 && !TryReadProperties(ref reader, out properties))
            {
                return false;
            }
        }

        packet = new MqttDisconnectPacket
        {
            ReasonCode = reasonCode,
            Properties = properties
        };
        if (!Mqtt5PropertyReader.TryReadDisconnect(properties, (MqttDisconnectPacket)packet))
        {
            packet = null;
            return false;
        }

        return reader.Remaining == 0;
    }

    private static void EncodePubAck(MqttPubAckPacket packet, IBufferWriter<byte> writer)
    {
        bool includeReason = packet.ReasonCode != MqttReasonCode.Success || !packet.Properties.IsEmpty;
        int bodyLength = includeReason
            ? 3 + GetVariableByteIntegerSize(packet.Properties.Length) + (int)packet.Properties.Length
            : 2;

        Span<byte> fixedHeader = writer.GetSpan(1 + 4);
        fixedHeader[0] = 0x40;
        int lengthBytes = WriteVariableByteInteger(fixedHeader[1..], bodyLength);
        writer.Advance(1 + lengthBytes);
        WriteUInt16(writer, packet.PacketIdentifier);

        if (includeReason)
        {
            WriteByte(writer, (byte)packet.ReasonCode);
            WriteProperties(writer, packet.Properties);
        }
    }

    private static void EncodeEmptyPacket(MqttPacketType packetType, IBufferWriter<byte> writer)
    {
        Span<byte> destination = writer.GetSpan(2);
        destination[0] = (byte)((byte)packetType << 4);
        destination[1] = 0;
        writer.Advance(2);
    }

    private static void EncodeDisconnect(MqttDisconnectPacket packet, IBufferWriter<byte> writer)
    {
        int propertiesLength = Mqtt5PropertyWriter.GetDisconnectPropertiesLength(packet);
        bool includeDetail =
            packet.ReasonCode != MqttReasonCode.Success ||
            propertiesLength > 0;
        int bodyLength = includeDetail
            ? 1 + GetVariableByteIntegerSize(propertiesLength) + propertiesLength
            : 0;

        Span<byte> fixedHeader = writer.GetSpan(1 + 4);
        fixedHeader[0] = 0xE0;
        int lengthBytes = WriteVariableByteInteger(fixedHeader[1..], bodyLength);
        writer.Advance(1 + lengthBytes);

        if (includeDetail)
        {
            WriteByte(writer, (byte)packet.ReasonCode);
            Mqtt5PropertyWriter.WriteDisconnectProperties(packet, writer);
        }
    }

}
