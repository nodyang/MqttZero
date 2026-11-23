using System.Buffers;
using System.Buffers.Binary;

namespace MqttZero.Protocol;

/// <summary>
/// Implements MQTT CONNECT and CONNACK packet codecs.
/// </summary>
public static partial class MqttPacketCodec
{
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

        if (!Mqtt5PropertyReader.TryReadConnect(properties, (MqttConnectPacket)packet) ||
            !Mqtt5PropertyReader.TryReadWill(willProperties, (MqttConnectPacket)packet))
        {
            packet = null;
            return false;
        }

        return reader.Remaining == 0;
    }

    private static void EncodeConnect(MqttConnectPacket packet, IBufferWriter<byte> writer)
    {
        ReadOnlySequence<byte> connectProperties = Mqtt5PropertyWriter.BuildConnectProperties(packet);
        ReadOnlySequence<byte> willProperties = packet.HasWill
            ? Mqtt5PropertyWriter.BuildWillProperties(packet)
            : ReadOnlySequence<byte>.Empty;
        int bodyLength = 10 +
            GetVariableByteIntegerSize(connectProperties.Length) +
            (int)connectProperties.Length +
            2 +
            (int)packet.ClientId.Length;

        if (packet.HasWill)
        {
            bodyLength += GetVariableByteIntegerSize(willProperties.Length) +
                (int)willProperties.Length +
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
        WriteProperties(writer, connectProperties);
        WriteUtf8(writer, packet.ClientId.IsEmpty ? ReadOnlySpan<byte>.Empty : packet.ClientId.FirstSpan);

        if (packet.HasWill)
        {
            WriteProperties(writer, willProperties);
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

    private static void EncodeConnAck(MqttConnAckPacket packet, IBufferWriter<byte> writer)
    {
        ReadOnlySequence<byte> properties = Mqtt5PropertyWriter.BuildConnAckProperties(packet);
        int bodyLength =
            2 +
            GetVariableByteIntegerSize(properties.Length) +
            (int)properties.Length;

        Span<byte> fixedHeader = writer.GetSpan(1 + 4);
        fixedHeader[0] = 0x20;
        int lengthBytes = WriteVariableByteInteger(fixedHeader[1..], bodyLength);
        writer.Advance(1 + lengthBytes);
        WriteByte(writer, packet.SessionPresent ? (byte)1 : (byte)0);
        WriteByte(writer, (byte)packet.ReasonCode);
        WriteProperties(writer, properties);
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

}
