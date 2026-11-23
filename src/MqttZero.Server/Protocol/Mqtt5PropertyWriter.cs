using System.Buffers;
using System.Buffers.Binary;

namespace MqttZero.Protocol;

internal static class Mqtt5PropertyWriter
{
    public static ReadOnlySequence<byte> BuildConnectProperties(MqttConnectPacket packet)
    {
        if (!packet.Properties.IsEmpty)
        {
            return packet.Properties;
        }

        ArrayBufferWriter<byte> writer = new();
        if (packet.SessionExpiryInterval.HasValue)
        {
            WriteUInt32(writer, 0x11, packet.SessionExpiryInterval.Value);
        }

        if (packet.ReceiveMaximum != ushort.MaxValue)
        {
            WriteUInt16(writer, 0x21, packet.ReceiveMaximum);
        }

        if (packet.MaximumPacketSize != uint.MaxValue)
        {
            WriteUInt32(writer, 0x27, packet.MaximumPacketSize);
        }

        if (packet.TopicAliasMaximum != 0)
        {
            WriteUInt16(writer, 0x22, packet.TopicAliasMaximum);
        }

        return new ReadOnlySequence<byte>(writer.WrittenMemory);
    }

    public static ReadOnlySequence<byte> BuildWillProperties(MqttConnectPacket packet)
    {
        if (!packet.WillProperties.IsEmpty)
        {
            return packet.WillProperties;
        }

        ArrayBufferWriter<byte> writer = new();
        if (packet.WillDelayInterval != 0)
        {
            WriteUInt32(writer, 0x18, packet.WillDelayInterval);
        }

        if (packet.WillMessageExpiryInterval != 0)
        {
            WriteUInt32(writer, 0x02, packet.WillMessageExpiryInterval);
        }

        return new ReadOnlySequence<byte>(writer.WrittenMemory);
    }

    public static ReadOnlySequence<byte> BuildPublishProperties(MqttPublishPacket packet)
    {
        if (!packet.Properties.IsEmpty)
        {
            return packet.Properties;
        }

        ArrayBufferWriter<byte> writer = new();
        if (packet.MessageExpiryInterval.HasValue)
        {
            WriteUInt32(writer, 0x02, packet.MessageExpiryInterval.Value);
        }

        if (packet.SubscriptionIdentifier.HasValue)
        {
            WriteByte(writer, 0x0B);
            WriteVariableByteInteger(writer, packet.SubscriptionIdentifier.Value);
        }

        if (packet.TopicAlias.HasValue)
        {
            WriteUInt16(writer, 0x23, packet.TopicAlias.Value);
        }

        return new ReadOnlySequence<byte>(writer.WrittenMemory);
    }

    public static ReadOnlySequence<byte> BuildConnAckProperties(MqttConnAckPacket packet)
    {
        if (!packet.Properties.IsEmpty)
        {
            return packet.Properties;
        }

        ArrayBufferWriter<byte> writer = new();
        if (packet.TopicAliasMaximum.HasValue)
        {
            WriteUInt16(writer, 0x22, packet.TopicAliasMaximum.Value);
        }

        if (packet.ReceiveMaximum.HasValue)
        {
            WriteUInt16(writer, 0x21, packet.ReceiveMaximum.Value);
        }

        if (packet.MaximumPacketSize.HasValue)
        {
            WriteUInt32(writer, 0x27, packet.MaximumPacketSize.Value);
        }

        if (packet.SessionExpiryInterval.HasValue)
        {
            WriteUInt32(writer, 0x11, packet.SessionExpiryInterval.Value);
        }

        return new ReadOnlySequence<byte>(writer.WrittenMemory);
    }

    public static ReadOnlySequence<byte> BuildDisconnectProperties(MqttDisconnectPacket packet)
    {
        if (!packet.Properties.IsEmpty || !packet.SessionExpiryInterval.HasValue)
        {
            return packet.Properties;
        }

        ArrayBufferWriter<byte> writer = new();
        WriteUInt32(writer, 0x11, packet.SessionExpiryInterval.Value);
        return new ReadOnlySequence<byte>(writer.WrittenMemory);
    }

    public static ReadOnlySequence<byte> BuildSubscribeProperties(MqttSubscribePacket packet)
    {
        if (!packet.Properties.IsEmpty)
        {
            return packet.Properties;
        }

        if (!packet.SubscriptionIdentifier.HasValue)
        {
            return ReadOnlySequence<byte>.Empty;
        }

        ArrayBufferWriter<byte> writer = new();
        WriteByte(writer, 0x0B);
        WriteVariableByteInteger(writer, packet.SubscriptionIdentifier.Value);
        return new ReadOnlySequence<byte>(writer.WrittenMemory);
    }

    private static void WriteUInt16(ArrayBufferWriter<byte> writer, byte identifier, ushort value)
    {
        WriteByte(writer, identifier);
        Span<byte> destination = writer.GetSpan(2);
        BinaryPrimitives.WriteUInt16BigEndian(destination, value);
        writer.Advance(2);
    }

    private static void WriteUInt32(ArrayBufferWriter<byte> writer, byte identifier, uint value)
    {
        WriteByte(writer, identifier);
        Span<byte> destination = writer.GetSpan(4);
        BinaryPrimitives.WriteUInt32BigEndian(destination, value);
        writer.Advance(4);
    }

    private static void WriteByte(ArrayBufferWriter<byte> writer, byte value)
    {
        Span<byte> destination = writer.GetSpan(1);
        destination[0] = value;
        writer.Advance(1);
    }

    private static void WriteVariableByteInteger(ArrayBufferWriter<byte> writer, uint value)
    {
        do
        {
            byte encoded = (byte)(value % 128);
            value /= 128;
            if (value > 0)
            {
                encoded |= 0x80;
            }

            WriteByte(writer, encoded);
        }
        while (value > 0);
    }
}
