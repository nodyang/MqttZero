using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace MqttZero.Protocol;

internal static class Mqtt5PropertyWriter
{
    public static int GetConnectPropertiesLength(MqttConnectPacket packet)
    {
        if (!packet.Properties.IsEmpty)
        {
            return (int)packet.Properties.Length;
        }

        int length = 0;
        if (packet.SessionExpiryInterval.HasValue)
        {
            length += 5;
        }

        if (packet.ReceiveMaximum != ushort.MaxValue)
        {
            length += 3;
        }

        if (packet.MaximumPacketSize != uint.MaxValue)
        {
            length += 5;
        }

        if (packet.TopicAliasMaximum != 0)
        {
            length += 3;
        }

        return length;
    }

    public static void WriteConnectProperties(
        MqttConnectPacket packet,
        IBufferWriter<byte> writer)
    {
        int propertyLength = GetConnectPropertiesLength(packet);
        WritePropertyLength(writer, propertyLength);
        if (!packet.Properties.IsEmpty)
        {
            WriteSequence(writer, packet.Properties);
            return;
        }

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
    }

    public static int GetWillPropertiesLength(MqttConnectPacket packet)
    {
        if (!packet.WillProperties.IsEmpty)
        {
            return (int)packet.WillProperties.Length;
        }

        int length = 0;
        if (packet.WillDelayInterval != 0)
        {
            length += 5;
        }

        if (packet.WillMessageExpiryInterval != 0)
        {
            length += 5;
        }

        return length;
    }

    public static void WriteWillProperties(
        MqttConnectPacket packet,
        IBufferWriter<byte> writer)
    {
        int propertyLength = GetWillPropertiesLength(packet);
        WritePropertyLength(writer, propertyLength);
        if (!packet.WillProperties.IsEmpty)
        {
            WriteSequence(writer, packet.WillProperties);
            return;
        }

        if (packet.WillDelayInterval != 0)
        {
            WriteUInt32(writer, 0x18, packet.WillDelayInterval);
        }

        if (packet.WillMessageExpiryInterval != 0)
        {
            WriteUInt32(writer, 0x02, packet.WillMessageExpiryInterval);
        }
    }

    public static int GetPublishPropertiesLength(MqttPublishPacket packet)
    {
        if (!packet.Properties.IsEmpty)
        {
            return (int)packet.Properties.Length;
        }

        int length = 0;
        if (packet.MessageExpiryInterval.HasValue)
        {
            length += 5;
        }

        if (packet.SubscriptionIdentifier.HasValue)
        {
            length += 1 + GetVariableByteIntegerSize(
                packet.SubscriptionIdentifier.Value);
        }

        if (packet.TopicAlias.HasValue)
        {
            length += 3;
        }

        return length;
    }

    public static void WritePublishProperties(
        MqttPublishPacket packet,
        IBufferWriter<byte> writer)
    {
        int propertyLength = GetPublishPropertiesLength(packet);
        WritePropertyLength(writer, propertyLength);
        if (!packet.Properties.IsEmpty)
        {
            WriteSequence(writer, packet.Properties);
            return;
        }

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
    }

    public static int GetConnAckPropertiesLength(MqttConnAckPacket packet)
    {
        if (!packet.Properties.IsEmpty)
        {
            return (int)packet.Properties.Length;
        }

        int length = 0;
        if (packet.TopicAliasMaximum.HasValue)
        {
            length += 3;
        }

        if (packet.ReceiveMaximum.HasValue)
        {
            length += 3;
        }

        if (packet.MaximumPacketSize.HasValue)
        {
            length += 5;
        }

        if (packet.SessionExpiryInterval.HasValue)
        {
            length += 5;
        }

        if (!string.IsNullOrEmpty(packet.ReasonString))
        {
            length += 3 + Encoding.UTF8.GetByteCount(packet.ReasonString);
        }

        return length;
    }

    public static void WriteConnAckProperties(
        MqttConnAckPacket packet,
        IBufferWriter<byte> writer)
    {
        int propertyLength = GetConnAckPropertiesLength(packet);
        WritePropertyLength(writer, propertyLength);
        if (!packet.Properties.IsEmpty)
        {
            WriteSequence(writer, packet.Properties);
            return;
        }

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

        if (!string.IsNullOrEmpty(packet.ReasonString))
        {
            WriteString(writer, 0x1F, packet.ReasonString);
        }
    }

    public static int GetDisconnectPropertiesLength(MqttDisconnectPacket packet)
    {
        if (!packet.Properties.IsEmpty || !packet.SessionExpiryInterval.HasValue)
        {
            return (int)packet.Properties.Length;
        }

        return 5;
    }

    public static void WriteDisconnectProperties(
        MqttDisconnectPacket packet,
        IBufferWriter<byte> writer)
    {
        int propertyLength = GetDisconnectPropertiesLength(packet);
        WritePropertyLength(writer, propertyLength);
        if (!packet.Properties.IsEmpty || !packet.SessionExpiryInterval.HasValue)
        {
            WriteSequence(writer, packet.Properties);
            return;
        }

        WriteUInt32(writer, 0x11, packet.SessionExpiryInterval.Value);
    }

    public static int GetSubscribePropertiesLength(MqttSubscribePacket packet)
    {
        if (!packet.Properties.IsEmpty)
        {
            return (int)packet.Properties.Length;
        }

        return packet.SubscriptionIdentifier.HasValue
            ? 1 + GetVariableByteIntegerSize(packet.SubscriptionIdentifier.Value)
            : 0;
    }

    public static void WriteSubscribeProperties(
        MqttSubscribePacket packet,
        IBufferWriter<byte> writer)
    {
        int propertyLength = GetSubscribePropertiesLength(packet);
        WritePropertyLength(writer, propertyLength);
        if (!packet.Properties.IsEmpty)
        {
            WriteSequence(writer, packet.Properties);
            return;
        }

        if (packet.SubscriptionIdentifier.HasValue)
        {
            WriteByte(writer, 0x0B);
            WriteVariableByteInteger(writer, packet.SubscriptionIdentifier.Value);
        }
    }

    private static void WritePropertyLength(IBufferWriter<byte> writer, int length)
    {
        Span<byte> destination = writer.GetSpan(4);
        int count = MqttZeroPacketWriter.WriteVariableByteInteger(
            destination,
            (uint)length);
        writer.Advance(count);
    }

    private static void WriteSequence(
        IBufferWriter<byte> writer,
        ReadOnlySequence<byte> value)
    {
        if (value.IsSingleSegment)
        {
            WriteBytes(writer, value.FirstSpan);
            return;
        }

        foreach (ReadOnlyMemory<byte> segment in value)
        {
            WriteBytes(writer, segment.Span);
        }
    }

    private static void WriteBytes(IBufferWriter<byte> writer, ReadOnlySpan<byte> value)
    {
        Span<byte> destination = writer.GetSpan(value.Length);
        value.CopyTo(destination);
        writer.Advance(value.Length);
    }

    private static void WriteString(IBufferWriter<byte> writer, byte identifier, string value)
    {
        int byteCount = Encoding.UTF8.GetByteCount(value);
        Span<byte> destination = writer.GetSpan(byteCount + 3);
        destination[0] = identifier;
        BinaryPrimitives.WriteUInt16BigEndian(destination[1..], (ushort)byteCount);
        Encoding.UTF8.GetBytes(value, destination[3..]);
        writer.Advance(byteCount + 3);
    }

    private static void WriteUInt16(
        IBufferWriter<byte> writer,
        byte identifier,
        ushort value)
    {
        Span<byte> destination = writer.GetSpan(3);
        destination[0] = identifier;
        BinaryPrimitives.WriteUInt16BigEndian(destination[1..], value);
        writer.Advance(3);
    }

    private static void WriteUInt32(
        IBufferWriter<byte> writer,
        byte identifier,
        uint value)
    {
        Span<byte> destination = writer.GetSpan(5);
        destination[0] = identifier;
        BinaryPrimitives.WriteUInt32BigEndian(destination[1..], value);
        writer.Advance(5);
    }

    private static void WriteByte(IBufferWriter<byte> writer, byte value)
    {
        Span<byte> destination = writer.GetSpan(1);
        destination[0] = value;
        writer.Advance(1);
    }

    private static void WriteVariableByteInteger(IBufferWriter<byte> writer, uint value)
    {
        Span<byte> destination = writer.GetSpan(4);
        int count = MqttZeroPacketWriter.WriteVariableByteInteger(destination, value);
        writer.Advance(count);
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
