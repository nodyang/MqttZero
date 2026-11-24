using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace MqttZero.Protocol;

internal static class Mqtt5PropertyReader
{
    private const byte PayloadFormatIndicator = 0x01;
    private const byte MessageExpiryInterval = 0x02;
    private const byte ContentType = 0x03;
    private const byte ResponseTopic = 0x08;
    private const byte CorrelationData = 0x09;
    private const byte SubscriptionIdentifier = 0x0B;
    private const byte SessionExpiryInterval = 0x11;
    private const byte AssignedClientIdentifier = 0x12;
    private const byte ServerKeepAlive = 0x13;
    private const byte AuthenticationMethod = 0x15;
    private const byte AuthenticationData = 0x16;
    private const byte RequestProblemInformation = 0x17;
    private const byte WillDelayInterval = 0x18;
    private const byte RequestResponseInformation = 0x19;
    private const byte ResponseInformation = 0x1A;
    private const byte ServerReference = 0x1C;
    private const byte ReasonString = 0x1F;
    private const byte ReceiveMaximum = 0x21;
    private const byte TopicAliasMaximum = 0x22;
    private const byte TopicAlias = 0x23;
    private const byte MaximumQos = 0x24;
    private const byte RetainAvailable = 0x25;
    private const byte UserProperty = 0x26;
    private const byte MaximumPacketSize = 0x27;
    private const byte WildcardSubscriptionAvailable = 0x28;
    private const byte SubscriptionIdentifierAvailable = 0x29;
    private const byte SharedSubscriptionAvailable = 0x2A;

    public static bool TryReadConnect(
        ReadOnlySequence<byte> properties,
        MqttConnectPacket packet)
    {
        bool hasSessionExpiry = false;
        bool hasReceiveMaximum = false;
        bool hasMaximumPacketSize = false;
        bool hasTopicAliasMaximum = false;
        SequenceReader<byte> reader = new(properties);

        while (reader.Remaining > 0)
        {
            if (!reader.TryRead(out byte identifier))
            {
                return false;
            }

            switch (identifier)
            {
                case SessionExpiryInterval:
                    if (hasSessionExpiry || !TryReadUInt32(ref reader, out uint sessionExpiry))
                    {
                        return false;
                    }

                    hasSessionExpiry = true;
                    packet.SessionExpiryInterval = sessionExpiry;
                    break;
                case ReceiveMaximum:
                    if (hasReceiveMaximum ||
                        !TryReadUInt16(ref reader, out ushort receiveMaximum) ||
                        receiveMaximum == 0)
                    {
                        return false;
                    }

                    hasReceiveMaximum = true;
                    packet.ReceiveMaximum = receiveMaximum;
                    break;
                case MaximumPacketSize:
                    if (hasMaximumPacketSize ||
                        !TryReadUInt32(ref reader, out uint maximumPacketSize) ||
                        maximumPacketSize == 0)
                    {
                        return false;
                    }

                    hasMaximumPacketSize = true;
                    packet.MaximumPacketSize = maximumPacketSize;
                    break;
                case TopicAliasMaximum:
                    if (hasTopicAliasMaximum ||
                        !TryReadUInt16(ref reader, out ushort topicAliasMaximum))
                    {
                        return false;
                    }

                    hasTopicAliasMaximum = true;
                    packet.TopicAliasMaximum = topicAliasMaximum;
                    break;
                default:
                    if (!TrySkipProperty(identifier, ref reader))
                    {
                        return false;
                    }

                    break;
            }
        }

        return true;
    }

    public static bool TryReadWill(
        ReadOnlySequence<byte> properties,
        MqttConnectPacket packet)
    {
        bool hasWillDelay = false;
        bool hasMessageExpiry = false;
        SequenceReader<byte> reader = new(properties);

        while (reader.Remaining > 0)
        {
            if (!reader.TryRead(out byte identifier))
            {
                return false;
            }

            switch (identifier)
            {
                case WillDelayInterval:
                    if (hasWillDelay || !TryReadUInt32(ref reader, out uint willDelay))
                    {
                        return false;
                    }

                    hasWillDelay = true;
                    packet.WillDelayInterval = willDelay;
                    break;
                case MessageExpiryInterval:
                    if (hasMessageExpiry || !TryReadUInt32(ref reader, out uint messageExpiry))
                    {
                        return false;
                    }

                    hasMessageExpiry = true;
                    packet.WillMessageExpiryInterval = messageExpiry;
                    break;
                default:
                    if (!TrySkipProperty(identifier, ref reader))
                    {
                        return false;
                    }

                    break;
            }
        }

        return true;
    }

    public static bool TryReadPublish(
        ReadOnlySequence<byte> properties,
        MqttPublishPacket packet)
    {
        bool hasMessageExpiry = false;
        SequenceReader<byte> reader = new(properties);

        while (reader.Remaining > 0)
        {
            if (!reader.TryRead(out byte identifier))
            {
                return false;
            }

            switch (identifier)
            {
                case MessageExpiryInterval:
                    if (hasMessageExpiry || !TryReadUInt32(ref reader, out uint messageExpiry))
                    {
                        return false;
                    }

                    hasMessageExpiry = true;
                    packet.MessageExpiryInterval = messageExpiry;
                    break;
                case SubscriptionIdentifier:
                    if (packet.SubscriptionIdentifier.HasValue ||
                        !TryReadVariableByteInteger(ref reader, out int subscriptionIdentifier) ||
                        subscriptionIdentifier == 0)
                    {
                        return false;
                    }

                    packet.SubscriptionIdentifier = (uint)subscriptionIdentifier;
                    break;
                case TopicAlias:
                    if (packet.TopicAlias.HasValue ||
                        !TryReadUInt16(ref reader, out ushort topicAlias) ||
                        topicAlias == 0)
                    {
                        return false;
                    }

                    packet.TopicAlias = topicAlias;
                    break;
                default:
                    if (!TrySkipProperty(identifier, ref reader))
                    {
                        return false;
                    }

                    break;
            }
        }

        return true;
    }

    public static bool TryReadConnAck(
        ReadOnlySequence<byte> properties,
        MqttConnAckPacket packet)
    {
        bool hasTopicAliasMaximum = false;
        bool hasReceiveMaximum = false;
        bool hasMaximumPacketSize = false;
        bool hasSessionExpiry = false;
        bool hasAssignedClientIdentifier = false;
        SequenceReader<byte> reader = new(properties);
        while (reader.Remaining > 0)
        {
            if (!reader.TryRead(out byte identifier))
            {
                return false;
            }

            switch (identifier)
            {
                case TopicAliasMaximum:
                    if (hasTopicAliasMaximum ||
                        !TryReadUInt16(ref reader, out ushort topicAliasMaximum))
                    {
                        return false;
                    }

                    hasTopicAliasMaximum = true;
                    packet.TopicAliasMaximum = topicAliasMaximum;
                    break;
                case ReceiveMaximum:
                    if (hasReceiveMaximum ||
                        !TryReadUInt16(ref reader, out ushort receiveMaximum) ||
                        receiveMaximum == 0)
                    {
                        return false;
                    }

                    hasReceiveMaximum = true;
                    packet.ReceiveMaximum = receiveMaximum;
                    break;
                case MaximumPacketSize:
                    if (hasMaximumPacketSize ||
                        !TryReadUInt32(ref reader, out uint maximumPacketSize) ||
                        maximumPacketSize == 0)
                    {
                        return false;
                    }

                    hasMaximumPacketSize = true;
                    packet.MaximumPacketSize = maximumPacketSize;
                    break;
                case SessionExpiryInterval:
                    if (hasSessionExpiry ||
                        !TryReadUInt32(ref reader, out uint sessionExpiry))
                    {
                        return false;
                    }

                    hasSessionExpiry = true;
                    packet.SessionExpiryInterval = sessionExpiry;
                    break;
                case AssignedClientIdentifier:
                    if (hasAssignedClientIdentifier ||
                        !TryReadUtf8String(ref reader, out string? assignedClientIdentifier))
                    {
                        return false;
                    }

                    hasAssignedClientIdentifier = true;
                    packet.AssignedClientIdentifier = assignedClientIdentifier;
                    break;
                default:
                    if (!TrySkipProperty(identifier, ref reader))
                    {
                        return false;
                    }

                    break;
            }
        }

        return true;
    }

    public static bool TryReadSubscribe(
        ReadOnlySequence<byte> properties,
        MqttSubscribePacket packet)
    {
        SequenceReader<byte> reader = new(properties);
        while (reader.Remaining > 0)
        {
            if (!reader.TryRead(out byte identifier))
            {
                return false;
            }

            if (identifier == SubscriptionIdentifier)
            {
                if (packet.SubscriptionIdentifier.HasValue ||
                    !TryReadVariableByteInteger(ref reader, out int subscriptionIdentifier) ||
                    subscriptionIdentifier == 0)
                {
                    return false;
                }

                packet.SubscriptionIdentifier = (uint)subscriptionIdentifier;
            }
            else if (!TrySkipProperty(identifier, ref reader))
            {
                return false;
            }
        }

        return true;
    }

    public static bool TryReadDisconnect(
        ReadOnlySequence<byte> properties,
        MqttDisconnectPacket packet)
    {
        bool hasSessionExpiry = false;
        SequenceReader<byte> reader = new(properties);
        while (reader.Remaining > 0)
        {
            if (!reader.TryRead(out byte identifier))
            {
                return false;
            }

            if (identifier == SessionExpiryInterval)
            {
                if (hasSessionExpiry ||
                    !TryReadUInt32(ref reader, out uint sessionExpiry))
                {
                    return false;
                }

                hasSessionExpiry = true;
                packet.SessionExpiryInterval = sessionExpiry;
            }
            else if (!TrySkipProperty(identifier, ref reader))
            {
                return false;
            }
        }

        return true;
    }

    private static bool TrySkipProperty(byte identifier, ref SequenceReader<byte> reader)
    {
        return identifier switch
        {
            PayloadFormatIndicator or RequestProblemInformation or RequestResponseInformation or
                MaximumQos or RetainAvailable or WildcardSubscriptionAvailable or
                SubscriptionIdentifierAvailable or SharedSubscriptionAvailable => TryAdvance(ref reader, 1),
            MessageExpiryInterval or SessionExpiryInterval or WillDelayInterval or MaximumPacketSize =>
                TryAdvance(ref reader, 4),
            ServerKeepAlive or ReceiveMaximum or TopicAliasMaximum or TopicAlias =>
                TryAdvance(ref reader, 2),
            CorrelationData or AuthenticationData => TrySkipBinary(ref reader),
            ContentType or ResponseTopic or AssignedClientIdentifier or AuthenticationMethod or
                ResponseInformation or ServerReference or ReasonString => TrySkipUtf8(ref reader),
            UserProperty => TrySkipUtf8(ref reader) && TrySkipUtf8(ref reader),
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

    private static bool TrySkipUtf8(ref SequenceReader<byte> reader)
    {
        return TrySkipBinary(ref reader);
    }

    private static bool TryReadUtf8String(
        ref SequenceReader<byte> reader,
        out string? value)
    {
        value = null;
        if (!TryReadUInt16(ref reader, out ushort length) ||
            reader.Remaining < length)
        {
            return false;
        }

        ReadOnlySequence<byte> encoded = length == 0
            ? ReadOnlySequence<byte>.Empty
            : reader.Sequence.Slice(reader.Position, length);
        if (!MqttZeroProtocolValidator.IsValidUtf8(encoded))
        {
            return false;
        }

        value = length == 0
            ? string.Empty
            : Encoding.UTF8.GetString(encoded);
        reader.Advance(length);
        return true;
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

    private static bool TryReadUInt32(ref SequenceReader<byte> reader, out uint value)
    {
        value = 0;
        if (!reader.TryRead(out byte b1) ||
            !reader.TryRead(out byte b2) ||
            !reader.TryRead(out byte b3) ||
            !reader.TryRead(out byte b4))
        {
            return false;
        }

        value = BinaryPrimitives.ReadUInt32BigEndian([b1, b2, b3, b4]);
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
}
