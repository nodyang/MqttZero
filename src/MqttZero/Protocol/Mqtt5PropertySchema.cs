namespace MqttZero.Protocol;

internal static class Mqtt5PropertySchema
{
    private static readonly ulong ConnectProperties =
        PropertyMask(0x11, 0x15, 0x16, 0x17, 0x19, 0x21, 0x22, 0x26, 0x27);
    private static readonly ulong WillProperties =
        PropertyMask(0x01, 0x02, 0x03, 0x08, 0x09, 0x18, 0x26);
    private static readonly ulong ConnAckProperties =
        PropertyMask(
            0x11,
            0x12,
            0x13,
            0x15,
            0x16,
            0x1A,
            0x1C,
            0x1F,
            0x21,
            0x22,
            0x24,
            0x25,
            0x26,
            0x27,
            0x28,
            0x29,
            0x2A);
    private static readonly ulong PublishProperties =
        PropertyMask(0x01, 0x02, 0x03, 0x08, 0x09, 0x0B, 0x23, 0x26);
    private static readonly ulong AcknowledgementProperties = PropertyMask(0x1F, 0x26);
    private static readonly ulong SubscribeProperties = PropertyMask(0x0B, 0x26);
    private static readonly ulong UnsubscribeProperties = PropertyMask(0x26);
    private static readonly ulong DisconnectProperties =
        PropertyMask(0x11, 0x1C, 0x1F, 0x26);

    public static bool IsAllowed(MqttPacketType packetType, byte identifier)
    {
        ulong properties = packetType switch
        {
            MqttPacketType.Connect => ConnectProperties,
            MqttPacketType.ConnAck => ConnAckProperties,
            MqttPacketType.Publish => PublishProperties,
            MqttPacketType.PubAck => AcknowledgementProperties,
            MqttPacketType.PubRec => AcknowledgementProperties,
            MqttPacketType.PubRel => AcknowledgementProperties,
            MqttPacketType.PubComp => AcknowledgementProperties,
            MqttPacketType.Subscribe => SubscribeProperties,
            MqttPacketType.SubAck => AcknowledgementProperties,
            MqttPacketType.Unsubscribe => UnsubscribeProperties,
            MqttPacketType.UnsubAck => AcknowledgementProperties,
            MqttPacketType.Disconnect => DisconnectProperties,
            _ => 0
        };
        return identifier < 64 && (properties & (1UL << identifier)) != 0;
    }

    public static bool IsAllowedWillProperty(byte identifier)
    {
        return identifier < 64 && (WillProperties & (1UL << identifier)) != 0;
    }

    private static ulong PropertyMask(params ReadOnlySpan<byte> identifiers)
    {
        ulong mask = 0;
        foreach (byte identifier in identifiers)
        {
            mask |= 1UL << identifier;
        }

        return mask;
    }
}
