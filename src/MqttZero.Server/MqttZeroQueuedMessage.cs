using System.Buffers;

namespace MqttZero;

internal sealed class MqttZeroQueuedMessage
{
    public MqttZeroQueuedMessage(
        ReadOnlySequence<byte> topic,
        ReadOnlySequence<byte> payload,
        ReadOnlySequence<byte> properties,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retain,
        uint? subscriptionIdentifier,
        DateTimeOffset? expiresAt)
    {
        Topic = topic.ToArray();
        Payload = payload.ToArray();
        Properties = properties.ToArray();
        QualityOfServiceLevel = qualityOfServiceLevel;
        Retain = retain;
        SubscriptionIdentifier = subscriptionIdentifier;
        ExpiresAt = expiresAt;
    }

    public byte[] Topic { get; }

    public byte[] Payload { get; }

    public byte[] Properties { get; }

    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; }

    public bool Retain { get; }

    public uint? SubscriptionIdentifier { get; }

    public DateTimeOffset? ExpiresAt { get; }

    public ushort PacketIdentifier { get; set; }

    public bool IsExpired(DateTimeOffset now)
    {
        return ExpiresAt.HasValue && ExpiresAt.Value <= now;
    }

    public MqttPublishPacket CreatePacket(bool duplicate)
    {
        return new MqttPublishPacket
        {
            TopicName = new ReadOnlySequence<byte>(Topic),
            Payload = new ReadOnlySequence<byte>(Payload),
            Properties = Properties.Length == 0
                ? ReadOnlySequence<byte>.Empty
                : new ReadOnlySequence<byte>(Properties),
            SubscriptionIdentifier = SubscriptionIdentifier,
            QualityOfServiceLevel = QualityOfServiceLevel,
            PacketIdentifier = PacketIdentifier,
            Duplicate = duplicate,
            Retain = Retain
        };
    }
}
