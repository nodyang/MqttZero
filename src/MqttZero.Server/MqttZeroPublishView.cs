using System.Buffers;

namespace MqttZero;

internal readonly ref struct MqttZeroPublishView
{
    public MqttZeroPublishView(
        ReadOnlySequence<byte> topic,
        ReadOnlySequence<byte> properties,
        ReadOnlySequence<byte> payload,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        ushort packetIdentifier,
        bool duplicate,
        bool retain,
        uint? messageExpiryInterval,
        uint? subscriptionIdentifier,
        ushort? topicAlias)
    {
        Topic = topic;
        Properties = properties;
        Payload = payload;
        QualityOfServiceLevel = qualityOfServiceLevel;
        PacketIdentifier = packetIdentifier;
        Duplicate = duplicate;
        Retain = retain;
        MessageExpiryInterval = messageExpiryInterval;
        SubscriptionIdentifier = subscriptionIdentifier;
        TopicAlias = topicAlias;
    }

    public ReadOnlySequence<byte> Topic { get; }

    public ReadOnlySequence<byte> Properties { get; }

    public ReadOnlySequence<byte> Payload { get; }

    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; }

    public ushort PacketIdentifier { get; }

    public bool Duplicate { get; }

    public bool Retain { get; }

    public uint? MessageExpiryInterval { get; }

    public uint? SubscriptionIdentifier { get; }

    public ushort? TopicAlias { get; }
}
