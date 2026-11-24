using MqttZero.Protocol;

namespace MqttZero.Storage;

/// <summary>Describes a persisted broker message.</summary>
public sealed class MqttZeroPersistedMessage
{
    /// <summary>Gets the stable message identifier.</summary>
    public long MessageId { get; init; }
    /// <summary>Gets the owning client identifier.</summary>
    public required string ClientId { get; init; }
    /// <summary>Gets the message category.</summary>
    public MqttZeroPersistedMessageKind Kind { get; init; }
    /// <summary>Gets the delivery state.</summary>
    public MqttZeroPersistedDeliveryState State { get; init; }
    /// <summary>Gets the packet identifier.</summary>
    public ushort PacketIdentifier { get; init; }
    /// <summary>Gets the topic.</summary>
    public required ReadOnlyMemory<byte> Topic { get; init; }
    /// <summary>Gets the properties.</summary>
    public ReadOnlyMemory<byte> Properties { get; init; }
    /// <summary>Gets the payload.</summary>
    public ReadOnlyMemory<byte> Payload { get; init; }
    /// <summary>Gets the QoS.</summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; init; }
    /// <summary>Gets whether RETAIN is set.</summary>
    public bool Retain { get; init; }
    /// <summary>Gets the subscription identifier.</summary>
    public uint? SubscriptionIdentifier { get; init; }
    /// <summary>Gets the expiration time.</summary>
    public DateTimeOffset? ExpiresAt { get; init; }
}
