using MqttZero.Events;
using MqttZero.Protocol;
using System.Buffers;

namespace MqttZero.Interceptors;

/// <summary>Contains delivery authorization metadata.</summary>
public sealed class MqttZeroEnqueueContext
{
    /// <summary>Gets the sender information.</summary>
    public MqttZeroClientInfo? Sender { get; init; }
    /// <summary>Gets the receiver information.</summary>
    public required MqttZeroClientInfo Receiver { get; init; }
    /// <summary>Gets the UTF-8 topic.</summary>
    public required ReadOnlySequence<byte> Topic { get; init; }
    /// <summary>Gets the payload length.</summary>
    public long PayloadLength { get; init; }
    /// <summary>Gets the delivery QoS.</summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; init; }
    /// <summary>Gets whether RETAIN is set.</summary>
    public bool Retain { get; init; }
    /// <summary>Gets the subscription identifier.</summary>
    public uint? SubscriptionIdentifier { get; init; }
}
