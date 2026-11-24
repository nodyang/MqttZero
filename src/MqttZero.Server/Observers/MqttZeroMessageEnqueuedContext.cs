using MqttZero.Events;
using MqttZero.Protocol;
using System.Buffers;

namespace MqttZero.Observers;

/// <summary>Describes an enqueued delivery.</summary>
public sealed class MqttZeroMessageEnqueuedContext
{
    /// <summary>Gets the sender information.</summary>
    public MqttZeroClientInfo? Sender { get; init; }
    /// <summary>Gets the receiver information.</summary>
    public required MqttZeroClientInfo Receiver { get; init; }
    /// <summary>Gets the topic.</summary>
    public required ReadOnlySequence<byte> Topic { get; init; }
    /// <summary>Gets the payload length.</summary>
    public long PayloadLength { get; init; }
    /// <summary>Gets the QoS.</summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; init; }
}
