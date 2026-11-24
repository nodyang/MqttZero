using MqttZero.Events;
using System.Buffers;

namespace MqttZero.Observers;

/// <summary>Describes a dropped delivery.</summary>
public sealed class MqttZeroMessageDroppedContext
{
    /// <summary>Gets the sender information.</summary>
    public MqttZeroClientInfo? Sender { get; init; }
    /// <summary>Gets the receiver information.</summary>
    public MqttZeroClientInfo? Receiver { get; init; }
    /// <summary>Gets the topic.</summary>
    public required ReadOnlySequence<byte> Topic { get; init; }
    /// <summary>Gets the drop reason.</summary>
    public MqttZeroMessageDropReason Reason { get; init; }
}
