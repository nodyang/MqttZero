using MqttZero.Clients;
using MqttZero.Events;
using System.Buffers;

namespace MqttZero.Observers;

/// <summary>Describes a dropped delivery.</summary>
public sealed class MqttZeroMessageDroppedContext
{
    /// <summary>Gets the connected sender.</summary>
    public MqttZeroConnectedClient? Sender { get; init; }
    /// <summary>Gets the receiver information.</summary>
    public MqttZeroClientInfo? ReceiverInfo { get; init; }
    /// <summary>Gets the connected receiver when online.</summary>
    public MqttZeroConnectedClient? Receiver { get; init; }
    /// <summary>Gets the topic.</summary>
    public required ReadOnlySequence<byte> Topic { get; init; }
    /// <summary>Gets the drop reason.</summary>
    public MqttZeroMessageDropReason Reason { get; init; }
}
