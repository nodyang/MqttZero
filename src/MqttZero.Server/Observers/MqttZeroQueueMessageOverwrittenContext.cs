using MqttZero.Clients;
using MqttZero.Events;
using System.Buffers;

namespace MqttZero.Observers;

/// <summary>Describes an overwritten queued message.</summary>
public sealed class MqttZeroQueueMessageOverwrittenContext
{
    /// <summary>Gets the receiver information.</summary>
    public required MqttZeroClientInfo ReceiverInfo { get; init; }
    /// <summary>Gets the connected receiver when online.</summary>
    public MqttZeroConnectedClient? Receiver { get; init; }
    /// <summary>Gets the overwritten topic.</summary>
    public required ReadOnlySequence<byte> Topic { get; init; }
}
