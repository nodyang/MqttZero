using MqttZero.Events;
using System.Buffers;

namespace MqttZero.Observers;

/// <summary>Describes an overwritten queued message.</summary>
public sealed class MqttZeroQueueMessageOverwrittenContext
{
    /// <summary>Gets the receiver information.</summary>
    public required MqttZeroClientInfo Receiver { get; init; }
    /// <summary>Gets the overwritten topic.</summary>
    public required ReadOnlySequence<byte> Topic { get; init; }
}
