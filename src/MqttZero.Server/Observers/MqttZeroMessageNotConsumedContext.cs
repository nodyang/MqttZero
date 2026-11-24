using MqttZero.Events;
using System.Buffers;

namespace MqttZero.Observers;

/// <summary>Describes a publish with no matching subscriber.</summary>
public sealed class MqttZeroMessageNotConsumedContext
{
    /// <summary>Gets the sender information.</summary>
    public MqttZeroClientInfo? Sender { get; init; }
    /// <summary>Gets the topic.</summary>
    public required ReadOnlySequence<byte> Topic { get; init; }
    /// <summary>Gets the payload length.</summary>
    public long PayloadLength { get; init; }
}
