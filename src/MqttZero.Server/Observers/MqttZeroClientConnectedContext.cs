using MqttZero.Events;

namespace MqttZero.Observers;

/// <summary>Describes a connected client.</summary>
public sealed class MqttZeroClientConnectedContext
{
    /// <summary>Gets the client information.</summary>
    public required MqttZeroClientInfo Client { get; init; }
    /// <summary>Gets whether a persistent session was resumed.</summary>
    public bool SessionPresent { get; init; }
}
