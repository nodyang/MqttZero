using MqttZero.Server.Clients;

namespace MqttZero.Server.Observers;

/// <summary>Describes a connected client.</summary>
public sealed class MqttZeroClientConnectedContext
{
    /// <summary>Gets the connected client control handle.</summary>
    public required MqttZeroConnectedClient ConnectedClient { get; init; }
    /// <summary>Gets whether a persistent session was resumed.</summary>
    public bool SessionPresent { get; init; }
}
