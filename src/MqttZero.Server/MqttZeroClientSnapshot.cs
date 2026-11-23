using System.Net;

namespace MqttZero;

/// <summary>
/// Represents a point-in-time view of a connected MQTT client.
/// </summary>
public sealed class MqttZeroClientSnapshot
{
    /// <summary>
    /// Gets the client identifier.
    /// </summary>
    public required string ClientId { get; init; }

    /// <summary>
    /// Gets the remote network endpoint.
    /// </summary>
    public EndPoint? RemoteEndPoint { get; init; }

    /// <summary>
    /// Gets a value indicating whether the connection uses TLS.
    /// </summary>
    public bool IsSecure { get; init; }

    /// <summary>
    /// Gets a value indicating whether the connection uses WebSocket.
    /// </summary>
    public bool IsWebSocket { get; init; }

    /// <summary>
    /// Gets the number of active subscriptions.
    /// </summary>
    public int SubscriptionCount { get; init; }
}
