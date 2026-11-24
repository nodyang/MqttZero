using System.Net;

namespace MqttZero.AspNetCore;

/// <summary>
/// Describes transport details available while accepting a MQTTZero connection.
/// </summary>
public sealed class MqttZeroTransportInfo
{
    /// <summary>
    /// Gets the remote endpoint.
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
}
