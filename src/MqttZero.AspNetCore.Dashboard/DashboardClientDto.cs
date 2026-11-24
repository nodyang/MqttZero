using MqttZero.Server.Broker;
using MqttZero.Protocol;
namespace MqttZero.AspNetCore.Dashboard;

/// <summary>
/// Describes one connected client in the management dashboard.
/// </summary>
/// <param name="ClientId">The client identifier.</param>
/// <param name="RemoteEndPoint">The remote endpoint.</param>
/// <param name="IsSecure">Whether the connection uses TLS.</param>
/// <param name="IsWebSocket">Whether the connection uses WebSocket.</param>
/// <param name="SubscriptionCount">The number of active subscriptions.</param>
public sealed record DashboardClientDto(
    string ClientId,
    string? RemoteEndPoint,
    bool IsSecure,
    bool IsWebSocket,
    int SubscriptionCount);
