using MqttZero.Server.Broker;
using MqttZero.Protocol;
namespace MqttZero.AspNetCore.Dashboard;

/// <summary>
/// Provides dashboard overview values.
/// </summary>
/// <param name="State">The current broker state.</param>
/// <param name="ActiveConnections">The current active transport connection count.</param>
/// <param name="ConnectedClients">The current connected client count.</param>
public sealed record DashboardOverviewDto(
    string State,
    int ActiveConnections,
    int ConnectedClients);
