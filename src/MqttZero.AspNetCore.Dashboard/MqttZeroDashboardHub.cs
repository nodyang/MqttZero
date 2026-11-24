using MqttZero.Server.Broker;
using MqttZero.Protocol;
using Microsoft.AspNetCore.SignalR;

namespace MqttZero.AspNetCore.Dashboard;

/// <summary>
/// Pushes management events to authenticated dashboard clients.
/// </summary>
public sealed class MqttZeroDashboardHub : Hub
{
    private readonly DateTimeOffset _createdAt = DateTimeOffset.UtcNow;

    /// <summary>
    /// Returns the current server timestamp for connection diagnostics.
    /// </summary>
    /// <returns>The current UTC timestamp.</returns>
    public DateTimeOffset Ping()
    {
        return _createdAt;
    }
}
