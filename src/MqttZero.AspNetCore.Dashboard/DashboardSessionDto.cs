using MqttZero.Server.Broker;
using MqttZero.Protocol;
namespace MqttZero.AspNetCore.Dashboard;

/// <summary>
/// Describes one MQTT session in the management dashboard.
/// </summary>
/// <param name="ClientId">The client identifier.</param>
/// <param name="IsConnected">Whether the session has a network connection.</param>
/// <param name="CleanStart">The latest Clean Start flag.</param>
/// <param name="SessionPresent">Whether the session can be restored.</param>
/// <param name="ExpiryIntervalSeconds">The session expiry interval.</param>
/// <param name="ExpiresAt">The next expiry time when disconnected.</param>
/// <param name="SubscriptionCount">The subscription count.</param>
/// <param name="PendingMessageCount">The pending message count.</param>
/// <param name="InflightMessageCount">The in-flight message count.</param>
/// <param name="HasWill">Whether a will message is stored.</param>
/// <param name="WillTopic">The stored will topic.</param>
public sealed record DashboardSessionDto(
    string ClientId,
    bool IsConnected,
    bool CleanStart,
    bool SessionPresent,
    uint ExpiryIntervalSeconds,
    DateTimeOffset? ExpiresAt,
    int SubscriptionCount,
    int PendingMessageCount,
    int InflightMessageCount,
    bool HasWill,
    string? WillTopic);
