using MqttZero.Server.Broker;
using MqttZero.Protocol;
namespace MqttZero.AspNetCore.Dashboard;

/// <summary>
/// Configures the MQTTZero management dashboard.
/// </summary>
public sealed class MqttZeroDashboardOptions
{
    /// <summary>
    /// Gets or sets the authorization policy required for read-only dashboard access.
    /// </summary>
    public string ReadAuthorizationPolicy { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the authorization policy required for management operations.
    /// </summary>
    public string OperateAuthorizationPolicy { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the maximum number of retained dashboard events.
    /// </summary>
    public int EventBufferCapacity { get; set; } = 10_000;

    /// <summary>
    /// Gets or sets the interval used to batch real-time events.
    /// </summary>
    public TimeSpan EventBatchInterval { get; set; } = TimeSpan.FromMilliseconds(250);

    /// <summary>
    /// Gets or sets the maximum payload bytes returned by a preview request.
    /// </summary>
    public int PayloadPreviewLimit { get; set; } = 64 * 1024;

    /// <summary>
    /// Gets or sets the default page size for list APIs.
    /// </summary>
    public int DefaultPageSize { get; set; } = 50;

    /// <summary>
    /// Gets or sets the maximum page size for list APIs.
    /// </summary>
    public int MaxPageSize { get; set; } = 200;
}
