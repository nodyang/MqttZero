using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace MqttZero.AspNetCore;

/// <summary>
/// Registers MQTTZero health checks.
/// </summary>
public static class MqttZeroHealthChecksBuilderExtensions
{
    /// <summary>
    /// Adds a readiness check for the MQTTZero broker.
    /// </summary>
    /// <param name="builder">The health checks builder.</param>
    /// <param name="name">The health check name.</param>
    /// <param name="failureStatus">The status used for broker failure.</param>
    /// <param name="tags">Optional health check tags.</param>
    /// <param name="timeout">An optional health check timeout.</param>
    /// <returns>The same health checks builder.</returns>
    public static IHealthChecksBuilder AddMqttZeroHealthCheck(
        this IHealthChecksBuilder builder,
        string name = "mqttzero",
        HealthStatus? failureStatus = null,
        IEnumerable<string>? tags = null,
        TimeSpan? timeout = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.AddCheck<MqttZeroHealthCheck>(
            name,
            failureStatus,
            tags ?? [],
            timeout);
    }
}
