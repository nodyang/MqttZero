using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using Microsoft.Extensions.DependencyInjection;

namespace MqttZero.AspNetCore;

/// <summary>
/// Exposes service registration capabilities for MQTTZero.
/// </summary>
public interface IMqttZeroBuilder
{
    /// <summary>
    /// Gets the service collection used by the builder.
    /// </summary>
    IServiceCollection Services { get; }
}
