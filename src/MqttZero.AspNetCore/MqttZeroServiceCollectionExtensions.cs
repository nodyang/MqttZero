using MqttZero.Broker;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace MqttZero.AspNetCore;

/// <summary>
/// Registers MQTTZero services.
/// </summary>
public static class MqttZeroServiceCollectionExtensions
{
    /// <summary>
    /// Adds the MQTTZero broker and Kestrel hosting services.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">An optional broker configuration callback.</param>
    /// <returns>A builder for additional MQTTZero configuration.</returns>
    public static IMqttZeroBuilder AddMqttZero(
        this IServiceCollection services,
        Action<MqttZeroOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddOptions();
        services.AddConnections();
        if (configure != null)
        {
            services.Configure(configure);
        }

        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<MqttZeroBroker>();
        services.TryAddSingleton<MqttZeroConnectionHandler>();
        services.AddHostedService<MqttZeroHostedService>();

        return new MqttZeroBuilder(services);
    }
}
