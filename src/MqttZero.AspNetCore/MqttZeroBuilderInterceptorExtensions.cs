using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace MqttZero.AspNetCore;

/// <summary>
/// Registers MQTTZero pipeline extensions.
/// </summary>
public static class MqttZeroBuilderInterceptorExtensions
{
    /// <summary>
    /// Registers an MQTT publish interceptor.
    /// </summary>
    /// <typeparam name="TInterceptor">The interceptor implementation.</typeparam>
    /// <param name="builder">The MQTTZero builder.</param>
    /// <param name="order">The registration order hint.</param>
    /// <returns>The same builder.</returns>
    public static IMqttZeroBuilder AddInterceptor<TInterceptor>(
        this IMqttZeroBuilder builder,
        int order = 0)
        where TInterceptor : class, IMqttZeroPublishInterceptor
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.Services.TryAddSingleton<TInterceptor>();
        builder.Services.AddSingleton<IMqttZeroPublishInterceptor>(
            provider => new MqttZeroOrderedPublishInterceptor(
                provider.GetRequiredService<TInterceptor>(),
                order));
        return builder;
    }
}
