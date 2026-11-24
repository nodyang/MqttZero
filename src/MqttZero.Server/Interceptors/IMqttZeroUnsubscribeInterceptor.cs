namespace MqttZero.Server.Interceptors;

/// <summary>Authorizes one unsubscribe filter.</summary>
public interface IMqttZeroUnsubscribeInterceptor
{
    /// <summary>Intercepts one unsubscribe filter.</summary>
    ValueTask<MqttZeroInterceptionResult> InterceptAsync(
        MqttZeroUnsubscribeContext context,
        CancellationToken cancellationToken);
}
