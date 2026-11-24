namespace MqttZero.Interceptors;

/// <summary>Authorizes one subscription filter.</summary>
public interface IMqttZeroSubscribeInterceptor
{
    /// <summary>Intercepts one subscription filter.</summary>
    ValueTask<MqttZeroInterceptionResult> InterceptAsync(
        MqttZeroSubscribeContext context,
        CancellationToken cancellationToken);
}
