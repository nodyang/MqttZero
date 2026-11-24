namespace MqttZero.Server.Interceptors;

/// <summary>Validates CONNECT requests before sessions are changed.</summary>
public interface IMqttZeroConnectionInterceptor
{
    /// <summary>Intercepts one CONNECT request.</summary>
    ValueTask<MqttZeroConnectionInterceptionResult> InterceptAsync(
        MqttZeroConnectionContext context,
        CancellationToken cancellationToken);
}
