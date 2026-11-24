namespace MqttZero.Server.Interceptors;

/// <summary>Controls delivery to one receiver.</summary>
public interface IMqttZeroEnqueueInterceptor
{
    /// <summary>Intercepts one receiver delivery.</summary>
    ValueTask<MqttZeroEnqueueInterceptionResult> InterceptAsync(
        MqttZeroEnqueueContext context,
        CancellationToken cancellationToken);
}
