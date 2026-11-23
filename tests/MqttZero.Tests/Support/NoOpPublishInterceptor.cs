using MqttZero.Interceptors;

namespace MqttZero.Tests.Support
{
    internal sealed class NoOpPublishInterceptor : IMqttZeroPublishInterceptor
    {
        public ValueTask<MqttZeroInterceptionResult> InterceptAsync(
            MqttZeroPublishContext context,
            CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(MqttZeroInterceptionResult.Continue);
        }
    }

}
