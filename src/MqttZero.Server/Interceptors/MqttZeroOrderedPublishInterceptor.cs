using MqttZero.Protocol;
namespace MqttZero.Interceptors;

internal sealed class MqttZeroOrderedPublishInterceptor : IMqttZeroPublishInterceptor
{
    private readonly IMqttZeroPublishInterceptor _interceptor;

    public MqttZeroOrderedPublishInterceptor(
        IMqttZeroPublishInterceptor interceptor,
        int order)
    {
        _interceptor = interceptor;
        Order = order;
    }

    public int Order { get; }

    public ValueTask<MqttZeroInterceptionResult> InterceptAsync(
        MqttZeroPublishContext context,
        CancellationToken cancellationToken)
    {
        return _interceptor.InterceptAsync(context, cancellationToken);
    }
}
