using System.Collections.Concurrent;

namespace MqttZero;

internal sealed class MqttZeroRouteScopePool
{
    private readonly ConcurrentQueue<MqttZeroRouteScope> _scopes = new();

    public MqttZeroRouteScope Rent()
    {
        if (_scopes.TryDequeue(out MqttZeroRouteScope? scope))
        {
            scope.SetOwner(this);
            return scope;
        }

        MqttZeroRouteScope created = new();
        created.SetOwner(this);
        return created;
    }

    public void Return(MqttZeroRouteScope scope)
    {
        _scopes.Enqueue(scope);
    }
}
