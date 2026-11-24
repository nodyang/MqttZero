using MqttZero.Protocol;
using MqttZero.Server.Sessions;
using System.Collections.Concurrent;

namespace MqttZero.Server.Routing;

internal sealed class MqttZeroRouteScopePool : IDisposable
{
    private readonly ConcurrentQueue<MqttZeroRouteScope> _scopes = new();
    private int _disposed;

    public MqttZeroRouteScope Rent()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
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
        if (Volatile.Read(ref _disposed) != 0)
        {
            scope.ReleaseBuffers();
            return;
        }

        _scopes.Enqueue(scope);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        while (_scopes.TryDequeue(out MqttZeroRouteScope? scope))
        {
            scope.ReleaseBuffers();
        }
    }
}
