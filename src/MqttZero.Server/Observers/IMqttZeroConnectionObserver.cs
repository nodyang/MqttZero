using MqttZero.Events;
using MqttZero.Protocol;

namespace MqttZero.Observers;

/// <summary>Observes client connection lifecycle events.</summary>
public interface IMqttZeroConnectionObserver
{
    /// <summary>Called after a client is connected.</summary>
    ValueTask OnConnectedAsync(
        MqttZeroClientConnectedContext context,
        CancellationToken cancellationToken);
    /// <summary>Called after a client connection ends.</summary>
    ValueTask OnDisconnectedAsync(
        MqttZeroClientDisconnectedContext context,
        CancellationToken cancellationToken);
}
