namespace MqttZero.Server.Observers;

/// <summary>Observes delivery and queue outcomes.</summary>
public interface IMqttZeroDeliveryObserver
{
    /// <summary>Called after a delivery is queued or made inflight.</summary>
    ValueTask OnMessageEnqueuedAsync(MqttZeroMessageEnqueuedContext context, CancellationToken cancellationToken);
    /// <summary>Called when a delivery is dropped.</summary>
    ValueTask OnMessageDroppedAsync(MqttZeroMessageDroppedContext context, CancellationToken cancellationToken);
    /// <summary>Called before a queued message is overwritten.</summary>
    ValueTask OnQueueMessageOverwrittenAsync(MqttZeroQueueMessageOverwrittenContext context, CancellationToken cancellationToken);
    /// <summary>Called when no subscriber consumes a publish.</summary>
    ValueTask OnMessageNotConsumedAsync(MqttZeroMessageNotConsumedContext context, CancellationToken cancellationToken);
}
