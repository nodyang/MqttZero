namespace MqttZero.Server.Storage;

/// <summary>Queues persistence writes behind the broker hot path.</summary>
public interface IMqttZeroPersistenceWriter
{
    /// <summary>Gets whether write-behind persistence is enabled.</summary>
    bool IsEnabled { get; }
    /// <summary>Gets the number of failed persistence writes.</summary>
    long FailureCount { get; }
    /// <summary>Starts persistence processing.</summary>
    ValueTask StartAsync(CancellationToken cancellationToken);
    /// <summary>Stops persistence processing after queued writes complete.</summary>
    ValueTask StopAsync(CancellationToken cancellationToken);
    /// <summary>Queues a session save.</summary>
    ValueTask EnqueueSessionSaveAsync(MqttZeroPersistedSession session, CancellationToken cancellationToken);
    /// <summary>Queues a session delete.</summary>
    ValueTask EnqueueSessionDeleteAsync(string clientId, CancellationToken cancellationToken);
    /// <summary>Queues a retained-message save.</summary>
    ValueTask EnqueueRetainedSaveAsync(MqttZeroPersistedRetainedMessage message, CancellationToken cancellationToken);
    /// <summary>Queues a retained-message delete.</summary>
    ValueTask EnqueueRetainedDeleteAsync(string topic, CancellationToken cancellationToken);
    /// <summary>Queues a retained-message clear.</summary>
    ValueTask EnqueueRetainedClearAsync(CancellationToken cancellationToken);
    /// <summary>Queues a queued-message save.</summary>
    ValueTask EnqueueQueuedMessageAsync(MqttZeroPersistedMessage message, CancellationToken cancellationToken);
    /// <summary>Queues an outbound-message save.</summary>
    ValueTask EnqueueOutboundMessageAsync(MqttZeroPersistedMessage message, CancellationToken cancellationToken);
    /// <summary>Queues an inbound QoS 2 save.</summary>
    ValueTask EnqueueInboundQos2Async(MqttZeroPersistedMessage message, CancellationToken cancellationToken);
    /// <summary>Queues an outbound-message delete.</summary>
    ValueTask EnqueueOutboundDeleteAsync(string clientId, long messageId, CancellationToken cancellationToken);
    /// <summary>Queues an inbound QoS 2 delete.</summary>
    ValueTask EnqueueInboundQos2DeleteAsync(string clientId, ushort packetIdentifier, CancellationToken cancellationToken);
}
