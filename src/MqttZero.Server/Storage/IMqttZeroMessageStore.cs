namespace MqttZero.Server.Storage;

/// <summary>Persists queued, outbound, and inbound QoS 2 messages.</summary>
public interface IMqttZeroMessageStore
{
    /// <summary>Gets whether persistence is enabled.</summary>
    bool IsEnabled { get; }
    /// <summary>Loads all persisted messages.</summary>
    ValueTask<IReadOnlyList<MqttZeroPersistedMessage>> LoadAllAsync(CancellationToken cancellationToken);
    /// <summary>Saves a queued message.</summary>
    ValueTask SaveQueuedAsync(MqttZeroPersistedMessage message, CancellationToken cancellationToken);
    /// <summary>Saves an outbound in-flight message.</summary>
    ValueTask SaveOutboundAsync(MqttZeroPersistedMessage message, CancellationToken cancellationToken);
    /// <summary>Saves an inbound QoS 2 message.</summary>
    ValueTask SaveInboundQos2Async(MqttZeroPersistedMessage message, CancellationToken cancellationToken);
    /// <summary>Deletes an outbound message.</summary>
    ValueTask DeleteOutboundAsync(string clientId, long messageId, CancellationToken cancellationToken);
    /// <summary>Deletes an inbound QoS 2 message.</summary>
    ValueTask DeleteInboundQos2Async(string clientId, ushort packetIdentifier, CancellationToken cancellationToken);
}
