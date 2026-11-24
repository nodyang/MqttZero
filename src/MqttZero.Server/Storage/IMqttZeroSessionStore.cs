namespace MqttZero.Storage;

/// <summary>Persists session metadata and subscriptions.</summary>
public interface IMqttZeroSessionStore
{
    /// <summary>Gets whether persistence is enabled.</summary>
    bool IsEnabled { get; }
    /// <summary>Loads all persisted sessions.</summary>
    ValueTask<IReadOnlyList<MqttZeroPersistedSession>> LoadAllAsync(CancellationToken cancellationToken);
    /// <summary>Saves one session.</summary>
    ValueTask SaveAsync(MqttZeroPersistedSession session, CancellationToken cancellationToken);
    /// <summary>Deletes one session.</summary>
    ValueTask DeleteAsync(string clientId, CancellationToken cancellationToken);
}
