namespace MqttZero.Server.Storage;

internal sealed class MqttZeroNoopSessionStore : IMqttZeroSessionStore
{
    public bool IsEnabled
    {
        get
        {
            return false;
        }
    }
    public ValueTask<IReadOnlyList<MqttZeroPersistedSession>> LoadAllAsync(CancellationToken cancellationToken)
    {
        return ValueTask.FromResult<IReadOnlyList<MqttZeroPersistedSession>>([]);
    }

    public ValueTask SaveAsync(MqttZeroPersistedSession session, CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }

    public ValueTask DeleteAsync(string clientId, CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }
}
