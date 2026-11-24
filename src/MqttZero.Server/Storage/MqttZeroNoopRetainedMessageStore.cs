namespace MqttZero.Server.Storage;

internal sealed class MqttZeroNoopRetainedMessageStore : IMqttZeroRetainedMessageStore
{
    public bool IsEnabled
    {
        get
        {
            return false;
        }
    }
    public ValueTask<IReadOnlyList<MqttZeroPersistedRetainedMessage>> LoadAllAsync(CancellationToken cancellationToken)
    {
        return ValueTask.FromResult<IReadOnlyList<MqttZeroPersistedRetainedMessage>>([]);
    }

    public ValueTask SaveAsync(MqttZeroPersistedRetainedMessage message, CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }

    public ValueTask DeleteAsync(string topic, CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }

    public ValueTask ClearAsync(CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }
}
