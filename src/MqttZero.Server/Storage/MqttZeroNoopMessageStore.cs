namespace MqttZero.Storage;

internal sealed class MqttZeroNoopMessageStore : IMqttZeroMessageStore
{
    public bool IsEnabled
    {
        get
        {
            return false;
        }
    }
    public ValueTask<IReadOnlyList<MqttZeroPersistedMessage>> LoadAllAsync(CancellationToken cancellationToken)
    {
        return ValueTask.FromResult<IReadOnlyList<MqttZeroPersistedMessage>>([]);
    }

    public ValueTask SaveQueuedAsync(MqttZeroPersistedMessage message, CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }

    public ValueTask SaveOutboundAsync(MqttZeroPersistedMessage message, CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }

    public ValueTask SaveInboundQos2Async(MqttZeroPersistedMessage message, CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }

    public ValueTask DeleteOutboundAsync(string clientId, long messageId, CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }

    public ValueTask DeleteInboundQos2Async(string clientId, ushort packetIdentifier, CancellationToken cancellationToken)
    {
        return ValueTask.CompletedTask;
    }
}
