using System.Threading.Channels;

namespace MqttZero.Storage;

internal sealed class MqttZeroPersistenceWriter : IMqttZeroPersistenceWriter
{
    private readonly IMqttZeroSessionStore _sessionStore;
    private readonly IMqttZeroMessageStore _messageStore;
    private readonly IMqttZeroRetainedMessageStore _retainedStore;
    private readonly Channel<Operation> _channel;
    private Task? _worker;
    private long _failureCount;

    public MqttZeroPersistenceWriter(
        IMqttZeroSessionStore sessionStore,
        IMqttZeroMessageStore messageStore,
        IMqttZeroRetainedMessageStore retainedStore)
    {
        _sessionStore = sessionStore;
        _messageStore = messageStore;
        _retainedStore = retainedStore;
        IsEnabled = sessionStore.IsEnabled || messageStore.IsEnabled || retainedStore.IsEnabled;
        _channel = Channel.CreateUnbounded<Operation>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });
    }

    public bool IsEnabled { get; }

    public long FailureCount
    {
        get
        {
            return Interlocked.Read(ref _failureCount);
        }
    }

    public ValueTask StartAsync(CancellationToken cancellationToken)
    {
        if (!IsEnabled || _worker != null)
        {
            return ValueTask.CompletedTask;
        }

        _worker = ProcessAsync();
        return ValueTask.CompletedTask;
    }

    public async ValueTask StopAsync(CancellationToken cancellationToken)
    {
        _channel.Writer.TryComplete();
        if (_worker != null)
        {
            await _worker.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public ValueTask EnqueueSessionSaveAsync(MqttZeroPersistedSession session, CancellationToken cancellationToken)
    {
        return Enqueue(new Operation(OperationKind.SessionSave) { Session = session });
    }

    public ValueTask EnqueueSessionDeleteAsync(string clientId, CancellationToken cancellationToken)
    {
        return Enqueue(new Operation(OperationKind.SessionDelete) { ClientId = clientId });
    }

    public ValueTask EnqueueRetainedSaveAsync(MqttZeroPersistedRetainedMessage message, CancellationToken cancellationToken)
    {
        return Enqueue(new Operation(OperationKind.RetainedSave) { Retained = message });
    }

    public ValueTask EnqueueRetainedDeleteAsync(string topic, CancellationToken cancellationToken)
    {
        return Enqueue(new Operation(OperationKind.RetainedDelete) { Topic = topic });
    }

    public ValueTask EnqueueRetainedClearAsync(CancellationToken cancellationToken)
    {
        return Enqueue(new Operation(OperationKind.RetainedClear));
    }

    public ValueTask EnqueueQueuedMessageAsync(MqttZeroPersistedMessage message, CancellationToken cancellationToken)
    {
        return Enqueue(new Operation(OperationKind.MessageQueued) { Message = message });
    }

    public ValueTask EnqueueOutboundMessageAsync(MqttZeroPersistedMessage message, CancellationToken cancellationToken)
    {
        return Enqueue(new Operation(OperationKind.MessageOutbound) { Message = message });
    }

    public ValueTask EnqueueInboundQos2Async(MqttZeroPersistedMessage message, CancellationToken cancellationToken)
    {
        return Enqueue(new Operation(OperationKind.MessageInboundQos2) { Message = message });
    }

    public ValueTask EnqueueOutboundDeleteAsync(string clientId, long messageId, CancellationToken cancellationToken)
    {
        return Enqueue(new Operation(OperationKind.MessageOutboundDelete)
        {
            ClientId = clientId,
            MessageId = messageId
        });
    }

    public ValueTask EnqueueInboundQos2DeleteAsync(string clientId, ushort packetIdentifier, CancellationToken cancellationToken)
    {
        return Enqueue(new Operation(OperationKind.MessageInboundQos2Delete)
        {
            ClientId = clientId,
            PacketIdentifier = packetIdentifier
        });
    }

    private ValueTask Enqueue(Operation operation)
    {
        if (!IsEnabled)
        {
            return ValueTask.CompletedTask;
        }

        return _channel.Writer.TryWrite(operation)
            ? ValueTask.CompletedTask
            : WriteAsync(operation);
    }

    private async ValueTask WriteAsync(Operation operation)
    {
        await _channel.Writer.WriteAsync(operation).ConfigureAwait(false);
    }

    private async Task ProcessAsync()
    {
        await foreach (Operation operation in _channel.Reader.ReadAllAsync().ConfigureAwait(false))
        {
            try
            {
                await ExecuteAsync(operation).ConfigureAwait(false);
            }
            catch
            {
                Interlocked.Increment(ref _failureCount);
            }
        }
    }

    private ValueTask ExecuteAsync(Operation operation)
    {
        return operation.Kind switch
        {
            OperationKind.SessionSave => _sessionStore.SaveAsync(operation.Session!, CancellationToken.None),
            OperationKind.SessionDelete => _sessionStore.DeleteAsync(operation.ClientId!, CancellationToken.None),
            OperationKind.RetainedSave => _retainedStore.SaveAsync(operation.Retained!, CancellationToken.None),
            OperationKind.RetainedDelete => _retainedStore.DeleteAsync(operation.Topic!, CancellationToken.None),
            OperationKind.RetainedClear => _retainedStore.ClearAsync(CancellationToken.None),
            OperationKind.MessageQueued => _messageStore.SaveQueuedAsync(operation.Message!, CancellationToken.None),
            OperationKind.MessageOutbound => _messageStore.SaveOutboundAsync(operation.Message!, CancellationToken.None),
            OperationKind.MessageInboundQos2 => _messageStore.SaveInboundQos2Async(operation.Message!, CancellationToken.None),
            OperationKind.MessageOutboundDelete => _messageStore.DeleteOutboundAsync(operation.ClientId!, operation.MessageId, CancellationToken.None),
            OperationKind.MessageInboundQos2Delete => _messageStore.DeleteInboundQos2Async(operation.ClientId!, operation.PacketIdentifier, CancellationToken.None),
            _ => ValueTask.CompletedTask
        };
    }

    private enum OperationKind
    {
        SessionSave,
        SessionDelete,
        RetainedSave,
        RetainedDelete,
        RetainedClear,
        MessageQueued,
        MessageOutbound,
        MessageInboundQos2,
        MessageOutboundDelete,
        MessageInboundQos2Delete
    }

    private sealed class Operation
    {
        public Operation(OperationKind kind)
        {
            Kind = kind;
        }

        public OperationKind Kind { get; }
        public MqttZeroPersistedSession? Session { get; init; }
        public MqttZeroPersistedMessage? Message { get; init; }
        public MqttZeroPersistedRetainedMessage? Retained { get; init; }
        public string? ClientId { get; init; }
        public string? Topic { get; init; }
        public long MessageId { get; init; }
        public ushort PacketIdentifier { get; init; }
    }
}
