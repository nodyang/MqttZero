using System.Buffers;
using System.IO.Pipelines;

namespace MqttZero;

internal sealed class MqttZeroClientSession : IAsyncDisposable
{
    private readonly ArrayBufferWriter<byte> _bufferWriter = new();
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly PipeWriter _writer;
    private int _nextPacketIdentifier;

    public MqttZeroClientSession(
        string clientId,
        PipeWriter writer,
        string? remoteEndPoint,
        bool isSecure,
        bool isWebSocket)
    {
        ClientId = clientId;
        _writer = writer;
        RemoteEndPoint = remoteEndPoint;
        IsSecure = isSecure;
        IsWebSocket = isWebSocket;
    }

    public string ClientId { get; }

    public string? RemoteEndPoint { get; }

    public bool IsSecure { get; }

    public bool IsWebSocket { get; }

    public List<MqttZeroSubscription> Subscriptions { get; } = [];

    public object SubscriptionLock { get; } = new();

    public ushort GetNextPacketIdentifier()
    {
        ushort identifier = (ushort)Interlocked.Increment(ref _nextPacketIdentifier);
        if (identifier == 0)
        {
            identifier = (ushort)Interlocked.Increment(ref _nextPacketIdentifier);
        }

        return identifier;
    }

    public async ValueTask SendAsync(MqttPacket packet, CancellationToken cancellationToken)
    {
        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _bufferWriter.Clear();
            MqttPacketCodec.Encode(packet, _bufferWriter);
            _ = await _writer.WriteAsync(_bufferWriter.WrittenMemory, cancellationToken).ConfigureAwait(false);
            _ = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        _writeLock.Dispose();
        await _writer.CompleteAsync().ConfigureAwait(false);
    }
}
