using System.Buffers;
using System.IO.Pipelines;

namespace MqttZero;

internal sealed class MqttZeroClientConnection : IAsyncDisposable
{
    private readonly ArrayBufferWriter<byte> _bufferWriter = new();
    private readonly CancellationTokenSource _closed = new();
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly PipeWriter _writer;

    public MqttZeroClientConnection(
        PipeWriter writer,
        string? remoteEndPoint,
        bool isSecure,
        bool isWebSocket,
        ushort keepAliveSeconds,
        ushort receiveMaximum,
        uint maximumPacketSize,
        TimeProvider timeProvider)
    {
        _writer = writer;
        RemoteEndPoint = remoteEndPoint;
        IsSecure = isSecure;
        IsWebSocket = isWebSocket;
        KeepAliveSeconds = keepAliveSeconds;
        ReceiveMaximum = receiveMaximum;
        MaximumPacketSize = maximumPacketSize;
        LastPacketReceivedAt = timeProvider.GetUtcNow();
    }

    public string? RemoteEndPoint { get; }

    public bool IsSecure { get; }

    public bool IsWebSocket { get; }

    public ushort KeepAliveSeconds { get; }

    public ushort ReceiveMaximum { get; }

    public uint MaximumPacketSize { get; }

    public Dictionary<ushort, byte[]> InboundTopicAliases { get; } = [];

    public DateTimeOffset LastPacketReceivedAt { get; private set; }

    public CancellationToken ClosedToken
    {
        get
        {
            return _closed.Token;
        }
    }

    public void MarkPacketReceived(TimeProvider timeProvider)
    {
        LastPacketReceivedAt = timeProvider.GetUtcNow();
    }

    public async ValueTask SendAsync(MqttPacket packet, CancellationToken cancellationToken)
    {
        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _bufferWriter.Clear();
            MqttPacketCodec.Encode(packet, _bufferWriter);
            if (_bufferWriter.WrittenCount > MaximumPacketSize)
            {
                throw new InvalidOperationException(
                    "The outbound MQTT packet exceeds the client maximum packet size.");
            }

            _ = await _writer.WriteAsync(_bufferWriter.WrittenMemory, cancellationToken).ConfigureAwait(false);
            _ = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async ValueTask CloseAsync(
        MqttReasonCode reasonCode,
        CancellationToken cancellationToken)
    {
        try
        {
            await SendAsync(
                new MqttDisconnectPacket
                {
                    ReasonCode = reasonCode
                },
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _closed.Cancel();
        }
    }

    public async ValueTask DisposeAsync()
    {
        _closed.Cancel();
        _writeLock.Dispose();
        _closed.Dispose();
        await _writer.CompleteAsync().ConfigureAwait(false);
    }
}
