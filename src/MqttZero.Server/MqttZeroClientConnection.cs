using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Sessions;
using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;

namespace MqttZero;

internal sealed class MqttZeroClientConnection : IAsyncDisposable
{
    private readonly MqttZeroPacketWriter _bufferWriter = new();
    private readonly MqttZeroBufferPool _bufferPool;
    private readonly CancellationTokenSource _closed = new();
    private readonly Dictionary<ushort, MqttZeroPooledTopicAlias> _inboundTopicAliases = [];
    private readonly MqttZeroPooledTopicAlias?[] _inboundTopicAliasArray = new MqttZeroPooledTopicAlias?[257];
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
        TimeProvider timeProvider,
        MqttZeroBufferPool? bufferPool = null)
    {
        _bufferPool = bufferPool ?? MqttZeroBufferPool.Shared;
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

    public void SetInboundTopicAlias(ushort alias, ReadOnlySequence<byte> topic)
    {
        int length = checked((int)topic.Length);
        MqttZeroPooledTopicAlias? entry = alias <= 256
            ? _inboundTopicAliasArray[alias]
            : _inboundTopicAliases.GetValueOrDefault(alias);
        if (entry == null ||
            entry.Buffer.Length < length)
        {
            if (entry != null)
            {
                _bufferPool.Return(entry.Buffer);
            }

            entry = new MqttZeroPooledTopicAlias(_bufferPool.Rent(length), length);
            if (alias <= 256)
            {
                _inboundTopicAliasArray[alias] = entry;
            }
            else
            {
                _inboundTopicAliases[alias] = entry;
            }
        }

        topic.CopyTo(entry.Buffer);
        entry.Length = length;
    }

    public bool TryGetInboundTopicAlias(ushort alias, out ReadOnlyMemory<byte> topic)
    {
        MqttZeroPooledTopicAlias? entry = alias <= 256
            ? _inboundTopicAliasArray[alias]
            : _inboundTopicAliases.GetValueOrDefault(alias);
        if (entry != null)
        {
            topic = entry.Buffer.AsMemory(0, entry.Length);
            return true;
        }

        topic = ReadOnlyMemory<byte>.Empty;
        return false;
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

    public async ValueTask SendPublishAsync(
        MqttZeroQueuedMessage message,
        bool duplicate,
        CancellationToken cancellationToken)
    {
        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            MqttZeroPublishView view = message.CreateView(duplicate);
            if (MqttPublishPacketCodec.GetEncodedLength(view) > MaximumPacketSize)
            {
                throw new InvalidOperationException(
                    "The outbound MQTT packet exceeds the client maximum packet size.");
            }

            MqttPublishPacketCodec.Encode(view, _writer);
            _ = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async ValueTask SendPublishAsync(
        MqttZeroMessageLease lease,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retain,
        uint? subscriptionIdentifier,
        ushort packetIdentifier,
        bool duplicate,
        CancellationToken cancellationToken)
    {
        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            MqttZeroPublishView view = new(
                lease.TopicSequence,
                lease.PropertiesSequence,
                lease.PayloadSequence,
                qualityOfServiceLevel,
                packetIdentifier,
                duplicate,
                retain,
                lease.MessageExpiryInterval,
                subscriptionIdentifier,
                null);
            if (MqttPublishPacketCodec.GetEncodedLength(view) > MaximumPacketSize)
            {
                throw new InvalidOperationException(
                    "The outbound MQTT packet exceeds the client maximum packet size.");
            }

            MqttPublishPacketCodec.Encode(view, _writer);
            _ = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async ValueTask SendPubAckAsync(
        ushort packetIdentifier,
        MqttReasonCode reasonCode,
        CancellationToken cancellationToken)
    {
        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            bool includeReason = reasonCode != MqttReasonCode.Success;
            Span<byte> header = _writer.GetSpan(includeReason ? 5 : 4);
            header[0] = 0x40;
            header[1] = includeReason ? (byte)3 : (byte)2;
            BinaryPrimitives.WriteUInt16BigEndian(header[2..], packetIdentifier);
            _writer.Advance(4);
            if (includeReason)
            {
                _writer.GetSpan(1)[0] = (byte)reasonCode;
                _writer.Advance(1);
            }

            _ = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async ValueTask SendPingRespAsync(CancellationToken cancellationToken)
    {
        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            Span<byte> header = _writer.GetSpan(2);
            header[0] = 0xD0;
            header[1] = 0;
            _writer.Advance(2);
            _ = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async ValueTask SendSubAckAsync(
        MqttSubAckPacket packet,
        CancellationToken cancellationToken)
    {
        await SendControlPacketDirectAsync(
            packet,
            2 + packet.ReasonCodes.Length,
            packet.Properties.Length,
            cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask SendUnsubAckAsync(
        MqttUnsubAckPacket packet,
        CancellationToken cancellationToken)
    {
        await SendControlPacketDirectAsync(
            packet,
            2 + packet.ReasonCodes.Length,
            packet.Properties.Length,
            cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask SendConnAckAsync(
        MqttConnAckPacket packet,
        CancellationToken cancellationToken)
    {
        int propertiesLength = Mqtt5PropertyWriter.GetConnAckPropertiesLength(packet);
        int bodyLength =
            2 +
            GetVariableByteIntegerSize(propertiesLength) +
            propertiesLength;
        long encodedLength =
            1 +
            GetVariableByteIntegerSize(bodyLength) +
            bodyLength;
        if (encodedLength > MaximumPacketSize)
        {
            throw new InvalidOperationException(
                "The outbound MQTT packet exceeds the client maximum packet size.");
        }

        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            Span<byte> fixedHeader = _writer.GetSpan(5);
            fixedHeader[0] = 0x20;
            int lengthBytes = MqttZeroPacketWriter.WriteVariableByteInteger(
                fixedHeader[1..],
                (uint)bodyLength);
            _writer.Advance(1 + lengthBytes);
            Span<byte> acknowledgeHeader = _writer.GetSpan(2);
            acknowledgeHeader[0] = packet.SessionPresent ? (byte)1 : (byte)0;
            acknowledgeHeader[1] = (byte)packet.ReasonCode;
            _writer.Advance(2);
            Mqtt5PropertyWriter.WriteConnAckProperties(packet, _writer);
            _ = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async ValueTask SendDisconnectAsync(
        MqttDisconnectPacket packet,
        CancellationToken cancellationToken)
    {
        int propertiesLength =
            Mqtt5PropertyWriter.GetDisconnectPropertiesLength(packet);
        bool includeDetail =
            packet.ReasonCode != MqttReasonCode.Success ||
            propertiesLength > 0;
        int bodyLength = includeDetail
            ? 1 +
                GetVariableByteIntegerSize(propertiesLength) +
                propertiesLength
            : 0;
        long encodedLength =
            1 +
            GetVariableByteIntegerSize(bodyLength) +
            bodyLength;
        if (encodedLength > MaximumPacketSize)
        {
            throw new InvalidOperationException(
                "The outbound MQTT packet exceeds the client maximum packet size.");
        }

        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            Span<byte> fixedHeader = _writer.GetSpan(5);
            fixedHeader[0] = 0xE0;
            int lengthBytes = MqttZeroPacketWriter.WriteVariableByteInteger(
                fixedHeader[1..],
                (uint)bodyLength);
            _writer.Advance(1 + lengthBytes);
            if (includeDetail)
            {
                _writer.GetSpan(1)[0] = (byte)packet.ReasonCode;
                _writer.Advance(1);
                Mqtt5PropertyWriter.WriteDisconnectProperties(packet, _writer);
            }

            _ = await _writer.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private async ValueTask SendControlPacketDirectAsync(
        MqttPacket packet,
        long bodyLength,
        long propertyLength,
        CancellationToken cancellationToken)
    {
        int propertyLengthSize = propertyLength < 128
            ? 1
            : propertyLength < 16_384
                ? 2
                : propertyLength < 2_097_152
                    ? 3
                    : 4;
        bodyLength += propertyLength + propertyLengthSize;
        int remainingLengthSize = GetVariableByteIntegerSize(bodyLength);
        long encodedLength = 1 + remainingLengthSize + bodyLength;
        if (encodedLength > MaximumPacketSize)
        {
            throw new InvalidOperationException(
                "The outbound MQTT packet exceeds the client maximum packet size.");
        }

        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            MqttPacketCodec.Encode(packet, _writer);
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
            await SendDisconnectAsync(
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

    private static int GetVariableByteIntegerSize(long value)
    {
        if (value < 128)
        {
            return 1;
        }

        if (value < 16_384)
        {
            return 2;
        }

        if (value < 2_097_152)
        {
            return 3;
        }

        return 4;
    }

    public async ValueTask DisposeAsync()
    {
        _closed.Cancel();
        _writeLock.Dispose();
        _closed.Dispose();
        foreach (MqttZeroPooledTopicAlias alias in _inboundTopicAliases.Values)
        {
            _bufferPool.Return(alias.Buffer);
        }

        for (int index = 0; index < _inboundTopicAliasArray.Length; index++)
        {
            if (_inboundTopicAliasArray[index] is MqttZeroPooledTopicAlias alias)
            {
                _bufferPool.Return(alias.Buffer);
            }
        }

        _inboundTopicAliases.Clear();
        await _writer.CompleteAsync().ConfigureAwait(false);
    }
}
