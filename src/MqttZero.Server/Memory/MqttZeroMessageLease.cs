using MqttZero.Protocol;
using System.Buffers;
using System.Collections.Concurrent;

namespace MqttZero.Memory;

internal sealed class MqttZeroMessageLease : IDisposable
{
    private static readonly ConcurrentQueue<MqttZeroMessageLease> LeasePool = new();

    private byte[] _payloadBuffer = [];
    private byte[] _propertiesBuffer = [];
    private byte[] _topicBuffer = [];
    private MqttZeroBufferPool _bufferPool = MqttZeroBufferPool.Shared;
    private bool _isBorrowed;
    private int _referenceCount;

    private MqttZeroMessageLease()
    {
    }

    public ReadOnlyMemory<byte> Topic { get; private set; }

    public ReadOnlyMemory<byte> Properties { get; private set; }

    public ReadOnlyMemory<byte> Payload { get; private set; }

    public ReadOnlySequence<byte> TopicSequence { get; private set; }

    public ReadOnlySequence<byte> PropertiesSequence { get; private set; }

    public ReadOnlySequence<byte> PayloadSequence { get; private set; }

    public bool IsBorrowed
    {
        get
        {
            return _isBorrowed;
        }
    }

    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; private set; }

    public bool RetainFlag { get; private set; }

    public uint? MessageExpiryInterval { get; private set; }

    public uint? SubscriptionIdentifier { get; private set; }

    public ushort PacketIdentifier { get; set; }

    public static MqttZeroMessageLease CopyFrom(
        ReadOnlySequence<byte> topic,
        ReadOnlySequence<byte> properties,
        ReadOnlySequence<byte> payload,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retain,
        uint? messageExpiryInterval,
        uint? subscriptionIdentifier,
        MqttZeroBufferPool? bufferPool = null)
    {
        if (!LeasePool.TryDequeue(out MqttZeroMessageLease? lease))
        {
            lease = new MqttZeroMessageLease();
        }

        lease.Initialize(
            topic,
            properties,
            payload,
            qualityOfServiceLevel,
            retain,
            messageExpiryInterval,
            subscriptionIdentifier,
            bufferPool ?? MqttZeroBufferPool.Shared);
        return lease;
    }

    public static MqttZeroMessageLease BorrowFrom(
        ReadOnlySequence<byte> topic,
        ReadOnlySequence<byte> properties,
        ReadOnlySequence<byte> payload,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retain,
        uint? messageExpiryInterval,
        uint? subscriptionIdentifier,
        MqttZeroBufferPool? bufferPool = null)
    {
        if (!LeasePool.TryDequeue(out MqttZeroMessageLease? lease))
        {
            lease = new MqttZeroMessageLease();
        }

        lease.InitializeBorrowed(
            topic,
            properties,
            payload,
            qualityOfServiceLevel,
            retain,
            messageExpiryInterval,
            subscriptionIdentifier,
            bufferPool ?? MqttZeroBufferPool.Shared);
        return lease;
    }

    public MqttZeroMessageLease CreateOwnedCopy()
    {
        return CopyFrom(
            TopicSequence,
            PropertiesSequence,
            PayloadSequence,
            QualityOfServiceLevel,
            RetainFlag,
            MessageExpiryInterval,
            SubscriptionIdentifier,
            _bufferPool);
    }

    public void Retain()
    {
        Interlocked.Increment(ref _referenceCount);
    }

    public void Release()
    {
        int references = Interlocked.Decrement(ref _referenceCount);
        if (references > 0)
        {
            return;
        }

        ObjectDisposedException.ThrowIf(references < 0, this);

        Reset();
        ReturnBuffers();
        LeasePool.Enqueue(this);
    }

    public void Dispose()
    {
        Release();
    }

    private void Initialize(
        ReadOnlySequence<byte> topic,
        ReadOnlySequence<byte> properties,
        ReadOnlySequence<byte> payload,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retain,
        uint? messageExpiryInterval,
        uint? subscriptionIdentifier,
        MqttZeroBufferPool bufferPool)
    {
        _bufferPool = bufferPool;
        try
        {
            _isBorrowed = false;
            _topicBuffer = CopyToPooledBuffer(topic, _topicBuffer);
            _propertiesBuffer = CopyToPooledBuffer(properties, _propertiesBuffer);
            _payloadBuffer = CopyToPooledBuffer(payload, _payloadBuffer);
            Topic = _topicBuffer.AsMemory(0, (int)topic.Length);
            Properties = _propertiesBuffer.AsMemory(0, (int)properties.Length);
            Payload = _payloadBuffer.AsMemory(0, (int)payload.Length);
            TopicSequence = new ReadOnlySequence<byte>(Topic);
            PropertiesSequence = new ReadOnlySequence<byte>(Properties);
            PayloadSequence = new ReadOnlySequence<byte>(Payload);
            QualityOfServiceLevel = qualityOfServiceLevel;
            RetainFlag = retain;
            MessageExpiryInterval = messageExpiryInterval;
            SubscriptionIdentifier = subscriptionIdentifier;
            PacketIdentifier = 0;
            _referenceCount = 1;
        }
        catch
        {
            Reset();
            ReturnBuffers();
            throw;
        }
    }

    private void InitializeBorrowed(
        ReadOnlySequence<byte> topic,
        ReadOnlySequence<byte> properties,
        ReadOnlySequence<byte> payload,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retain,
        uint? messageExpiryInterval,
        uint? subscriptionIdentifier,
        MqttZeroBufferPool bufferPool)
    {
        _bufferPool = bufferPool;
        _isBorrowed = true;
        try
        {
            TopicSequence = topic;
            PropertiesSequence = properties;
            PayloadSequence = payload;
            if (topic.IsSingleSegment)
            {
                Topic = topic.First;
            }
            else
            {
                _topicBuffer = CopyToPooledBuffer(topic, _topicBuffer);
                TopicSequence = new ReadOnlySequence<byte>(
                    _topicBuffer.AsMemory(0, (int)topic.Length));
                Topic = TopicSequence.First;
            }

            if (properties.IsSingleSegment)
            {
                Properties = properties.First;
            }
            else
            {
                _propertiesBuffer = CopyToPooledBuffer(properties, _propertiesBuffer);
                PropertiesSequence = new ReadOnlySequence<byte>(
                    _propertiesBuffer.AsMemory(0, (int)properties.Length));
                Properties = PropertiesSequence.First;
            }

            Payload = payload.IsSingleSegment ? payload.First : ReadOnlyMemory<byte>.Empty;
            QualityOfServiceLevel = qualityOfServiceLevel;
            RetainFlag = retain;
            MessageExpiryInterval = messageExpiryInterval;
            SubscriptionIdentifier = subscriptionIdentifier;
            PacketIdentifier = 0;
            _referenceCount = 1;
        }
        catch
        {
            Reset();
            ReturnBuffers();
            throw;
        }
    }

    private byte[] CopyToPooledBuffer(ReadOnlySequence<byte> source, byte[] existing)
    {
        int length = (int)source.Length;
        if (length == 0)
        {
            return existing;
        }

        byte[] buffer = existing.Length >= length
            ? existing
            : _bufferPool.Rent(length);
        source.CopyTo(buffer);
        return buffer;
    }

    private void Reset()
    {
        Topic = ReadOnlyMemory<byte>.Empty;
        Properties = ReadOnlyMemory<byte>.Empty;
        Payload = ReadOnlyMemory<byte>.Empty;
        TopicSequence = ReadOnlySequence<byte>.Empty;
        PropertiesSequence = ReadOnlySequence<byte>.Empty;
        PayloadSequence = ReadOnlySequence<byte>.Empty;
        _isBorrowed = false;
        MessageExpiryInterval = null;
        SubscriptionIdentifier = null;
        PacketIdentifier = 0;
    }

    private void ReturnBuffers()
    {
        if (_topicBuffer.Length > 0)
        {
            _bufferPool.Return(_topicBuffer);
            _topicBuffer = [];
        }

        if (_propertiesBuffer.Length > 0)
        {
            _bufferPool.Return(_propertiesBuffer);
            _propertiesBuffer = [];
        }

        if (_payloadBuffer.Length > 0)
        {
            _bufferPool.Return(_payloadBuffer);
            _payloadBuffer = [];
        }
    }
}
