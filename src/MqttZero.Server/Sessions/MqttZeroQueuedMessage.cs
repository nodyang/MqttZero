using MqttZero.Memory;
using MqttZero.Protocol;
using System.Buffers;
using System.Collections.Concurrent;

namespace MqttZero.Sessions;

internal sealed class MqttZeroQueuedMessage
{
    private static readonly ConcurrentQueue<MqttZeroQueuedMessage> MessagePool = new();

    private MqttZeroMessageLease _lease = null!;

    private MqttZeroQueuedMessage()
    {
    }

    public ReadOnlyMemory<byte> Topic
    {
        get
        {
            return _lease.Topic;
        }
    }

    public ReadOnlyMemory<byte> Payload
    {
        get
        {
            return _lease.Payload;
        }
    }

    public ReadOnlyMemory<byte> Properties
    {
        get
        {
            return _lease.Properties;
        }
    }

    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; private set; }

    public bool Retain { get; private set; }

    public uint? SubscriptionIdentifier { get; private set; }

    public DateTimeOffset? ExpiresAt { get; private set; }

    public ushort PacketIdentifier { get; set; }

    public static MqttZeroQueuedMessage Rent(
        MqttZeroMessageLease lease,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retain,
        uint? subscriptionIdentifier,
        DateTimeOffset? expiresAt)
    {
        if (!MessagePool.TryDequeue(out MqttZeroQueuedMessage? message))
        {
            message = new MqttZeroQueuedMessage();
        }

        lease.EnsureOwned();
        lease.Retain();
        message._lease = lease;
        message.QualityOfServiceLevel = qualityOfServiceLevel;
        message.Retain = retain;
        message.SubscriptionIdentifier = subscriptionIdentifier;
        message.ExpiresAt = expiresAt;
        message.PacketIdentifier = 0;
        return message;
    }

    public bool IsExpired(DateTimeOffset now)
    {
        return ExpiresAt.HasValue && ExpiresAt.Value <= now;
    }

    public MqttPublishPacket CreatePacket(bool duplicate)
    {
        return new MqttPublishPacket
        {
            TopicName = new ReadOnlySequence<byte>(Topic),
            Payload = new ReadOnlySequence<byte>(Payload),
            Properties = Properties.Length == 0
                ? ReadOnlySequence<byte>.Empty
                : new ReadOnlySequence<byte>(Properties),
            SubscriptionIdentifier = SubscriptionIdentifier,
            QualityOfServiceLevel = QualityOfServiceLevel,
            PacketIdentifier = PacketIdentifier,
            Duplicate = duplicate,
            Retain = Retain
        };
    }

    public MqttZeroPublishView CreateView(bool duplicate)
    {
        return new MqttZeroPublishView(
            _lease.TopicSequence,
            _lease.PropertiesSequence,
            _lease.PayloadSequence,
            QualityOfServiceLevel,
            PacketIdentifier,
            duplicate,
            Retain,
            _lease.MessageExpiryInterval,
            SubscriptionIdentifier,
            null);
    }

    public void Release()
    {
        ObjectDisposedException.ThrowIf(_lease is null, this);
        _lease.Release();
        _lease = null!;
        SubscriptionIdentifier = null;
        ExpiresAt = null;
        PacketIdentifier = 0;
        MessagePool.Enqueue(this);
    }
}
