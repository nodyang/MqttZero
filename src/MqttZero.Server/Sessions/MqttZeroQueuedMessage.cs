using MqttZero.Memory;
using MqttZero.Protocol;
using System.Buffers;
using System.Collections.Concurrent;

namespace MqttZero.Server.Sessions;

internal sealed class MqttZeroQueuedMessage
{
    private static readonly ConcurrentQueue<MqttZeroQueuedMessage> MessagePool = new();

    private MqttZeroMessageLease _lease = null!;

    private MqttZeroQueuedMessage()
    {
    }

    internal MqttZeroMessageLease Lease
    {
        get
        {
            return _lease;
        }
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

    public MqttZeroOutboundState State { get; private set; }

    public long MessageId { get; private set; }

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
        message.MessageId = 0;
        message.State = qualityOfServiceLevel switch
        {
            MqttQualityOfServiceLevel.ExactlyOnce => MqttZeroOutboundState.AwaitPubRec,
            MqttQualityOfServiceLevel.AtLeastOnce => MqttZeroOutboundState.AwaitPubAck,
            _ => MqttZeroOutboundState.AwaitPubAck
        };
        return message;
    }

    public void SetAwaitPubComp()
    {
        State = MqttZeroOutboundState.AwaitPubComp;
    }

    public void SetAwaitPubRec()
    {
        State = MqttZeroOutboundState.AwaitPubRec;
    }

    public void SetMessageId(long messageId)
    {
        MessageId = messageId;
    }

    public void Restore(ushort packetIdentifier, MqttZeroOutboundState state)
    {
        PacketIdentifier = packetIdentifier;
        State = state;
    }

    public void Restore(long messageId, ushort packetIdentifier, MqttZeroOutboundState state)
    {
        MessageId = messageId;
        PacketIdentifier = packetIdentifier;
        State = state;
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
        MessageId = 0;
        State = MqttZeroOutboundState.AwaitPubAck;
        MessagePool.Enqueue(this);
    }
}
