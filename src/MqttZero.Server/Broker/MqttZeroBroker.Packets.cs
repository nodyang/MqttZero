using MqttZero.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Routing;
using MqttZero.Sessions;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Text;

namespace MqttZero.Broker;

/// <summary>
/// Implements packet dispatch, subscription, and unsubscription handling.
/// </summary>
public sealed partial class MqttZeroBroker
{
    private async ValueTask ProcessPacketAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttPacket packet,
        CancellationToken cancellationToken)
    {
        switch (packet)
        {
            case MqttSubscribePacket subscribePacket:
                await ProcessSubscribeAsync(session, connection, subscribePacket, cancellationToken)
                    .ConfigureAwait(false);
                break;
            case MqttUnsubscribePacket unsubscribePacket:
                await ProcessUnsubscribeAsync(session, connection, unsubscribePacket, cancellationToken)
                    .ConfigureAwait(false);
                break;
            case MqttPublishPacket publishPacket:
                ResolveTopicAlias(connection, publishPacket);
                await RoutePublishAsync(session, publishPacket, cancellationToken).ConfigureAwait(false);
                break;
            case MqttPingReqPacket:
                await connection.SendPingRespAsync(cancellationToken).ConfigureAwait(false);
                break;
            case MqttPubAckPacket pubAckPacket:
                lock (session.SyncRoot)
                {
                    if (session.InflightMessages.Remove(
                        pubAckPacket.PacketIdentifier,
                        out MqttZeroQueuedMessage? acknowledged))
                    {
                        acknowledged.Release();
                    }
                }

                await DrainSessionAsync(session, cancellationToken).ConfigureAwait(false);
                break;
            case MqttDisconnectPacket:
                if (packet is MqttDisconnectPacket disconnectPacket &&
                    disconnectPacket.SessionExpiryInterval.HasValue)
                {
                    uint maximumExpiry = (uint)Math.Clamp(
                        _options.MaxSessionExpiryInterval.TotalSeconds,
                        0,
                        uint.MaxValue);
                    lock (session.SyncRoot)
                    {
                        session.ExpiryInterval = Math.Min(
                            disconnectPacket.SessionExpiryInterval.Value,
                            maximumExpiry);
                    }
                }

                break;
        }
    }

    private async ValueTask ProcessSubscribeAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttSubscribePacket packet,
        CancellationToken cancellationToken)
    {
        byte[] reasonCodes = new byte[packet.Subscriptions.Count];
        bool[] existed = new bool[packet.Subscriptions.Count];

        for (int index = 0; index < packet.Subscriptions.Count; index++)
        {
            MqttSubscriptionRequest request = packet.Subscriptions[index];
            if (request.QualityOfServiceLevel == MqttQualityOfServiceLevel.ExactlyOnce)
            {
                reasonCodes[index] = (byte)MqttReasonCode.QosNotSupported;
                continue;
            }

            byte[] topicFilter = request.TopicFilter.ToArray();
            lock (session.SyncRoot)
            {
                int existingIndex = session.Subscriptions.FindIndex(
                    subscription => subscription.TopicFilter.AsSpan().SequenceEqual(topicFilter));
                if (existingIndex < 0 &&
                    session.Subscriptions.Count >= _options.MaxSubscriptionsPerSession)
                {
                    reasonCodes[index] = (byte)MqttReasonCode.QuotaExceeded;
                    continue;
                }

                existed[index] = existingIndex >= 0;
                MqttZeroSubscription subscription = new(
                    topicFilter,
                    request.QualityOfServiceLevel,
                    request.NoLocal,
                    request.RetainAsPublished,
                    packet.SubscriptionIdentifier);
                if (existingIndex >= 0)
                {
                    session.Subscriptions[existingIndex] = subscription;
                }
                else
                {
                    session.Subscriptions.Add(subscription);
                }
            }

            reasonCodes[index] = request.QualityOfServiceLevel == MqttQualityOfServiceLevel.AtLeastOnce
                ? (byte)MqttReasonCode.GrantedQos1
                : (byte)MqttReasonCode.Success;
        }

        RebuildSubscriptionRouteSnapshot();

        await connection.SendSubAckAsync(
            new MqttSubAckPacket
            {
                PacketIdentifier = packet.PacketIdentifier,
                ReasonCodes = reasonCodes
            },
            cancellationToken).ConfigureAwait(false);

        for (int index = 0; index < packet.Subscriptions.Count; index++)
        {
            MqttSubscriptionRequest request = packet.Subscriptions[index];
            if (request.RetainHandling == 2 ||
                (request.RetainHandling == 1 && existed[index]) ||
                request.TopicFilter.IsEmpty)
            {
                continue;
            }

            byte[] filter = request.TopicFilter.ToArray();
            if (filter.AsSpan().StartsWith("$share/"u8))
            {
                continue;
            }

            foreach (MqttZeroRetainedMessage retainedMessage in _retainedMessages.Values)
            {
                DateTimeOffset now = _timeProvider.GetUtcNow();
                if (retainedMessage.ExpiresAt.HasValue && retainedMessage.ExpiresAt.Value <= now)
                {
                    continue;
                }

                if (!MqttZeroTopicMatcher.Matches(filter, retainedMessage.Topic.Span))
                {
                    continue;
                }

                MqttQualityOfServiceLevel qos = (MqttQualityOfServiceLevel)Math.Min(
                    (byte)retainedMessage.QualityOfServiceLevel,
                    (byte)request.QualityOfServiceLevel);
                MqttPublishPacket retainedPacket = new()
                {
                    TopicName = new ReadOnlySequence<byte>(retainedMessage.Topic),
                    Payload = new ReadOnlySequence<byte>(retainedMessage.Payload),
                    QualityOfServiceLevel = qos,
                    Retain = true,
                    SubscriptionIdentifier = packet.SubscriptionIdentifier
                };
                await DeliverAsync(
                    session,
                    retainedPacket,
                    qos,
                    true,
                    packet.SubscriptionIdentifier,
                    cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async ValueTask ProcessUnsubscribeAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttUnsubscribePacket packet,
        CancellationToken cancellationToken)
    {
        byte[] reasonCodes = new byte[packet.TopicFilters.Count];
        for (int index = 0; index < packet.TopicFilters.Count; index++)
        {
            byte[] topicFilter = packet.TopicFilters[index].ToArray();
            bool removed;
            lock (session.SyncRoot)
            {
                int existingIndex = session.Subscriptions.FindIndex(
                    subscription => subscription.TopicFilter.AsSpan().SequenceEqual(topicFilter));
                removed = existingIndex >= 0;
                if (removed)
                {
                    session.Subscriptions.RemoveAt(existingIndex);
                }
            }

            reasonCodes[index] = removed
                ? (byte)MqttReasonCode.Success
                : (byte)MqttReasonCode.NoSubscriptionExisted;
        }

        RebuildSubscriptionRouteSnapshot();

        await connection.SendUnsubAckAsync(
            new MqttUnsubAckPacket
            {
                PacketIdentifier = packet.PacketIdentifier,
                ReasonCodes = reasonCodes
            },
            cancellationToken).ConfigureAwait(false);
    }

}
