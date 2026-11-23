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
/// Implements publish routing, topic aliases, and subscription snapshots.
/// </summary>
public sealed partial class MqttZeroBroker
{
    private async ValueTask RoutePublishAsync(
        MqttZeroSessionState? sender,
        MqttPublishPacket packet,
        CancellationToken cancellationToken)
    {
        MqttZeroPublishContext context = new()
        {
            Topic = packet.TopicName,
            Payload = packet.Payload,
            Properties = packet.Properties,
            QualityOfServiceLevel = packet.QualityOfServiceLevel,
            Retain = packet.Retain,
            SenderClientId = sender?.ClientId
        };

        if (_publishInterceptors.Length > 0)
        {
            foreach (IMqttZeroPublishInterceptor interceptor in _publishInterceptors)
            {
                MqttZeroInterceptionResult result = await interceptor
                    .InterceptAsync(context, cancellationToken)
                    .ConfigureAwait(false);
                if (result != MqttZeroInterceptionResult.Continue)
                {
                    return;
                }
            }
        }

        packet.TopicName = context.Topic;
        packet.Payload = context.Payload;
        packet.Properties = context.Properties;
        packet.QualityOfServiceLevel = context.QualityOfServiceLevel;
        packet.Retain = context.Retain;

        MqttZeroMessageLease lease = MqttZeroMessageLease.BorrowFrom(
            packet.TopicName,
            packet.Properties,
            packet.Payload,
            packet.QualityOfServiceLevel,
            packet.Retain,
            packet.MessageExpiryInterval,
            packet.SubscriptionIdentifier,
            _bufferPool);
        lease.PacketIdentifier = packet.PacketIdentifier;
        try
        {
            if (lease.RetainFlag)
            {
                StoreRetainedMessage(lease);
            }

            MqttZeroSubscriptionRouteSnapshot routeSnapshot =
                Volatile.Read(ref _subscriptionRouteSnapshot);
            using MqttZeroRouteScope deliveries = MqttZeroRouteSelector.Select(
                routeSnapshot,
                lease.Topic.Span,
                sender,
                _sharedSubscriptionCursors,
                _routeScopes);
            for (int index = 0; index < deliveries.Count; index++)
            {
                MqttZeroDeliveryCandidate delivery = deliveries[index];
                await DeliverLeaseAsync(
                    delivery.Session,
                    lease,
                    delivery.QualityOfServiceLevel,
                    delivery.Subscription.RetainAsPublished && lease.RetainFlag,
                    delivery.Subscription.SubscriptionIdentifier,
                    cancellationToken).ConfigureAwait(false);
            }

            if (sender != null &&
                lease.QualityOfServiceLevel == MqttQualityOfServiceLevel.AtLeastOnce)
            {
                MqttZeroClientConnection? senderConnection;
                lock (sender.SyncRoot)
                {
                    senderConnection = sender.Connection;
                }

                if (senderConnection != null)
                {
                    await senderConnection.SendPubAckAsync(
                        lease.PacketIdentifier,
                        MqttReasonCode.Success,
                        cancellationToken).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            lease.Release();
        }
    }

    internal MqttZeroMessageLease CreateMessageLease(
        MqttZeroClientConnection connection,
        MqttZeroPublishView view)
    {
        ReadOnlySequence<byte> topic = view.Topic;
        if (view.TopicAlias.HasValue)
        {
            ushort alias = view.TopicAlias.Value;
            if (alias == 0 || alias > _options.TopicAliasMaximum)
            {
                throw new InvalidOperationException("The PUBLISH topic alias is invalid.");
            }

            if (!topic.IsEmpty)
            {
                connection.SetInboundTopicAlias(alias, topic);
            }
            else if (connection.TryGetInboundTopicAlias(alias, out ReadOnlyMemory<byte> mappedTopic))
            {
                topic = new ReadOnlySequence<byte>(mappedTopic);
            }
            else
            {
                throw new InvalidOperationException("The PUBLISH topic alias is unknown.");
            }
        }
        else if (topic.IsEmpty)
        {
            throw new InvalidOperationException("The PUBLISH topic is empty.");
        }

        MqttZeroMessageLease lease = MqttZeroMessageLease.BorrowFrom(
            topic,
            view.Properties,
            view.Payload,
            view.QualityOfServiceLevel,
            view.Retain,
            view.MessageExpiryInterval,
            view.SubscriptionIdentifier,
            _bufferPool);
        lease.PacketIdentifier = view.PacketIdentifier;
        return lease;
    }

    internal async ValueTask RoutePublishLeaseAsync(
        MqttZeroSessionState sender,
        MqttZeroClientConnection senderConnection,
        MqttZeroMessageLease lease,
        CancellationToken cancellationToken)
    {
        MqttZeroMessageLease effectiveLease = lease;
        bool ownsEffectiveLease = false;
        if (_publishInterceptors.Length > 0)
        {
            MqttZeroPublishContext context = MqttZeroPublishContext.Rent();
            context.Topic = new ReadOnlySequence<byte>(lease.Topic);
            context.Payload = new ReadOnlySequence<byte>(lease.Payload);
            context.Properties = lease.Properties.Length == 0
                ? ReadOnlySequence<byte>.Empty
                : new ReadOnlySequence<byte>(lease.Properties);
            context.QualityOfServiceLevel = lease.QualityOfServiceLevel;
            context.Retain = lease.RetainFlag;
            context.SenderClientId = sender.ClientId;
            try
            {
                foreach (IMqttZeroPublishInterceptor interceptor in _publishInterceptors)
                {
                    MqttZeroInterceptionResult result = await interceptor
                        .InterceptAsync(context, cancellationToken)
                        .ConfigureAwait(false);
                    if (result != MqttZeroInterceptionResult.Continue)
                    {
                        if (lease.QualityOfServiceLevel == MqttQualityOfServiceLevel.AtLeastOnce)
                        {
                            await senderConnection.SendPubAckAsync(
                                lease.PacketIdentifier,
                                MqttReasonCode.Success,
                                cancellationToken).ConfigureAwait(false);
                        }

                        return;
                    }
                }

                effectiveLease = MqttZeroMessageLease.CopyFrom(
                    context.Topic,
                    context.Properties,
                    context.Payload,
                    context.QualityOfServiceLevel,
                    context.Retain,
                    lease.MessageExpiryInterval,
                    lease.SubscriptionIdentifier,
                    _bufferPool);
                effectiveLease.PacketIdentifier = lease.PacketIdentifier;
                ownsEffectiveLease = true;
            }
            finally
            {
                context.Release();
            }
        }

        try
        {
            if (effectiveLease.RetainFlag)
            {
                StoreRetainedMessage(effectiveLease);
            }

            MqttZeroSubscriptionRouteSnapshot routeSnapshot =
                Volatile.Read(ref _subscriptionRouteSnapshot);
            using MqttZeroRouteScope deliveries = MqttZeroRouteSelector.Select(
                routeSnapshot,
                effectiveLease.Topic.Span,
                sender,
                _sharedSubscriptionCursors,
                _routeScopes);
            for (int index = 0; index < deliveries.Count; index++)
            {
                MqttZeroDeliveryCandidate delivery = deliveries[index];
                await DeliverLeaseAsync(
                    delivery.Session,
                    effectiveLease,
                    delivery.QualityOfServiceLevel,
                    delivery.Subscription.RetainAsPublished && effectiveLease.RetainFlag,
                    delivery.Subscription.SubscriptionIdentifier,
                    cancellationToken).ConfigureAwait(false);
            }

            if (effectiveLease.QualityOfServiceLevel == MqttQualityOfServiceLevel.AtLeastOnce)
            {
                await senderConnection.SendPubAckAsync(
                    effectiveLease.PacketIdentifier,
                    MqttReasonCode.Success,
                    cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            if (ownsEffectiveLease)
            {
                effectiveLease.Release();
            }
        }
    }

    private void ResolveTopicAlias(
        MqttZeroClientConnection connection,
        MqttPublishPacket packet)
    {
        if (!packet.TopicAlias.HasValue)
        {
            if (packet.TopicName.IsEmpty)
            {
                throw new InvalidOperationException("The PUBLISH topic is empty.");
            }

            return;
        }

        ushort alias = packet.TopicAlias.Value;
        if (alias == 0 || alias > _options.TopicAliasMaximum)
        {
            throw new InvalidOperationException("The PUBLISH topic alias is invalid.");
        }

        if (!packet.TopicName.IsEmpty)
        {
            connection.SetInboundTopicAlias(alias, packet.TopicName);
            return;
        }

        if (!connection.TryGetInboundTopicAlias(alias, out ReadOnlyMemory<byte> topic))
        {
            throw new InvalidOperationException("The PUBLISH topic alias is unknown.");
        }

        packet.TopicName = new ReadOnlySequence<byte>(topic);
    }

    private void RebuildSubscriptionRouteSnapshot()
    {
        Dictionary<string, List<MqttZeroRouteEntry>> regularGroups =
            new(StringComparer.Ordinal);
        List<MqttZeroRouteEntry> sharedEntries = [];
        lock (_sessionGate)
        {
            foreach (MqttZeroSessionState session in _sessions.Values)
            {
                lock (session.SyncRoot)
                {
                    foreach (MqttZeroSubscription subscription in session.Subscriptions)
                    {
                        MqttZeroRouteEntry entry = new(session, subscription);
                        if (subscription.IsShared)
                        {
                            sharedEntries.Add(entry);
                            continue;
                        }

                        string key = Encoding.UTF8.GetString(subscription.MatchFilter);
                        if (!regularGroups.TryGetValue(key, out List<MqttZeroRouteEntry>? entries))
                        {
                            entries = [];
                            regularGroups[key] = entries;
                        }

                        entries.Add(entry);
                    }
                }
            }
        }

        MqttZeroRouteFilterGroup[] groups = new MqttZeroRouteFilterGroup[regularGroups.Count];
        int groupIndex = 0;
        foreach (List<MqttZeroRouteEntry> entries in regularGroups.Values)
        {
            groups[groupIndex++] = new MqttZeroRouteFilterGroup(
                entries[0].Subscription.MatchFilter,
                entries.ToArray());
        }

        Volatile.Write(
            ref _subscriptionRouteSnapshot,
            new MqttZeroSubscriptionRouteSnapshot(groups, sharedEntries.ToArray()));
    }

}
