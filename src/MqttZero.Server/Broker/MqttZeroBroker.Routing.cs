using MqttZero.Interceptors;
using MqttZero.Clients;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Routing;
using MqttZero.Sessions;
using MqttZero.Storage;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;

namespace MqttZero.Broker;

/// <summary>
/// Implements publish routing, topic aliases, and subscription snapshots.
/// </summary>
public sealed partial class MqttZeroBroker
{
    private async ValueTask<bool> AcceptInboundQos2Async(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttZeroMessageLease lease,
        ushort packetIdentifier,
        CancellationToken cancellationToken)
    {
        bool duplicate = false;
        bool receiveMaximumExceeded = false;
        lock (session.SyncRoot)
        {
            if (session.InboundQos2Messages.ContainsKey(packetIdentifier))
            {
                duplicate = true;
            }
            else if (session.InboundQos2Messages.Count >= _options.MaxInflight)
            {
                receiveMaximumExceeded = true;
            }
            else
            {
                lease.EnsureOwned();
                session.InboundQos2Messages[packetIdentifier] = lease;
            }
        }

        if (duplicate)
        {
            await connection.SendPubRecAsync(
                packetIdentifier,
                MqttReasonCode.Success,
                cancellationToken).ConfigureAwait(false);
            return false;
        }

        if (receiveMaximumExceeded)
        {
            await connection.SendPubRecAsync(
                packetIdentifier,
                MqttReasonCode.ReceiveMaximumExceeded,
                cancellationToken).ConfigureAwait(false);
            await connection.CloseAsync(
                MqttReasonCode.ReceiveMaximumExceeded,
                cancellationToken).ConfigureAwait(false);
            return false;
        }

        if (_persistenceWriter.IsEnabled)
        {
            await _persistenceWriter.EnqueueInboundQos2Async(
                CreateInboundPersistedMessage(lease, session.ClientId, packetIdentifier),
                cancellationToken).ConfigureAwait(false);
        }
        await connection.SendPubRecAsync(
            packetIdentifier,
            MqttReasonCode.Success,
            cancellationToken).ConfigureAwait(false);
        return true;
    }

    private async ValueTask RoutePublishAsync(
        MqttZeroSessionState? sender,
        MqttPublishPacket packet,
        CancellationToken cancellationToken)
    {
        MqttZeroConnectedClient? senderClient = null;
        if (sender != null)
        {
            lock (sender.SyncRoot)
            {
                senderClient = sender.Connection?.ConnectedClient;
            }
        }

        MqttZeroPublishContext context = new()
        {
            Topic = packet.TopicName,
            Payload = packet.Payload,
            Properties = packet.Properties,
            QualityOfServiceLevel = packet.QualityOfServiceLevel,
            Retain = packet.Retain,
            Sender = senderClient
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
                await StoreRetainedMessageAsync(lease, cancellationToken).ConfigureAwait(false);
            }

            MqttZeroSubscriptionRouteSnapshot routeSnapshot =
                Volatile.Read(ref _subscriptionRouteSnapshot);
            using MqttZeroRouteScope deliveries = MqttZeroRouteSelector.Select(
                routeSnapshot,
                lease.Topic.Span,
                sender,
                _sharedSubscriptionCursors,
                _routeScopes);
            if (deliveries.Count == 0 && _deliveryObservers.Length > 0)
            {
                await NotifyNotConsumedAsync(null, lease, cancellationToken).ConfigureAwait(false);
            }

            for (int index = 0; index < deliveries.Count; index++)
            {
                MqttZeroDeliveryCandidate delivery = deliveries[index];
                MqttQualityOfServiceLevel deliveryQos =
                    (MqttQualityOfServiceLevel)Math.Min(
                        (byte)lease.QualityOfServiceLevel,
                        (byte)delivery.QualityOfServiceLevel);
                await DeliverLeaseAsync(
                    delivery.Session,
                    lease,
                    deliveryQos,
                    delivery.Subscription.RetainAsPublished && lease.RetainFlag,
                    delivery.Subscription.SubscriptionIdentifier,
                    null,
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

    internal ValueTask RoutePublishLeaseAsync(
        MqttZeroSessionState sender,
        MqttZeroClientConnection senderConnection,
        MqttZeroMessageLease lease,
        CancellationToken cancellationToken)
    {
        if (!_persistenceWriter.IsEnabled &&
            _enqueueInterceptors.Length == 0 &&
            _deliveryObservers.Length == 0)
        {
            return RoutePublishLeaseFastAsync(
                sender,
                senderConnection,
                lease,
                cancellationToken);
        }

        return RoutePublishLeaseCoreAsync(
            sender,
            senderConnection,
            lease,
            null,
            cancellationToken);
    }

    internal async ValueTask<MqttReasonCode> RoutePublishLeaseWithResultAsync(
        MqttZeroSessionState sender,
        MqttZeroClientConnection senderConnection,
        MqttZeroMessageLease lease,
        CancellationToken cancellationToken)
    {
        MqttReasonCode[] result = [MqttReasonCode.Success];
        await RoutePublishLeaseCoreAsync(
            sender,
            senderConnection,
            lease,
            result,
            cancellationToken).ConfigureAwait(false);
        return result[0];
    }

    private async ValueTask RoutePublishLeaseCoreAsync(
        MqttZeroSessionState sender,
        MqttZeroClientConnection senderConnection,
        MqttZeroMessageLease lease,
        MqttReasonCode[]? resultHolder,
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
            context.Sender = senderConnection.ConnectedClient;
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
                                context.ResultReasonCode,
                                cancellationToken).ConfigureAwait(false);
                        }

                        if (resultHolder != null)
                        {
                            resultHolder[0] = context.ResultReasonCode;
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
                await StoreRetainedMessageAsync(
                    effectiveLease,
                    cancellationToken).ConfigureAwait(false);
            }

            MqttZeroSubscriptionRouteSnapshot routeSnapshot =
                Volatile.Read(ref _subscriptionRouteSnapshot);
            using MqttZeroRouteScope deliveries = MqttZeroRouteSelector.Select(
                routeSnapshot,
                effectiveLease.Topic.Span,
                sender,
                _sharedSubscriptionCursors,
                _routeScopes);
            if (deliveries.Count == 0 && _deliveryObservers.Length > 0)
            {
                await NotifyNotConsumedAsync(
                    senderConnection.ConnectedClient,
                    effectiveLease,
                    cancellationToken).ConfigureAwait(false);
            }

            for (int index = 0; index < deliveries.Count; index++)
            {
                MqttZeroDeliveryCandidate delivery = deliveries[index];
                MqttQualityOfServiceLevel deliveryQos =
                    (MqttQualityOfServiceLevel)Math.Min(
                        (byte)effectiveLease.QualityOfServiceLevel,
                        (byte)delivery.QualityOfServiceLevel);
                await DeliverLeaseAsync(
                    delivery.Session,
                    effectiveLease,
                    deliveryQos,
                    delivery.Subscription.RetainAsPublished && effectiveLease.RetainFlag,
                    delivery.Subscription.SubscriptionIdentifier,
                    senderConnection,
                    cancellationToken).ConfigureAwait(false);
            }


            if (effectiveLease.QualityOfServiceLevel == MqttQualityOfServiceLevel.AtLeastOnce)
            {
                await senderConnection.SendPubAckAsync(
                    effectiveLease.PacketIdentifier,
                    MqttReasonCode.Success,
                    cancellationToken).ConfigureAwait(false);
            }

            if (resultHolder != null)
            {
                resultHolder[0] = MqttReasonCode.Success;
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
        lock (_sessionGate)
        {
            MqttZeroSubscriptionRouteSnapshot snapshot =
                MqttZeroSubscriptionRouteSnapshotBuilder.Build(_sessions.Values);
            Volatile.Write(ref _subscriptionRouteSnapshot, snapshot);
        }
    }

}
