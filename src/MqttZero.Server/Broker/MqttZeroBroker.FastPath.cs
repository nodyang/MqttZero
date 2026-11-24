using MqttZero.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Routing;
using MqttZero.Sessions;
using System.Buffers;

namespace MqttZero.Broker;

public sealed partial class MqttZeroBroker
{
    private async ValueTask RoutePublishLeaseFastAsync(
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
            context.SenderClientInfo = senderConnection.ClientInfo;
            try
            {
                foreach (IMqttZeroPublishInterceptor interceptor in _publishInterceptors)
                {
                    MqttZeroInterceptionResult result = await interceptor
                        .InterceptAsync(context, cancellationToken)
                        .ConfigureAwait(false);
                    if (result != MqttZeroInterceptionResult.Continue)
                    {
                        if (lease.QualityOfServiceLevel ==
                            MqttQualityOfServiceLevel.AtLeastOnce)
                        {
                            await senderConnection.SendPubAckAsync(
                                lease.PacketIdentifier,
                                context.ResultReasonCode,
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
                StoreRetainedMessageInMemory(effectiveLease);
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
                MqttQualityOfServiceLevel deliveryQos =
                    (MqttQualityOfServiceLevel)Math.Min(
                        (byte)effectiveLease.QualityOfServiceLevel,
                        (byte)delivery.QualityOfServiceLevel);
                await DeliverLeaseFastAsync(
                    delivery.Session,
                    effectiveLease,
                    deliveryQos,
                    delivery.Subscription.RetainAsPublished && effectiveLease.RetainFlag,
                    delivery.Subscription.SubscriptionIdentifier,
                    cancellationToken).ConfigureAwait(false);
            }

            if (effectiveLease.QualityOfServiceLevel ==
                MqttQualityOfServiceLevel.AtLeastOnce)
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

    private async ValueTask DeliverLeaseFastAsync(
        MqttZeroSessionState session,
        MqttZeroMessageLease lease,
        MqttQualityOfServiceLevel qos,
        bool retain,
        uint? subscriptionIdentifier,
        CancellationToken cancellationToken)
    {
        DateTimeOffset? expiresAt = lease.MessageExpiryInterval.HasValue
            ? _timeProvider.GetUtcNow().AddSeconds(lease.MessageExpiryInterval.Value)
            : null;
        if (expiresAt <= _timeProvider.GetUtcNow())
        {
            return;
        }

        MqttZeroQueuedMessage? message = null;
        MqttZeroClientConnection? connection = null;
        bool sendImmediately;
        bool messageHeld = false;
        lock (session.SyncRoot)
        {
            connection = session.Connection;
            int inflightLimit = GetInflightLimit(connection);
            sendImmediately = connection != null &&
                session.PendingMessages.Count == 0 &&
                (qos == MqttQualityOfServiceLevel.AtMostOnce ||
                    session.InflightMessages.Count < inflightLimit);
            if (sendImmediately && qos != MqttQualityOfServiceLevel.AtMostOnce)
            {
                message = MqttZeroQueuedMessage.Rent(
                    lease,
                    qos,
                    retain,
                    subscriptionIdentifier,
                    expiresAt);
                message.SetMessageId(GetNextMessageId());
                message.PacketIdentifier = session.GetNextPacketIdentifier();
                session.InflightMessages[message.PacketIdentifier] = message;
                messageHeld = true;
            }
            else if (!sendImmediately)
            {
                if (qos == MqttQualityOfServiceLevel.AtMostOnce &&
                    connection == null &&
                    !_options.StoreQos0InQueue)
                {
                    return;
                }

                message = MqttZeroQueuedMessage.Rent(
                    lease,
                    qos,
                    retain,
                    subscriptionIdentifier,
                    expiresAt);
                message.SetMessageId(GetNextMessageId());
                MqttZeroQueuedMessage? evicted = EnqueueMessage(session, message, null);
                evicted?.Release();
                messageHeld = true;
            }
        }

        try
        {
            if (sendImmediately)
            {
                if (qos == MqttQualityOfServiceLevel.AtMostOnce)
                {
                    await connection!.SendPublishAsync(
                        lease,
                        qos,
                        retain,
                        subscriptionIdentifier,
                        0,
                        false,
                        cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await connection!.SendPublishAsync(
                        message!,
                        false,
                        cancellationToken).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            if (message != null && !messageHeld)
            {
                message.Release();
            }
        }
    }
}
