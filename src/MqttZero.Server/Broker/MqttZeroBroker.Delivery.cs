using MqttZero.Interceptors;
using MqttZero.Memory;
using MqttZero.Events;
using MqttZero.Observers;
using MqttZero.Protocol;
using MqttZero.Routing;
using MqttZero.Sessions;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Text;

namespace MqttZero.Broker;

/// <summary>
/// Implements delivery, queues, inflight messages, and session drain.
/// </summary>
public sealed partial class MqttZeroBroker
{
    private async ValueTask DeliverAsync(
        MqttZeroSessionState session,
        MqttPublishPacket source,
        MqttQualityOfServiceLevel qos,
        bool retain,
        uint? subscriptionIdentifier,
        CancellationToken cancellationToken)
    {
        MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
            source.TopicName,
            source.Properties,
            source.Payload,
            qos,
            retain,
            source.MessageExpiryInterval,
            subscriptionIdentifier,
            _bufferPool);
        try
        {
            await DeliverLeaseAsync(
                session,
                lease,
                qos,
                retain,
                subscriptionIdentifier,
                null,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            lease.Release();
        }
    }

    private async ValueTask DeliverLeaseAsync(
        MqttZeroSessionState session,
        MqttZeroMessageLease lease,
        MqttQualityOfServiceLevel qos,
        bool retain,
        uint? subscriptionIdentifier,
        MqttZeroClientConnection? senderConnection,
        CancellationToken cancellationToken)
    {
        DateTimeOffset now = _timeProvider.GetUtcNow();
        DateTimeOffset? expiresAt = lease.MessageExpiryInterval.HasValue
            ? now.AddSeconds(lease.MessageExpiryInterval.Value)
            : null;
        if (expiresAt <= now)
        {
            await NotifyDroppedAsync(
                senderConnection?.ClientInfo,
                session,
                lease,
                MqttZeroMessageDropReason.Expired,
                cancellationToken).ConfigureAwait(false);
            return;
        }

        MqttZeroEnqueueContext? enqueueContext = null;
        if (_enqueueInterceptors.Length > 0)
        {
            enqueueContext = new MqttZeroEnqueueContext
            {
                Sender = senderConnection?.ClientInfo,
                Receiver = session.Connection?.ClientInfo ?? CreateOfflineClientInfo(session),
                Topic = new ReadOnlySequence<byte>(lease.Topic),
                PayloadLength = lease.PayloadSequence.Length,
                QualityOfServiceLevel = qos,
                Retain = retain,
                SubscriptionIdentifier = subscriptionIdentifier
            };
            foreach (IMqttZeroEnqueueInterceptor interceptor in _enqueueInterceptors)
            {
                MqttZeroEnqueueInterceptionResult result =
                    await interceptor.InterceptAsync(enqueueContext, cancellationToken).ConfigureAwait(false);
                if (result == MqttZeroEnqueueInterceptionResult.Drop)
                {
                    await NotifyDroppedAsync(
                        senderConnection?.ClientInfo,
                        session,
                        lease,
                        MqttZeroMessageDropReason.InterceptorRejected,
                        cancellationToken).ConfigureAwait(false);
                    return;
                }

                if (result == MqttZeroEnqueueInterceptionResult.CloseSenderConnection &&
                    senderConnection != null)
                {
                    await senderConnection.CloseAsync(
                        MqttReasonCode.NotAuthorized,
                        cancellationToken).ConfigureAwait(false);
                    return;
                }
            }
        }

        MqttZeroQueuedMessage? message = null;
        MqttZeroClientConnection? connection = null;
        bool sendImmediately = false;
        bool messageHeld = false;

        try
        {
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
                    message.PacketIdentifier = session.GetNextPacketIdentifier();
                    session.InflightMessages[message.PacketIdentifier] = message;
                    messageHeld = true;
                }

                if (!sendImmediately)
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
                    if (_deliveryObservers.Length > 0)
                    {
                        EnqueueMessage(
                            session,
                            message,
                            session.Connection?.ClientInfo ?? CreateOfflineClientInfo(session));
                    }
                    else
                    {
                        EnqueueMessage(session, message, null);
                    }
                    messageHeld = true;
                }
            }

            if (messageHeld && _deliveryObservers.Length > 0)
            {
                MqttZeroClientInfo receiver = enqueueContext?.Receiver ??
                    session.Connection?.ClientInfo ??
                    CreateOfflineClientInfo(session);
                MqttZeroMessageEnqueuedContext observerContext = new()
                {
                    Sender = senderConnection?.ClientInfo,
                    Receiver = receiver,
                    Topic = new ReadOnlySequence<byte>(lease.Topic),
                    PayloadLength = lease.PayloadSequence.Length,
                    QualityOfServiceLevel = qos
                };
                await NotifyEnqueuedAsync(observerContext, cancellationToken).ConfigureAwait(false);
            }

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
                    await connection!.SendPublishAsync(message!, false, cancellationToken)
                        .ConfigureAwait(false);
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

    private void EnqueueMessage(
        MqttZeroSessionState session,
        MqttZeroQueuedMessage message,
        MqttZeroClientInfo? receiver)
    {
        if (session.PendingMessages.Count >= _options.MaxMessageQueueLength)
        {
            int oldestQos0Index = session.PendingMessages.FindIndex(
                item => item.QualityOfServiceLevel == MqttQualityOfServiceLevel.AtMostOnce);
            if (oldestQos0Index >= 0)
            {
                MqttZeroQueuedMessage evicted = session.PendingMessages[oldestQos0Index];
                session.PendingMessages.RemoveAt(oldestQos0Index);
                if (receiver != null)
                {
                    NotifyOverwritten(receiver, evicted);
                }
                evicted.Release();
            }
            else if (session.PendingMessages.Count > 0)
            {
                MqttZeroQueuedMessage evicted = session.PendingMessages[0];
                session.PendingMessages.RemoveAt(0);
                if (receiver != null)
                {
                    NotifyOverwritten(receiver, evicted);
                }
                evicted.Release();
            }
        }

        session.PendingMessages.Add(message);
    }

    private static MqttZeroClientInfo CreateOfflineClientInfo(MqttZeroSessionState session)
    {
        return new MqttZeroClientInfo
        {
            ClientId = session.ClientId
        };
    }

    private async ValueTask NotifyNotConsumedAsync(
        MqttZeroClientInfo? sender,
        MqttZeroMessageLease lease,
        CancellationToken cancellationToken)
    {
        if (_deliveryObservers.Length == 0)
        {
            return;
        }

        MqttZeroMessageNotConsumedContext context = new()
        {
            Sender = sender,
            Topic = new ReadOnlySequence<byte>(lease.Topic),
            PayloadLength = lease.PayloadSequence.Length
        };
        foreach (IMqttZeroDeliveryObserver observer in _deliveryObservers)
        {
            try
            {
                await observer.OnMessageNotConsumedAsync(context, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception)
            {
            }
        }
    }

    private async ValueTask NotifyDroppedAsync(
        MqttZeroClientInfo? sender,
        MqttZeroSessionState session,
        MqttZeroMessageLease lease,
        MqttZeroMessageDropReason reason,
        CancellationToken cancellationToken)
    {
        if (_deliveryObservers.Length == 0)
        {
            return;
        }

        MqttZeroMessageDroppedContext context = new()
        {
            Sender = sender,
            Receiver = session.Connection?.ClientInfo,
            Topic = new ReadOnlySequence<byte>(lease.Topic),
            Reason = reason
        };
        foreach (IMqttZeroDeliveryObserver observer in _deliveryObservers)
        {
            try
            {
                await observer.OnMessageDroppedAsync(context, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception)
            {
            }
        }
    }

    private async ValueTask NotifyEnqueuedAsync(
        MqttZeroMessageEnqueuedContext context,
        CancellationToken cancellationToken)
    {
        foreach (IMqttZeroDeliveryObserver observer in _deliveryObservers)
        {
            try
            {
                await observer.OnMessageEnqueuedAsync(context, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception)
            {
            }
        }
    }

    private async ValueTask ProcessOutboundPubRecAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttPubRecPacket packet,
        CancellationToken cancellationToken)
    {
        MqttZeroQueuedMessage? message;
        lock (session.SyncRoot)
        {
            session.InflightMessages.TryGetValue(packet.PacketIdentifier, out message);
        }

        if (message == null ||
            message.QualityOfServiceLevel != MqttQualityOfServiceLevel.ExactlyOnce)
        {
            return;
        }

        if (packet.ReasonCode >= MqttReasonCode.UnspecifiedError)
        {
            lock (session.SyncRoot)
            {
                if (session.InflightMessages.Remove(packet.PacketIdentifier, out MqttZeroQueuedMessage? failed))
                {
                    failed.Release();
                }
            }

            await DrainSessionAsync(session, cancellationToken).ConfigureAwait(false);
            return;
        }

        message.SetAwaitPubComp();
        await connection.SendPubRelAsync(
            packet.PacketIdentifier,
            MqttReasonCode.Success,
            cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask ProcessOutboundPubCompAsync(
        MqttZeroSessionState session,
        MqttPubCompPacket packet,
        CancellationToken cancellationToken)
    {
        lock (session.SyncRoot)
        {
            if (session.InflightMessages.Remove(
                packet.PacketIdentifier,
                out MqttZeroQueuedMessage? completed))
            {
                completed.Release();
            }
        }

        await DrainSessionAsync(session, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask ProcessInboundPubRelAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttPubRelPacket packet,
        CancellationToken cancellationToken)
    {
        MqttZeroMessageLease? lease;
        lock (session.SyncRoot)
        {
            session.InboundQos2Messages.Remove(
                packet.PacketIdentifier,
                out lease);
        }

        if (lease == null)
        {
            await connection.SendPubCompAsync(
                packet.PacketIdentifier,
                MqttReasonCode.PacketIdentifierNotFound,
                cancellationToken).ConfigureAwait(false);
            return;
        }

        try
        {
            MqttReasonCode result = await RoutePublishLeaseAsync(
                session,
                connection,
                lease,
                cancellationToken).ConfigureAwait(false);
            await connection.SendPubCompAsync(
                packet.PacketIdentifier,
                result,
                cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            lease.Release();
        }
    }

    private void NotifyOverwritten(
        MqttZeroClientInfo receiver,
        MqttZeroQueuedMessage evicted)
    {
        MqttZeroQueueMessageOverwrittenContext context = new()
        {
            Receiver = receiver,
            Topic = new ReadOnlySequence<byte>(evicted.Topic)
        };
        foreach (IMqttZeroDeliveryObserver observer in _deliveryObservers)
        {
            try
            {
                _ = observer.OnQueueMessageOverwrittenAsync(
                    context,
                    CancellationToken.None).AsTask();
            }
            catch (Exception)
            {
            }
        }
    }

    private async ValueTask ResumeSessionAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        CancellationToken cancellationToken)
    {
        await session.DrainLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            List<MqttZeroQueuedMessage> inflight;
            lock (session.SyncRoot)
            {
                inflight = [.. session.InflightMessages.Values];
            }

            foreach (MqttZeroQueuedMessage message in inflight)
            {
                if (message.State == MqttZeroOutboundState.AwaitPubComp)
                {
                    await connection.SendPubRelAsync(
                        message.PacketIdentifier,
                        MqttReasonCode.Success,
                        cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await connection.SendAsync(message.CreatePacket(true), cancellationToken)
                        .ConfigureAwait(false);
                }
            }

            await DrainQueuedMessagesAsync(session, connection, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            session.DrainLock.Release();
        }
    }

    private async ValueTask DrainSessionAsync(
        MqttZeroSessionState session,
        CancellationToken cancellationToken)
    {
        if (!await session.DrainLock.WaitAsync(0, cancellationToken).ConfigureAwait(false))
        {
            return;
        }

        try
        {
            MqttZeroClientConnection? connection;
            lock (session.SyncRoot)
            {
                connection = session.Connection;
            }

            if (connection != null)
            {
                await DrainQueuedMessagesAsync(session, connection, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            session.DrainLock.Release();
        }
    }

    private async ValueTask DrainQueuedMessagesAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            MqttZeroQueuedMessage? next = null;
            bool nextHeld = false;
            lock (session.SyncRoot)
            {
                DateTimeOffset now = _timeProvider.GetUtcNow();
                RemoveExpiredPendingMessages(session, now);
                if (session.PendingMessages.Count == 0 ||
                    (session.PendingMessages[0].QualityOfServiceLevel !=
                        MqttQualityOfServiceLevel.AtMostOnce &&
                    session.InflightMessages.Count >= GetInflightLimit(connection)))
                {
                    break;
                }

                next = session.PendingMessages.Dequeue();
                if (next.QualityOfServiceLevel != MqttQualityOfServiceLevel.AtMostOnce)
                {
                    next.PacketIdentifier = session.GetNextPacketIdentifier();
                    session.InflightMessages[next.PacketIdentifier] = next;
                    nextHeld = true;
                }
            }

            await connection.SendAsync(next.CreatePacket(false), cancellationToken)
                .ConfigureAwait(false);
            if (!nextHeld)
            {
                next.Release();
            }
        }
    }

    private static void RemoveExpiredPendingMessages(
        MqttZeroSessionState session,
        DateTimeOffset now)
    {
        session.PendingMessages.RemoveExpired(now);
    }

    private static void ReleaseSessionMessages(MqttZeroSessionState session)
    {
        lock (session.SyncRoot)
        {
            foreach (MqttZeroQueuedMessage message in session.PendingMessages)
            {
                message.Release();
            }

            session.PendingMessages.Clear();
            foreach (MqttZeroQueuedMessage message in session.InflightMessages.Values)
            {
                message.Release();
            }

            session.InflightMessages.Clear();
            foreach (MqttZeroMessageLease inbound in session.InboundQos2Messages.Values)
            {
                inbound.Release();
            }

            session.InboundQos2Messages.Clear();
        }
    }

}
