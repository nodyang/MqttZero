using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Server.Events;
using MqttZero.Server.Observers;
using MqttZero.Server.Clients;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using MqttZero.Server.Storage;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Text;

namespace MqttZero.Server.Broker;

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
        lease.SetExpiresAt(CalculateMessageExpiresAt(source.MessageExpiryInterval));
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
        DateTimeOffset? expiresAt = lease.ExpiresAt;
        if (expiresAt <= now)
        {
            await NotifyDroppedAsync(
                senderConnection?.ConnectedClient,
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
                Sender = senderConnection?.ConnectedClient,
                ReceiverInfo = session.Connection?.ClientInfo ??
                    CreateOfflineClientInfo(session),
                Receiver = session.Connection?.ConnectedClient,
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
                        senderConnection?.ConnectedClient,
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
        MqttZeroQueuedMessage? evicted = null;
        bool dropOfflineQos0 = false;

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
                    message.SetMessageId(GetNextMessageId());
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
                        dropOfflineQos0 = true;
                    }
                    else
                    {
                        message = MqttZeroQueuedMessage.Rent(
                            lease,
                            qos,
                            retain,
                            subscriptionIdentifier,
                            expiresAt);
                        message.SetMessageId(GetNextMessageId());
                        evicted = EnqueueMessage(session, message);
                        messageHeld = true;
                    }
                }
            }

            if (dropOfflineQos0)
            {
                await NotifyDroppedAsync(
                    senderConnection?.ConnectedClient,
                    session,
                    lease,
                    MqttZeroMessageDropReason.OfflineClient,
                    cancellationToken).ConfigureAwait(false);
                return;
            }

            if (evicted != null)
            {
                await NotifyOverwrittenAsync(
                    session,
                    session.Connection?.ClientInfo ?? CreateOfflineClientInfo(session),
                    evicted,
                    cancellationToken).ConfigureAwait(false);
            }

            if (messageHeld && _messageStore.IsEnabled)
            {
                try
                {
                    if (evicted != null)
                    {
                        await _messageStore.ReplaceQueuedAsync(
                            session.ClientId,
                            evicted.MessageId,
                            CreatePersistedMessage(
                                message!,
                                session.ClientId,
                                MqttZeroPersistedMessageKind.Queued),
                            cancellationToken).ConfigureAwait(false);
                    }
                    else if (sendImmediately)
                    {
                        await SaveOutboundAsync(
                            message!,
                            session.ClientId,
                            cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        await SaveQueuedAsync(
                            message!,
                            session.ClientId,
                            cancellationToken).ConfigureAwait(false);
                    }
                }
                catch
                {
                    lock (session.SyncRoot)
                    {
                        if (sendImmediately)
                        {
                            session.InflightMessages.Remove(message!.PacketIdentifier);
                        }
                        else
                        {
                            int index = session.PendingMessages.FindIndex(
                                item => ReferenceEquals(item, message));
                            if (index >= 0)
                            {
                                session.PendingMessages.RemoveAt(index);
                            }

                            if (evicted != null)
                            {
                                session.PendingMessages.Prepend(evicted);
                            }
                        }
                    }

                    message!.Release();
                    messageHeld = false;
                    if (connection != null)
                    {
                        await connection.CloseAsync(
                            MqttReasonCode.UnspecifiedError,
                            cancellationToken).ConfigureAwait(false);
                    }

                    throw;
                }
            }

            if (evicted != null)
            {
                evicted.Release();
            }

            if (messageHeld && _deliveryObservers.Length > 0)
            {
                MqttZeroClientInfo receiver = enqueueContext?.ReceiverInfo ??
                    session.Connection?.ClientInfo ??
                    CreateOfflineClientInfo(session);
                MqttZeroMessageEnqueuedContext observerContext = new()
                {
                    Sender = senderConnection?.ConnectedClient,
                    ReceiverInfo = receiver,
                    Receiver = session.Connection?.ConnectedClient,
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

    private MqttZeroQueuedMessage? EnqueueMessage(
        MqttZeroSessionState session,
        MqttZeroQueuedMessage message)
    {
        MqttZeroQueuedMessage? evicted = null;
        if (session.PendingMessages.Count >= _options.MaxMessageQueueLength)
        {
            int oldestQos0Index = session.PendingMessages.FindIndex(
                item => item.QualityOfServiceLevel == MqttQualityOfServiceLevel.AtMostOnce);
            if (oldestQos0Index >= 0)
            {
                evicted = session.PendingMessages[oldestQos0Index];
                session.PendingMessages.RemoveAt(oldestQos0Index);
            }
            else if (session.PendingMessages.Count > 0)
            {
                evicted = session.PendingMessages[0];
                session.PendingMessages.RemoveAt(0);
            }
        }

        session.PendingMessages.Add(message);
        return evicted;
    }

    private static MqttZeroClientInfo CreateOfflineClientInfo(MqttZeroSessionState session)
    {
        return new MqttZeroClientInfo
        {
            ClientId = session.ClientId
        };
    }

    private async ValueTask NotifyNotConsumedAsync(
        MqttZeroConnectedClient? sender,
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
        MqttZeroConnectedClient? sender,
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
            ReceiverInfo = session.Connection?.ClientInfo,
            Receiver = session.Connection?.ConnectedClient,
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
            MqttZeroQueuedMessage? failed = null;
            lock (session.SyncRoot)
            {
                session.InflightMessages.Remove(packet.PacketIdentifier, out failed);
            }

            if (failed != null)
            {
                try
                {
                    await DeleteOutboundAsync(
                        session.ClientId,
                        failed.MessageId,
                        cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    lock (session.SyncRoot)
                    {
                        session.InflightMessages[packet.PacketIdentifier] = failed;
                    }

                    throw;
                }

                failed.Release();
            }

            await DrainSessionAsync(session, cancellationToken).ConfigureAwait(false);
            return;
        }

        message.SetAwaitPubComp();
        try
        {
            await SaveOutboundAsync(
                message,
                session.ClientId,
                cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            message.SetAwaitPubRec();
            throw;
        }

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
        MqttZeroQueuedMessage? completed = null;
        lock (session.SyncRoot)
        {
            session.InflightMessages.Remove(packet.PacketIdentifier, out completed);
        }

        if (completed != null)
        {
            try
            {
                await DeleteOutboundAsync(
                    session.ClientId,
                    completed.MessageId,
                    cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                lock (session.SyncRoot)
                {
                    session.InflightMessages[packet.PacketIdentifier] = completed;
                }

                throw;
            }

            completed.Release();
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

        bool leaseRestored = false;
        try
        {
            await DeleteInboundQos2Async(
                session.ClientId,
                packet.PacketIdentifier,
                cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            lock (session.SyncRoot)
            {
                session.InboundQos2Messages[packet.PacketIdentifier] = lease;
            }

            leaseRestored = true;
            throw;
        }
        try
        {
            MqttReasonCode result = await RoutePublishLeaseWithResultAsync(
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
            if (!leaseRestored)
            {
                lease.Release();
            }
        }
    }

    private async ValueTask NotifyOverwrittenAsync(
        MqttZeroSessionState session,
        MqttZeroClientInfo receiver,
        MqttZeroQueuedMessage evicted,
        CancellationToken cancellationToken)
    {
        MqttZeroQueueMessageOverwrittenContext context = new()
        {
            ReceiverInfo = receiver,
            Receiver = session.Connection?.ConnectedClient,
            Topic = new ReadOnlySequence<byte>(evicted.Topic)
        };
        foreach (IMqttZeroDeliveryObserver observer in _deliveryObservers)
        {
            try
            {
                await observer.OnQueueMessageOverwrittenAsync(
                    context,
                    cancellationToken).ConfigureAwait(false);
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
            bool hasNext = false;
            List<MqttZeroQueuedMessage> expired = [];
            lock (session.SyncRoot)
            {
                DateTimeOffset now = _timeProvider.GetUtcNow();
                session.PendingMessages.RemoveExpired(now, expired);
                hasNext = session.PendingMessages.Count > 0 &&
                    (session.PendingMessages[0].QualityOfServiceLevel ==
                        MqttQualityOfServiceLevel.AtMostOnce ||
                    session.InflightMessages.Count < GetInflightLimit(connection));
                if (hasNext)
                {
                    next = session.PendingMessages.Dequeue();
                    if (next.QualityOfServiceLevel != MqttQualityOfServiceLevel.AtMostOnce)
                    {
                        next.PacketIdentifier = session.GetNextPacketIdentifier();
                        session.InflightMessages[next.PacketIdentifier] = next;
                        nextHeld = true;
                    }
                }
            }

            foreach (MqttZeroQueuedMessage expiredMessage in expired)
            {
                await NotifyDroppedAsync(
                    null,
                    session,
                    expiredMessage.Lease,
                    MqttZeroMessageDropReason.Expired,
                    cancellationToken).ConfigureAwait(false);
                await DeleteOutboundAsync(
                    session.ClientId,
                    expiredMessage.MessageId,
                    cancellationToken).ConfigureAwait(false);
                expiredMessage.Release();
            }

            if (!hasNext)
            {
                break;
            }

            if (nextHeld)
            {
                await SaveOutboundAsync(
                    next!,
                    session.ClientId,
                    cancellationToken).ConfigureAwait(false);
            }

            await connection.SendAsync(next!.CreatePacket(false), cancellationToken)
                .ConfigureAwait(false);
            if (!nextHeld)
            {
                next.Release();
            }
        }
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
