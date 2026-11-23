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
        CancellationToken cancellationToken)
    {
        DateTimeOffset now = _timeProvider.GetUtcNow();
        DateTimeOffset? expiresAt = lease.MessageExpiryInterval.HasValue
            ? now.AddSeconds(lease.MessageExpiryInterval.Value)
            : null;
        if (expiresAt <= now)
        {
            return;
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
                if (sendImmediately && qos == MqttQualityOfServiceLevel.AtLeastOnce)
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
                    EnqueueMessage(session, message);
                    messageHeld = true;
                }
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

    private void EnqueueMessage(MqttZeroSessionState session, MqttZeroQueuedMessage message)
    {
        if (session.PendingMessages.Count >= _options.MaxMessageQueueLength)
        {
            int oldestQos0Index = session.PendingMessages.FindIndex(
                item => item.QualityOfServiceLevel == MqttQualityOfServiceLevel.AtMostOnce);
            if (oldestQos0Index >= 0)
            {
                MqttZeroQueuedMessage evicted = session.PendingMessages[oldestQos0Index];
                session.PendingMessages.RemoveAt(oldestQos0Index);
                evicted.Release();
            }
            else if (session.PendingMessages.Count > 0)
            {
                MqttZeroQueuedMessage evicted = session.PendingMessages[0];
                session.PendingMessages.RemoveAt(0);
                evicted.Release();
            }
        }

        session.PendingMessages.Add(message);
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
                await connection.SendAsync(message.CreatePacket(true), cancellationToken)
                    .ConfigureAwait(false);
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
                    (session.PendingMessages[0].QualityOfServiceLevel ==
                        MqttQualityOfServiceLevel.AtLeastOnce &&
                    session.InflightMessages.Count >= GetInflightLimit(connection)))
                {
                    break;
                }

                next = session.PendingMessages[0];
                session.PendingMessages.RemoveAt(0);
                if (next.QualityOfServiceLevel == MqttQualityOfServiceLevel.AtLeastOnce)
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
        for (int index = session.PendingMessages.Count - 1; index >= 0; index--)
        {
            MqttZeroQueuedMessage message = session.PendingMessages[index];
            if (message.IsExpired(now))
            {
                session.PendingMessages.RemoveAt(index);
                message.Release();
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
        }
    }

}
