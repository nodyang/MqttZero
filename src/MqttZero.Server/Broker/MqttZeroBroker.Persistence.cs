using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Sessions;
using MqttZero.Server.Storage;
using System.Buffers;
using System.Text;

namespace MqttZero.Server.Broker;

public sealed partial class MqttZeroBroker
{
    private async ValueTask RestorePersistenceAsync(CancellationToken cancellationToken)
    {
        IReadOnlyList<MqttZeroPersistedSession> sessions =
            await _sessionStore.LoadAllAsync(cancellationToken).ConfigureAwait(false);
        foreach (MqttZeroPersistedSession persisted in sessions)
        {
            if (persisted.ExpiryInterval > 0 &&
                persisted.DisconnectedAt.HasValue &&
                persisted.DisconnectedAt.Value.AddSeconds(persisted.ExpiryInterval) <=
                    _timeProvider.GetUtcNow())
            {
                await _sessionStore.DeleteAsync(persisted.ClientId, cancellationToken)
                    .ConfigureAwait(false);
                continue;
            }

            MqttZeroSessionState session = new(persisted.ClientId, _options.MaxMessageQueueLength)
            {
                CleanStart = persisted.CleanStart,
                ExpiryInterval = persisted.ExpiryInterval,
                DisconnectedAt = persisted.DisconnectedAt
            };
            foreach (MqttZeroPersistedSubscription subscription in persisted.Subscriptions)
            {
                session.Subscriptions.Add(new MqttZeroSubscription(
                    subscription.TopicFilter.ToArray(),
                    subscription.QualityOfServiceLevel,
                    subscription.NoLocal,
                    subscription.RetainAsPublished,
                    subscription.SubscriptionIdentifier));
            }

            _sessions[session.ClientId] = session;
        }

        IReadOnlyList<MqttZeroPersistedMessage> messages =
            await _messageStore.LoadAllAsync(cancellationToken).ConfigureAwait(false);
        foreach (MqttZeroPersistedMessage persisted in messages)
        {
            if (!_sessions.TryGetValue(persisted.ClientId, out MqttZeroSessionState? session))
            {
                continue;
            }

            MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>(persisted.Topic),
                new ReadOnlySequence<byte>(persisted.Properties),
                new ReadOnlySequence<byte>(persisted.Payload),
                persisted.QualityOfServiceLevel,
                persisted.Retain,
                null,
                persisted.SubscriptionIdentifier,
                _bufferPool);
            if (persisted.Kind == MqttZeroPersistedMessageKind.InboundQos2)
            {
                session.InboundQos2Messages[persisted.PacketIdentifier] = lease;
                continue;
            }

            MqttZeroQueuedMessage message = MqttZeroQueuedMessage.Rent(
                lease,
                persisted.QualityOfServiceLevel,
                persisted.Retain,
                persisted.SubscriptionIdentifier,
                persisted.ExpiresAt);
            lease.Release();
            MqttZeroOutboundState state = persisted.State switch
            {
                MqttZeroPersistedDeliveryState.AwaitPubComp =>
                    MqttZeroOutboundState.AwaitPubComp,
                MqttZeroPersistedDeliveryState.AwaitPubRec =>
                    MqttZeroOutboundState.AwaitPubRec,
                _ => MqttZeroOutboundState.AwaitPubAck
            };
            message.Restore(
                persisted.MessageId,
                persisted.PacketIdentifier,
                state);
            if (persisted.Kind == MqttZeroPersistedMessageKind.Outbound)
            {
                session.InflightMessages[message.PacketIdentifier] = message;
            }
            else
            {
                session.PendingMessages.Add(message);
            }
        }

        IReadOnlyList<MqttZeroPersistedRetainedMessage> retained =
            await _retainedMessageStore.LoadAllAsync(cancellationToken).ConfigureAwait(false);
        foreach (MqttZeroPersistedRetainedMessage persisted in retained)
        {
            if (persisted.ExpiresAt.HasValue &&
                persisted.ExpiresAt.Value <= _timeProvider.GetUtcNow())
            {
                await _retainedMessageStore.DeleteAsync(
                    Encoding.UTF8.GetString(persisted.Topic.Span),
                    cancellationToken).ConfigureAwait(false);
                continue;
            }

            MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>(persisted.Topic),
                new ReadOnlySequence<byte>(persisted.Properties),
                new ReadOnlySequence<byte>(persisted.Payload),
                persisted.QualityOfServiceLevel,
                true,
                null,
                null,
                _bufferPool);
            _retainedMessages[Encoding.UTF8.GetString(lease.Topic.Span)] =
                new MqttZeroRetainedMessage(
                    lease,
                    persisted.QualityOfServiceLevel,
                    persisted.ExpiresAt);
        }

        RebuildSubscriptionRouteSnapshot();
    }

    private ValueTask SaveSessionAsync(
        MqttZeroSessionState session,
        CancellationToken cancellationToken)
    {
        if (!_persistenceWriter.IsEnabled)
        {
            return ValueTask.CompletedTask;
        }

        List<MqttZeroPersistedSubscription> subscriptions = [];
        lock (session.SyncRoot)
        {
            foreach (MqttZeroSubscription subscription in session.Subscriptions)
            {
                subscriptions.Add(new MqttZeroPersistedSubscription
                {
                    TopicFilter = subscription.TopicFilter,
                    QualityOfServiceLevel = subscription.QualityOfServiceLevel,
                    NoLocal = subscription.NoLocal,
                    RetainAsPublished = subscription.RetainAsPublished,
                    SubscriptionIdentifier = subscription.SubscriptionIdentifier
                });
            }
        }

        return _persistenceWriter.EnqueueSessionSaveAsync(
            new MqttZeroPersistedSession
            {
                ClientId = session.ClientId,
                CleanStart = session.CleanStart,
                ExpiryInterval = session.ExpiryInterval,
                DisconnectedAt = session.DisconnectedAt,
                Subscriptions = subscriptions
            },
            cancellationToken);
    }

    private ValueTask SaveRetainedAsync(
        MqttZeroMessageLease lease,
        DateTimeOffset? expiresAt,
        CancellationToken cancellationToken)
    {
        if (!_persistenceWriter.IsEnabled)
        {
            return ValueTask.CompletedTask;
        }

        return _persistenceWriter.EnqueueRetainedSaveAsync(
            new MqttZeroPersistedRetainedMessage
            {
                Topic = lease.Topic,
                Properties = lease.Properties,
                Payload = lease.Payload,
                QualityOfServiceLevel = lease.QualityOfServiceLevel,
                ExpiresAt = expiresAt
            },
            cancellationToken);
    }

    private ValueTask SaveQueuedAsync(
        MqttZeroQueuedMessage message,
        string clientId,
        CancellationToken cancellationToken)
    {
        if (!_persistenceWriter.IsEnabled)
        {
            return ValueTask.CompletedTask;
        }

        return _persistenceWriter.EnqueueQueuedMessageAsync(
            CreatePersistedMessage(
                message,
                clientId,
                MqttZeroPersistedMessageKind.Queued),
            cancellationToken);
    }

    private ValueTask SaveOutboundAsync(
        MqttZeroQueuedMessage message,
        string clientId,
        CancellationToken cancellationToken)
    {
        if (!_persistenceWriter.IsEnabled)
        {
            return ValueTask.CompletedTask;
        }

        return _persistenceWriter.EnqueueOutboundMessageAsync(
            CreatePersistedMessage(
                message,
                clientId,
                MqttZeroPersistedMessageKind.Outbound),
            cancellationToken);
    }

    private static MqttZeroPersistedMessage CreatePersistedMessage(
        MqttZeroQueuedMessage message,
        string clientId,
        MqttZeroPersistedMessageKind kind)
    {
        return new MqttZeroPersistedMessage
        {
            ClientId = clientId,
            MessageId = message.MessageId,
            PacketIdentifier = message.PacketIdentifier,
            Kind = kind,
            State = message.State switch
            {
                MqttZeroOutboundState.AwaitPubComp =>
                    MqttZeroPersistedDeliveryState.AwaitPubComp,
                MqttZeroOutboundState.AwaitPubRec =>
                    MqttZeroPersistedDeliveryState.AwaitPubRec,
                _ => MqttZeroPersistedDeliveryState.AwaitPubAck
            },
            Topic = message.Topic,
            Properties = message.Properties,
            Payload = message.Payload,
            QualityOfServiceLevel = message.QualityOfServiceLevel,
            Retain = message.Retain,
            SubscriptionIdentifier = message.SubscriptionIdentifier,
            ExpiresAt = message.ExpiresAt
        };
    }

    private static MqttZeroPersistedMessage CreateInboundPersistedMessage(
        MqttZeroMessageLease lease,
        string clientId,
        ushort packetIdentifier)
    {
        return new MqttZeroPersistedMessage
        {
            ClientId = clientId,
            MessageId = packetIdentifier,
            PacketIdentifier = packetIdentifier,
            Kind = MqttZeroPersistedMessageKind.InboundQos2,
            State = MqttZeroPersistedDeliveryState.Queued,
            Topic = lease.Topic,
            Properties = lease.Properties,
            Payload = lease.Payload,
            QualityOfServiceLevel = lease.QualityOfServiceLevel,
            Retain = lease.RetainFlag
        };
    }
}
