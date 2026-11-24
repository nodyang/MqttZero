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
                    _timeProvider.GetUtcNow() &&
                persisted.Will == null)
            {
                await _sessionStore.DeleteAsync(persisted.ClientId, cancellationToken)
                    .ConfigureAwait(false);
                continue;
            }

            MqttZeroSessionState session = new(persisted.ClientId, _options.MaxMessageQueueLength)
            {
                CleanStart = persisted.CleanStart,
                ExpiryInterval = persisted.ExpiryInterval,
                DisconnectedAt = persisted.DisconnectedAt,
                WillMessage = persisted.Will == null
                    ? null
                    : new MqttZeroWillMessage(persisted.Will)
            };
            if (session.DisconnectedAt.HasValue &&
                session.WillMessage != null)
            {
                uint delay = session.WillMessage.DelayInterval;
                if (session.ExpiryInterval > 0)
                {
                    delay = Math.Min(delay, session.ExpiryInterval);
                }

                session.WillDueAt = session.DisconnectedAt.Value.AddSeconds(delay);
            }

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
                if (_messageStore.IsEnabled)
                {
                    await _messageStore.DeleteByClientAsync(
                        persisted.ClientId,
                        cancellationToken).ConfigureAwait(false);
                }

                continue;
            }

            if (persisted.ExpiresAt.HasValue &&
                persisted.ExpiresAt.Value <= _timeProvider.GetUtcNow())
            {
                if (persisted.Kind == MqttZeroPersistedMessageKind.InboundQos2)
                {
                    await _messageStore.DeleteInboundQos2Async(
                        persisted.ClientId,
                        persisted.PacketIdentifier,
                        cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    await _messageStore.DeleteOutboundAsync(
                        persisted.ClientId,
                        persisted.MessageId,
                        cancellationToken).ConfigureAwait(false);
                }

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
                if (session.InboundQos2Messages.Count >= _options.MaxInflight)
                {
                    await _messageStore.DeleteInboundQos2Async(
                        persisted.ClientId,
                        persisted.PacketIdentifier,
                        cancellationToken).ConfigureAwait(false);
                    continue;
                }

                lease.SetExpiresAt(persisted.ExpiresAt);
                session.InboundQos2Messages[persisted.PacketIdentifier] = lease;
                continue;
            }

            if ((persisted.Kind == MqttZeroPersistedMessageKind.Outbound &&
                    session.InflightMessages.Count >= _options.MaxInflight) ||
                (persisted.Kind == MqttZeroPersistedMessageKind.Queued &&
                    session.PendingMessages.Count >= _options.MaxMessageQueueLength))
            {
                await _messageStore.DeleteOutboundAsync(
                    persisted.ClientId,
                    persisted.MessageId,
                    cancellationToken).ConfigureAwait(false);
                lease.Release();
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
        if (!_sessionStore.IsEnabled)
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

        return _sessionStore.SaveAsync(
            new MqttZeroPersistedSession
            {
                ClientId = session.ClientId,
                CleanStart = session.CleanStart,
                ExpiryInterval = session.ExpiryInterval,
                DisconnectedAt = session.DisconnectedAt,
                Will = CreatePersistedWill(session.WillMessage),
                Subscriptions = subscriptions
            },
            cancellationToken);
    }

    private ValueTask SaveRetainedAsync(
        MqttZeroMessageLease lease,
        DateTimeOffset? expiresAt,
        CancellationToken cancellationToken)
    {
        if (!_retainedMessageStore.IsEnabled)
        {
            return ValueTask.CompletedTask;
        }

        return _retainedMessageStore.SaveAsync(
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
        if (!_messageStore.IsEnabled)
        {
            return ValueTask.CompletedTask;
        }

        return _messageStore.SaveQueuedAsync(
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
        if (!_messageStore.IsEnabled)
        {
            return ValueTask.CompletedTask;
        }

        return _messageStore.SaveOutboundAsync(
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
            Retain = lease.RetainFlag,
            ExpiresAt = lease.ExpiresAt
        };
    }

    private DateTimeOffset? CalculateMessageExpiresAt(uint? expiryInterval)
    {
        return expiryInterval.HasValue
            ? _timeProvider.GetUtcNow().AddSeconds(expiryInterval.Value)
            : null;
    }

    private static uint? GetRemainingMessageExpiry(
        DateTimeOffset? expiresAt,
        DateTimeOffset now)
    {
        if (!expiresAt.HasValue)
        {
            return null;
        }

        double remainingSeconds = Math.Ceiling((expiresAt.Value - now).TotalSeconds);
        if (remainingSeconds <= 0)
        {
            return 0;
        }

        return (uint)Math.Min(remainingSeconds, uint.MaxValue);
    }

    private static MqttZeroPersistedWill? CreatePersistedWill(MqttZeroWillMessage? will)
    {
        if (will == null)
        {
            return null;
        }

        return new MqttZeroPersistedWill
        {
            Topic = will.Topic,
            Payload = will.Payload,
            Properties = will.Properties,
            QualityOfServiceLevel = will.QualityOfServiceLevel,
            Retain = will.Retain,
            DelayInterval = will.DelayInterval,
            MessageExpiryInterval = will.MessageExpiryInterval
        };
    }

    private ValueTask DeleteOutboundAsync(
        string clientId,
        long messageId,
        CancellationToken cancellationToken)
    {
        if (!_messageStore.IsEnabled)
        {
            return ValueTask.CompletedTask;
        }

        return _messageStore.DeleteOutboundAsync(clientId, messageId, cancellationToken);
    }

    private ValueTask DeleteInboundQos2Async(
        string clientId,
        ushort packetIdentifier,
        CancellationToken cancellationToken)
    {
        if (!_messageStore.IsEnabled)
        {
            return ValueTask.CompletedTask;
        }

        return _messageStore.DeleteInboundQos2Async(
            clientId,
            packetIdentifier,
            cancellationToken);
    }

    private async ValueTask DeleteClientPersistenceAsync(
        string clientId,
        CancellationToken cancellationToken)
    {
        if (_messageStore.IsEnabled)
        {
            await _messageStore.DeleteByClientAsync(clientId, cancellationToken)
                .ConfigureAwait(false);
        }

        if (_sessionStore.IsEnabled)
        {
            await _sessionStore.DeleteAsync(clientId, cancellationToken)
                .ConfigureAwait(false);
        }
    }
}
