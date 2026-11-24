using MqttZero.Server.Clients;
using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Publishing;
using MqttZero.Server.Retained;
using MqttZero.Server.Sessions;
using System.Buffers;
using System.Text;

namespace MqttZero.Server.Broker;

public sealed partial class MqttZeroBroker
{
    /// <summary>Tries to get the connected client handle for a client identifier.</summary>
    public bool TryGetClient(
        string clientId,
        out MqttZeroConnectedClient? client)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clientId);
        if (!_sessions.TryGetValue(clientId, out MqttZeroSessionState? session))
        {
            client = null;
            return false;
        }

        MqttZeroClientConnection? connection;
        lock (session.SyncRoot)
        {
            connection = session.Connection;
        }

        if (connection == null)
        {
            client = null;
            return false;
        }

        client = connection.ConnectedClient;
        return true;
    }

    internal static bool IsConnectionAttached(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection)
    {
        lock (session.SyncRoot)
        {
            return ReferenceEquals(session.Connection, connection);
        }
    }

    internal static MqttZeroClientSnapshot CreateClientSnapshot(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection)
    {
        EnsureAttached(session, connection);
        int subscriptionCount;
        lock (session.SyncRoot)
        {
            subscriptionCount = session.Subscriptions.Count;
        }

        return new MqttZeroClientSnapshot
        {
            ClientId = session.ClientId,
            UserName = connection.ClientInfo.UserName,
            RemoteEndPoint = connection.RemoteEndPoint,
            IsSecure = connection.IsSecure,
            IsWebSocket = connection.IsWebSocket,
            ProtocolVersion = connection.ClientInfo.ProtocolVersion,
            SubscriptionCount = subscriptionCount,
            Statistics = connection.GetStatistics()
        };
    }

    internal async ValueTask<IReadOnlyList<MqttZeroSubscribeResult>> SubscribeConnectedClientAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        IReadOnlyList<MqttSubscriptionRequest> subscriptions,
        uint? subscriptionIdentifier,
        CancellationToken cancellationToken)
    {
        EnsureAttached(session, connection);
        ArgumentNullException.ThrowIfNull(subscriptions);
        List<MqttZeroSubscribeResult> results = new(subscriptions.Count);
        bool changed = false;
        List<MqttZeroSubscription>? subscriptionSnapshot = null;
        if (_sessionStore.IsEnabled)
        {
            lock (session.SyncRoot)
            {
                subscriptionSnapshot = [.. session.Subscriptions];
            }
        }

        for (int index = 0; index < subscriptions.Count; index++)
        {
            MqttSubscriptionRequest request = subscriptions[index];
            if (!MqttZeroProtocolValidator.IsValidTopicFilter(request.TopicFilter))
            {
                results.Add(new MqttZeroSubscribeResult
                {
                    TopicFilter = Encoding.UTF8.GetString(request.TopicFilter.FirstSpan),
                    QualityOfServiceLevel = request.QualityOfServiceLevel,
                    ReasonCode = MqttReasonCode.TopicFilterInvalid
                });
                continue;
            }

            MqttZeroSubscribeContext context = new()
            {
                ConnectedClient = connection.ConnectedClient,
                TopicFilter = request.TopicFilter,
                QualityOfServiceLevel = request.QualityOfServiceLevel,
                NoLocal = request.NoLocal,
                RetainAsPublished = request.RetainAsPublished,
                RetainHandling = request.RetainHandling
            };
            MqttZeroInterceptionResult result = MqttZeroInterceptionResult.Continue;
            foreach (IMqttZeroSubscribeInterceptor interceptor in _subscribeInterceptors)
            {
                result = await interceptor.InterceptAsync(context, cancellationToken)
                    .ConfigureAwait(false);
                if (result != MqttZeroInterceptionResult.Continue)
                {
                    break;
                }
            }

            if (result == MqttZeroInterceptionResult.CloseConnection)
            {
                await connection.CloseAsync(context.ReasonCode, cancellationToken)
                    .ConfigureAwait(false);
                throw new InvalidOperationException("The subscription closed the connection.");
            }

            if (result == MqttZeroInterceptionResult.Consume)
            {
                results.Add(new MqttZeroSubscribeResult
                {
                    TopicFilter = Encoding.UTF8.GetString(request.TopicFilter.FirstSpan),
                    QualityOfServiceLevel = request.QualityOfServiceLevel,
                    ReasonCode = context.ReasonCode
                });
                continue;
            }

            byte[] filter = request.TopicFilter.ToArray();
            bool existed;
            lock (session.SyncRoot)
            {
                int existingIndex = session.Subscriptions.FindIndex(
                    item => item.TopicFilter.AsSpan().SequenceEqual(filter));
                existed = existingIndex >= 0;
                if (!existed &&
                    session.Subscriptions.Count >= _options.MaxSubscriptionsPerSession)
                {
                    results.Add(new MqttZeroSubscribeResult
                    {
                        TopicFilter = Encoding.UTF8.GetString(filter),
                        ReasonCode = MqttReasonCode.QuotaExceeded
                    });
                    continue;
                }

                MqttZeroSubscription subscription = new(
                    filter,
                    request.QualityOfServiceLevel,
                    request.NoLocal,
                    request.RetainAsPublished,
                    subscriptionIdentifier);
                if (existingIndex >= 0)
                {
                    session.Subscriptions[existingIndex] = subscription;
                }
                else
                {
                    session.Subscriptions.Add(subscription);
                }
            }

            changed = true;
            MqttReasonCode reasonCode = request.QualityOfServiceLevel switch
            {
                MqttQualityOfServiceLevel.ExactlyOnce => MqttReasonCode.GrantedQos2,
                MqttQualityOfServiceLevel.AtLeastOnce => MqttReasonCode.GrantedQos1,
                _ => MqttReasonCode.Success
            };
            results.Add(new MqttZeroSubscribeResult
            {
                TopicFilter = Encoding.UTF8.GetString(filter),
                QualityOfServiceLevel = request.QualityOfServiceLevel,
                ReasonCode = reasonCode
            });

            if (request.RetainHandling == MqttRetainHandling.DoNotSendOnSubscribe ||
                (request.RetainHandling ==
                    MqttRetainHandling.SendAtSubscribeIfNewSubscriptionOnly && existed))
            {
                continue;
            }

            foreach (MqttZeroRetainedMessage retained in _retainedMessages.Values)
            {
                if (retained.ExpiresAt.HasValue &&
                    retained.ExpiresAt.Value <= _timeProvider.GetUtcNow())
                {
                    continue;
                }

                if (!MqttZero.Server.Routing.MqttZeroTopicMatcher.Matches(
                        filter,
                        retained.Topic.Span))
                {
                    continue;
                }

                MqttQualityOfServiceLevel qos =
                    (MqttQualityOfServiceLevel)Math.Min(
                        (byte)retained.QualityOfServiceLevel,
                        (byte)request.QualityOfServiceLevel);
                MqttPublishPacket packet = new()
                {
                    TopicName = new ReadOnlySequence<byte>(retained.Topic),
                    Payload = new ReadOnlySequence<byte>(retained.Payload),
                    QualityOfServiceLevel = qos,
                    Retain = true,
                    SubscriptionIdentifier = subscriptionIdentifier
                };
                packet.MessageExpiryInterval =
                    GetRemainingMessageExpiry(
                        retained.ExpiresAt,
                        _timeProvider.GetUtcNow());
                await DeliverAsync(
                    session,
                    packet,
                    qos,
                    true,
                    subscriptionIdentifier,
                    cancellationToken).ConfigureAwait(false);
            }
        }

        if (changed)
        {
            RebuildSubscriptionRouteSnapshot();
            try
            {
                await SaveSessionAsync(session, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                if (subscriptionSnapshot != null)
                {
                    lock (session.SyncRoot)
                    {
                        session.Subscriptions.Clear();
                        session.Subscriptions.AddRange(subscriptionSnapshot);
                    }

                    RebuildSubscriptionRouteSnapshot();
                }

                throw;
            }
        }

        return results;
    }

    internal async ValueTask<IReadOnlyList<MqttZeroUnsubscribeResult>> UnsubscribeConnectedClientAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        IReadOnlyList<string> topicFilters,
        CancellationToken cancellationToken)
    {
        EnsureAttached(session, connection);
        ArgumentNullException.ThrowIfNull(topicFilters);
        List<MqttZeroUnsubscribeResult> results = new(topicFilters.Count);
        bool changed = false;
        List<MqttZeroSubscription>? subscriptionSnapshot = null;
        if (_sessionStore.IsEnabled)
        {
            lock (session.SyncRoot)
            {
                subscriptionSnapshot = [.. session.Subscriptions];
            }
        }

        foreach (string topicFilter in topicFilters)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(topicFilter);
            ReadOnlySequence<byte> encodedFilter =
                new(Encoding.UTF8.GetBytes(topicFilter));
            if (!MqttZeroProtocolValidator.IsValidTopicFilter(encodedFilter))
            {
                results.Add(new MqttZeroUnsubscribeResult
                {
                    TopicFilter = topicFilter,
                    ReasonCode = MqttReasonCode.TopicFilterInvalid
                });
                continue;
            }

            MqttZeroUnsubscribeContext context = new()
            {
                ConnectedClient = connection.ConnectedClient,
                TopicFilter = encodedFilter
            };
            MqttZeroInterceptionResult interception = MqttZeroInterceptionResult.Continue;
            foreach (IMqttZeroUnsubscribeInterceptor interceptor in _unsubscribeInterceptors)
            {
                interception = await interceptor.InterceptAsync(context, cancellationToken)
                    .ConfigureAwait(false);
                if (interception != MqttZeroInterceptionResult.Continue)
                {
                    break;
                }
            }

            if (interception == MqttZeroInterceptionResult.CloseConnection)
            {
                await connection.CloseAsync(context.ReasonCode, cancellationToken)
                    .ConfigureAwait(false);
                throw new InvalidOperationException("The unsubscribe closed the connection.");
            }

            if (interception == MqttZeroInterceptionResult.Consume)
            {
                results.Add(new MqttZeroUnsubscribeResult
                {
                    TopicFilter = topicFilter,
                    ReasonCode = context.ReasonCode
                });
                continue;
            }

            byte[] filter = encodedFilter.ToArray();
            bool removed = false;
            lock (session.SyncRoot)
            {
                int index = session.Subscriptions.FindIndex(
                    item => item.TopicFilter.AsSpan().SequenceEqual(filter));
                if (index >= 0)
                {
                    session.Subscriptions.RemoveAt(index);
                    removed = true;
                }
            }

            changed |= removed;
            results.Add(new MqttZeroUnsubscribeResult
            {
                TopicFilter = topicFilter,
                ReasonCode = removed
                    ? MqttReasonCode.Success
                    : MqttReasonCode.NoSubscriptionExisted
            });
        }

        if (changed)
        {
            RebuildSubscriptionRouteSnapshot();
            try
            {
                await SaveSessionAsync(session, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                if (subscriptionSnapshot != null)
                {
                    lock (session.SyncRoot)
                    {
                        session.Subscriptions.Clear();
                        session.Subscriptions.AddRange(subscriptionSnapshot);
                    }

                    RebuildSubscriptionRouteSnapshot();
                }

                throw;
            }
        }

        return results;
    }

    internal async ValueTask<bool> SendPublishToClientAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttZeroPublishMessage message,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentException.ThrowIfNullOrWhiteSpace(message.Topic);
        if (!IsConnectionAttached(session, connection) ||
            State != MqttZeroBrokerState.Running)
        {
            return false;
        }

        ReadOnlySequence<byte> topic = new(Encoding.UTF8.GetBytes(message.Topic));
        if (!MqttZeroProtocolValidator.IsValidTopicName(topic))
        {
            throw new ArgumentException("The MQTT topic name is invalid.", nameof(message));
        }

        MqttPublishProperties properties = message.Properties ?? MqttPublishProperties.Empty;
        if (properties.PayloadFormatIndicator == MqttPayloadFormatIndicator.Utf8 &&
            !MqttZeroProtocolValidator.IsValidUtf8(message.Payload))
        {
            throw new ArgumentException("The MQTT payload is not valid UTF-8.", nameof(message));
        }

        ReadOnlySequence<byte> encodedProperties = MqttPublishPropertiesCodec.Encode(properties);
        MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
            topic,
            encodedProperties,
            message.Payload,
            message.QualityOfServiceLevel,
            message.Retain,
            properties.MessageExpiryInterval,
            null,
            _bufferPool);
        lease.SetExpiresAt(CalculateMessageExpiresAt(properties.MessageExpiryInterval));
        try
        {
            MqttZeroPublishView view = new(
                lease.TopicSequence,
                lease.PropertiesSequence,
                lease.PayloadSequence,
                lease.QualityOfServiceLevel,
                0,
                false,
                lease.RetainFlag,
                lease.MessageExpiryInterval,
                null,
                null);
            if (MqttPublishPacketCodec.GetEncodedLength(view) > _options.MaxPacketSize)
            {
                throw new InvalidOperationException(
                    "The outbound MQTT packet exceeds the broker maximum packet size.");
            }

            await DeliverLeaseAsync(
                session,
                lease,
                message.QualityOfServiceLevel,
                message.Retain,
                ReadOnlyMemory<uint>.Empty,
                null,
                cancellationToken).ConfigureAwait(false);
            return true;
        }
        finally
        {
            lease.Release();
        }
    }

    internal static async ValueTask<bool> DisconnectConnectedClientAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        MqttReasonCode reasonCode,
        CancellationToken cancellationToken)
    {
        if (!IsConnectionAttached(session, connection))
        {
            return false;
        }

        await connection.CloseAsync(reasonCode, cancellationToken).ConfigureAwait(false);
        return true;
    }

    private static void EnsureAttached(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection)
    {
        lock (session.SyncRoot)
        {
            if (!ReferenceEquals(session.Connection, connection))
            {
                throw new InvalidOperationException("The connected client handle is stale.");
            }
        }
    }
}
