using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Text;

namespace MqttZero;

/// <summary>
/// Provides the in-memory MQTT 5 broker pipeline.
/// </summary>
public sealed class MqttZeroBroker : IAsyncDisposable
{
    private readonly object _sessionGate = new();
    private readonly MqttZeroBufferPool _bufferPool;
    private readonly IMqttZeroPublishInterceptor[] _publishInterceptors;
    private readonly ConcurrentDictionary<string, MqttZeroRetainedMessage> _retainedMessages =
        new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, MqttZeroSessionState> _sessions =
        new(StringComparer.Ordinal);
    private MqttZeroRouteEntry[] _subscriptionRouteSnapshot = [];
    private readonly MqttZeroRouteScopePool _routeScopes = new();
    private readonly ConcurrentDictionary<string, int> _sharedSubscriptionCursors =
        new(StringComparer.Ordinal);
    private readonly MqttZeroOptions _options;
    private readonly TimeProvider _timeProvider;
    private CancellationTokenSource? _maintenanceCancellation;
    private Task? _maintenanceTask;
    private int _connectionCount;
    private int _disposed;
    private long _publishedWillCount;
    private long _retainedBytes;
    private int _stopping;
    private int _state;

    /// <summary>
    /// Initializes a new broker.
    /// </summary>
    /// <param name="options">The broker options.</param>
    /// <param name="timeProvider">The time provider used for broker timings.</param>
    public MqttZeroBroker(MqttZeroOptions options, TimeProvider timeProvider)
        : this(options, timeProvider, [])
    {
    }

    /// <summary>
    /// Initializes a new broker with publish interceptors.
    /// </summary>
    /// <param name="options">The broker options.</param>
    /// <param name="timeProvider">The time provider used for broker timings.</param>
    /// <param name="publishInterceptors">The ordered publish interceptors.</param>
    public MqttZeroBroker(
        MqttZeroOptions options,
        TimeProvider timeProvider,
        IEnumerable<IMqttZeroPublishInterceptor> publishInterceptors)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
        ArgumentNullException.ThrowIfNull(publishInterceptors);
        _bufferPool = new MqttZeroBufferPool(options.MaxBufferedBytes);
        _publishInterceptors = publishInterceptors
            .OrderBy(static interceptor =>
                interceptor is MqttZeroOrderedPublishInterceptor ordered
                    ? ordered.Order
                    : 0)
            .ToArray();
    }

    /// <summary>
    /// Gets the current broker lifecycle state.
    /// </summary>
    public MqttZeroBrokerState State
    {
        get
        {
            return (MqttZeroBrokerState)Volatile.Read(ref _state);
        }
    }

    /// <summary>
    /// Gets the current connected client count.
    /// </summary>
    public int ConnectedClientCount
    {
        get
        {
            return Volatile.Read(ref _connectionCount);
        }
    }

    /// <summary>
    /// Gets the number of will messages published since broker creation.
    /// </summary>
    public long PublishedWillCount
    {
        get
        {
            return Volatile.Read(ref _publishedWillCount);
        }
    }

    /// <summary>
    /// Gets the time provider used by the broker.
    /// </summary>
    public TimeProvider TimeProvider
    {
        get
        {
            return _timeProvider;
        }
    }

    /// <summary>
    /// Gets a point-in-time snapshot of connected clients.
    /// </summary>
    /// <returns>The connected client snapshots.</returns>
    public IReadOnlyList<MqttZeroClientSnapshot> GetClients()
    {
        List<MqttZeroClientSnapshot> clients = [];
        foreach (MqttZeroSessionState session in _sessions.Values)
        {
            MqttZeroClientConnection? connection;
            int subscriptionCount;
            lock (session.SyncRoot)
            {
                connection = session.Connection;
                subscriptionCount = session.Subscriptions.Count;
            }

            if (connection == null)
            {
                continue;
            }

            clients.Add(new MqttZeroClientSnapshot
            {
                ClientId = session.ClientId,
                RemoteEndPoint = connection.RemoteEndPoint,
                IsSecure = connection.IsSecure,
                IsWebSocket = connection.IsWebSocket,
                SubscriptionCount = subscriptionCount
            });
        }

        return clients;
    }

    /// <summary>
    /// Gets snapshots for all stored sessions.
    /// </summary>
    /// <returns>The session snapshots.</returns>
    public IReadOnlyList<MqttZeroSessionSnapshot> GetSessions()
    {
        DateTimeOffset now = _timeProvider.GetUtcNow();
        List<MqttZeroSessionSnapshot> snapshots = [];
        foreach (MqttZeroSessionState session in _sessions.Values)
        {
            snapshots.Add(session.CreateSnapshot(now));
        }

        return snapshots;
    }

    /// <summary>
    /// Gets retained message metadata snapshots.
    /// </summary>
    /// <returns>The retained message snapshots.</returns>
    public IReadOnlyList<MqttZeroRetainedMessageSnapshot> GetRetainedMessages()
    {
        DateTimeOffset now = _timeProvider.GetUtcNow();
        List<MqttZeroRetainedMessageSnapshot> messages = [];
        foreach (KeyValuePair<string, MqttZeroRetainedMessage> pair in _retainedMessages)
        {
            MqttZeroRetainedMessage message = pair.Value;
            if (message.ExpiresAt.HasValue && message.ExpiresAt.Value <= now)
            {
                RemoveRetainedMessage(pair.Key);
                continue;
            }

            messages.Add(new MqttZeroRetainedMessageSnapshot
            {
                Topic = Encoding.UTF8.GetString(message.Topic.Span),
                PayloadLength = message.Payload.Length,
                QualityOfServiceLevel = message.QualityOfServiceLevel
            });
        }

        return messages;
    }

    /// <summary>
    /// Publishes a server-originated message to matching subscribers.
    /// </summary>
    /// <param name="topic">The UTF-8 topic name.</param>
    /// <param name="payload">The message payload bytes.</param>
    /// <param name="qualityOfServiceLevel">The requested quality of service.</param>
    /// <param name="retain">Whether the message should be retained.</param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>A task representing the publish operation.</returns>
    public async ValueTask PublishAsync(
        string topic,
        ReadOnlyMemory<byte> payload,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool retain,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);
        if (State != MqttZeroBrokerState.Running)
        {
            throw new InvalidOperationException("The MQTTZero broker is not running.");
        }

        MqttPublishPacket packet = new()
        {
            TopicName = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(topic)),
            Payload = new ReadOnlySequence<byte>(payload),
            QualityOfServiceLevel = qualityOfServiceLevel,
            Retain = retain
        };
        await RoutePublishAsync(null, packet, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Disconnects one connected client.
    /// </summary>
    /// <param name="clientId">The client identifier.</param>
    /// <param name="reasonCode">The DISCONNECT reason code.</param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns><see langword="true"/> when a connected client was found.</returns>
    public async ValueTask<bool> DisconnectClientAsync(
        string clientId,
        MqttReasonCode reasonCode,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(clientId);
        if (!_sessions.TryGetValue(clientId, out MqttZeroSessionState? session))
        {
            return false;
        }

        MqttZeroClientConnection? connection;
        lock (session.SyncRoot)
        {
            connection = session.Connection;
        }

        if (connection == null)
        {
            return false;
        }

        await connection.CloseAsync(reasonCode, cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>
    /// Starts the broker and maintenance scheduler.
    /// </summary>
    /// <param name="cancellationToken">A token that cancels startup.</param>
    /// <returns>A task representing startup.</returns>
    public ValueTask StartAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (State == MqttZeroBrokerState.Running)
        {
            return ValueTask.CompletedTask;
        }

        Volatile.Write(ref _stopping, 0);
        Volatile.Write(ref _state, (int)MqttZeroBrokerState.Running);
        _maintenanceCancellation = new CancellationTokenSource();
        _maintenanceTask = RunMaintenanceAsync(_maintenanceCancellation.Token);
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Stops the broker and closes active connections.
    /// </summary>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>A task representing shutdown.</returns>
    public async ValueTask StopAsync(CancellationToken cancellationToken = default)
    {
        if (State is MqttZeroBrokerState.Stopped or MqttZeroBrokerState.Created)
        {
            return;
        }

        Volatile.Write(ref _stopping, 1);
        Volatile.Write(ref _state, (int)MqttZeroBrokerState.Stopping);
        if (_maintenanceCancellation != null)
        {
            await _maintenanceCancellation.CancelAsync().ConfigureAwait(false);
        }

        if (_maintenanceTask != null)
        {
            try
            {
                await _maintenanceTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        }

        foreach (MqttZeroSessionState session in _sessions.Values)
        {
            MqttZeroClientConnection? connection;
            lock (session.SyncRoot)
            {
                connection = session.Connection;
            }

            if (connection == null)
            {
                continue;
            }

            try
            {
                await connection.CloseAsync(MqttReasonCode.ServerShuttingDown, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception) when (cancellationToken.IsCancellationRequested)
            {
            }
        }

        foreach (MqttZeroSessionState session in _sessions.Values)
        {
            ReleaseSessionMessages(session);
        }

        foreach (string topic in _retainedMessages.Keys)
        {
            RemoveRetainedMessage(topic);
        }

        _sessions.Clear();
        _subscriptionRouteSnapshot = [];
        Volatile.Write(ref _connectionCount, 0);
        Volatile.Write(ref _state, (int)MqttZeroBrokerState.Stopped);
    }

    /// <summary>
    /// Processes one accepted Kestrel connection until it disconnects.
    /// </summary>
    /// <param name="input">The inbound byte pipe.</param>
    /// <param name="output">The outbound byte pipe.</param>
    /// <param name="remoteEndPoint">The optional remote endpoint description.</param>
    /// <param name="isSecure">Whether the connection uses TLS.</param>
    /// <param name="isWebSocket">Whether the connection uses WebSocket.</param>
    /// <param name="cancellationToken">A token that terminates the connection.</param>
    /// <returns>A task that completes when the connection closes.</returns>
    public async ValueTask AcceptConnectionAsync(
        PipeReader input,
        PipeWriter output,
        string? remoteEndPoint,
        bool isSecure,
        bool isWebSocket,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(output);

        if (!_options.AcceptNewConnections ||
            State != MqttZeroBrokerState.Running ||
            Interlocked.Increment(ref _connectionCount) > _options.MaxConnections)
        {
            Interlocked.Decrement(ref _connectionCount);
            await output.CompleteAsync().ConfigureAwait(false);
            return;
        }

        MqttZeroClientConnection? connection = null;
        MqttZeroSessionState? session = null;
        CancellationTokenRegistration closeRegistration = default;
        bool cleanDisconnect = false;
        try
        {
            using CancellationTokenSource connectionLifetime =
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            using CancellationTokenSource connectTimeout =
                CancellationTokenSource.CreateLinkedTokenSource(connectionLifetime.Token);
            connectTimeout.CancelAfter(_options.ConnectTimeout);

            while (!connectionLifetime.IsCancellationRequested)
            {
                CancellationToken readToken = session == null
                    ? connectTimeout.Token
                    : connectionLifetime.Token;
                ReadResult readResult = await input.ReadAsync(readToken).ConfigureAwait(false);
                ReadOnlySequence<byte> buffer = readResult.Buffer;
                SequencePosition examined = readResult.Buffer.End;

                if (MqttPacketCodec.TryGetPacketLength(buffer, out long packetLength, out _) &&
                    packetLength > _options.MaxPacketSize)
                {
                    throw new InvalidOperationException("The inbound MQTT packet exceeds the broker maximum.");
                }

                while (true)
                {
                    if (session != null &&
                        MqttPublishPacketCodec.TryDecode(
                            buffer,
                            out MqttZeroPublishView publishView,
                            out long publishConsumed))
                    {
                        connection!.MarkPacketReceived(_timeProvider);
                        try
                        {
                            MqttZeroMessageLease lease = CreateMessageLease(connection, publishView);
                            try
                            {
                                await RoutePublishLeaseAsync(
                                    session,
                                    connection,
                                    lease,
                                    connectionLifetime.Token).ConfigureAwait(false);
                            }
                            finally
                            {
                                lease.Release();
                            }
                        }
                        catch (MqttZeroBufferPoolExhaustedException)
                        {
                            await _bufferPool.WaitForCapacityAsync(connectionLifetime.Token)
                                .ConfigureAwait(false);
                            continue;
                        }

                        buffer = buffer.Slice(publishConsumed);
                        continue;
                    }

                    if (!MqttPacketCodec.TryDecode(
                        buffer,
                        out MqttPacket? packet,
                        out long consumed) ||
                        packet == null)
                    {
                        break;
                    }

                    if (session == null)
                    {
                        if (packet is not MqttConnectPacket connectPacket)
                        {
                            return;
                        }

                        ConnectionAttachment attachment = await ConnectAsync(
                            connectPacket,
                            output,
                            remoteEndPoint,
                            isSecure,
                            isWebSocket).ConfigureAwait(false);
                        session = attachment.Session;
                        connection = attachment.Connection;
                        closeRegistration = connection.ClosedToken.Register(
                            static state => ((CancellationTokenSource)state!).Cancel(),
                            connectionLifetime);
                        await ResumeSessionAsync(session, connection, connectionLifetime.Token)
                            .ConfigureAwait(false);
                        buffer = buffer.Slice(consumed);
                        continue;
                    }

                    connection!.MarkPacketReceived(_timeProvider);
                    await ProcessPacketAsync(session, connection, packet, connectionLifetime.Token)
                        .ConfigureAwait(false);
                    buffer = buffer.Slice(consumed);

                    if (packet is MqttDisconnectPacket)
                    {
                        cleanDisconnect = true;
                        return;
                    }
                }

                if (readResult.IsCompleted)
                {
                    input.AdvanceTo(buffer.Start);
                    break;
                }

                input.AdvanceTo(buffer.Start, examined);
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (InvalidOperationException) when (session == null)
        {
        }
        finally
        {
            closeRegistration.Dispose();

            if (session != null && connection != null)
            {
                await DetachConnectionAsync(session, connection, cleanDisconnect).ConfigureAwait(false);
                await connection.DisposeAsync().ConfigureAwait(false);
            }

            Interlocked.Decrement(ref _connectionCount);
            await input.CompleteAsync().ConfigureAwait(false);
            if (connection == null)
            {
                await output.CompleteAsync().ConfigureAwait(false);
            }
        }
    }

    private async ValueTask<ConnectionAttachment> ConnectAsync(
        MqttConnectPacket connectPacket,
        PipeWriter output,
        string? remoteEndPoint,
        bool isSecure,
        bool isWebSocket)
    {
        string clientId = connectPacket.ClientId.IsEmpty
            ? $"mqttzero-{Guid.NewGuid():N}"
            : Encoding.UTF8.GetString(connectPacket.ClientId.ToArray());
        uint requestedExpiry = connectPacket.SessionExpiryInterval ?? 0;
        uint maximumExpiry = (uint)Math.Clamp(
            _options.MaxSessionExpiryInterval.TotalSeconds,
            0,
            uint.MaxValue);
        uint effectiveExpiry = Math.Min(requestedExpiry, maximumExpiry);
        DateTimeOffset now = _timeProvider.GetUtcNow();
        MqttZeroClientConnection newConnection = new(
            output,
            remoteEndPoint,
            isSecure,
            isWebSocket,
            connectPacket.KeepAliveSeconds,
            connectPacket.ReceiveMaximum,
            connectPacket.MaximumPacketSize,
            _timeProvider,
            _bufferPool);
        MqttZeroClientConnection? oldConnection = null;
        MqttZeroWillMessage? takeoverWill = null;
        MqttZeroSessionState session;
        bool sessionPresent;

        lock (_sessionGate)
        {
            _sessions.TryGetValue(clientId, out MqttZeroSessionState? existing);
            if (existing != null &&
                (connectPacket.CleanStart || IsSessionExpired(existing, now)))
            {
                _sessions.TryRemove(clientId, out _);
                ReleaseSessionMessages(existing);
                lock (existing.SyncRoot)
                {
                    oldConnection = existing.Connection;
                    if (oldConnection != null &&
                        existing.WillMessage != null &&
                        existing.WillMessage.DelayInterval == 0)
                    {
                        takeoverWill = existing.WillMessage;
                    }

                    existing.Connection = null;
                }

                existing = null;
            }

            if (existing == null)
            {
                if (_sessions.Count >= _options.MaxSessions)
                {
                    throw new InvalidOperationException("The maximum number of MQTT sessions was reached.");
                }

                session = new MqttZeroSessionState(clientId);
                _sessions[clientId] = session;
                sessionPresent = false;
            }
            else
            {
                session = existing;
                sessionPresent = true;
            }

            lock (session.SyncRoot)
            {
                MqttZeroClientConnection? currentConnection = session.Connection;
                if (currentConnection != null)
                {
                    oldConnection = currentConnection;
                    if (session.WillMessage != null &&
                        session.WillMessage.DelayInterval == 0)
                    {
                        takeoverWill = session.WillMessage;
                    }
                }

                session.Connection = newConnection;
                session.CleanStart = connectPacket.CleanStart;
                session.ExpiryInterval = effectiveExpiry;
                session.DisconnectedAt = null;
                session.WillDueAt = null;
                session.WillMessage = connectPacket.HasWill
                    ? new MqttZeroWillMessage(connectPacket)
                    : null;
            }
        }

        RebuildSubscriptionRouteSnapshot();

        if (oldConnection != null && !ReferenceEquals(oldConnection, newConnection))
        {
            try
            {
                await oldConnection.CloseAsync(MqttReasonCode.SessionTakenOver, CancellationToken.None)
                    .ConfigureAwait(false);
            }
            catch (Exception)
            {
            }
        }

        await newConnection.SendAsync(
            new MqttConnAckPacket
            {
                SessionPresent = sessionPresent,
                ReasonCode = MqttReasonCode.Success,
                TopicAliasMaximum = _options.TopicAliasMaximum,
                ReceiveMaximum = (ushort)Math.Min(_options.MaxInflight, ushort.MaxValue),
                MaximumPacketSize = (uint)_options.MaxPacketSize,
                SessionExpiryInterval = effectiveExpiry
            },
            CancellationToken.None).ConfigureAwait(false);
        if (takeoverWill != null)
        {
            await RoutePublishAsync(null, takeoverWill.CreatePacket(), CancellationToken.None)
                .ConfigureAwait(false);
            Interlocked.Increment(ref _publishedWillCount);
        }

        return new ConnectionAttachment(session, newConnection);
    }

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
                await connection.SendAsync(new MqttPingRespPacket(), cancellationToken)
                    .ConfigureAwait(false);
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

        await connection.SendAsync(
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

        await connection.SendAsync(
            new MqttUnsubAckPacket
            {
                PacketIdentifier = packet.PacketIdentifier,
                ReasonCodes = reasonCodes
            },
            cancellationToken).ConfigureAwait(false);
    }

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

            MqttZeroRouteEntry[] routeEntries =
                Volatile.Read(ref _subscriptionRouteSnapshot);
            using MqttZeroRouteScope deliveries = MqttZeroRouteSelector.Select(
                routeEntries,
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

            MqttZeroRouteEntry[] routeEntries =
                Volatile.Read(ref _subscriptionRouteSnapshot);
            using MqttZeroRouteScope deliveries = MqttZeroRouteSelector.Select(
                routeEntries,
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

    private void StoreRetainedMessage(MqttZeroMessageLease lease)
    {
        string topicKey = Encoding.UTF8.GetString(lease.Topic.Span);
        if (lease.PayloadSequence.IsEmpty)
        {
            RemoveRetainedMessage(topicKey);
            return;
        }

        MqttZeroMessageLease retainedLease = lease;
        bool ownsTemporaryLease = false;
        if (lease.IsBorrowed)
        {
            retainedLease = lease.CreateOwnedCopy();
            ownsTemporaryLease = true;
        }

        try
        {
            lock (_sessionGate)
            {
                _retainedMessages.TryGetValue(topicKey, out MqttZeroRetainedMessage? existing);
                if (existing == null && _retainedMessages.Count >= _options.MaxRetainedMessages)
                {
                    throw new InvalidOperationException("The retained message count limit was reached.");
                }

                long previousBytes = existing?.ByteLength ?? 0;
                long replacementBytes =
                    retainedLease.TopicSequence.Length + retainedLease.PayloadSequence.Length;
                long retainedBytes = _retainedBytes - previousBytes + replacementBytes;
                if (retainedBytes > _options.MaxRetainedBytes)
                {
                    throw new InvalidOperationException("The retained message byte limit was reached.");
                }

                MqttZeroRetainedMessage replacement = new(
                    retainedLease,
                    retainedLease.QualityOfServiceLevel,
                    retainedLease.MessageExpiryInterval.HasValue
                        ? _timeProvider.GetUtcNow().AddSeconds(
                            retainedLease.MessageExpiryInterval.Value)
                        : null);
                _retainedMessages[topicKey] = replacement;
                _retainedBytes = retainedBytes;
                existing?.Release();
            }
        }
        finally
        {
            if (ownsTemporaryLease)
            {
                retainedLease.Release();
            }
        }
    }

    private void RemoveRetainedMessage(string topicKey)
    {
        MqttZeroRetainedMessage? removed = null;
        lock (_sessionGate)
        {
            if (_retainedMessages.TryRemove(topicKey, out removed))
            {
                _retainedBytes -= removed.ByteLength;
            }
        }

        removed?.Release();
    }

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

    private async ValueTask DetachConnectionAsync(
        MqttZeroSessionState session,
        MqttZeroClientConnection connection,
        bool cleanDisconnect)
    {
        DateTimeOffset now = _timeProvider.GetUtcNow();
        bool removeSession = false;

        lock (session.SyncRoot)
        {
            if (!ReferenceEquals(session.Connection, connection))
            {
                return;
            }

            session.Connection = null;
            session.DisconnectedAt = now;
            uint expiryInterval = session.ExpiryInterval;
            if (cleanDisconnect || Volatile.Read(ref _stopping) != 0)
            {
                session.WillDueAt = null;
                session.WillMessage = null;
            }
            else if (session.WillMessage != null)
            {
                uint delay = session.WillMessage.DelayInterval;
                if (expiryInterval > 0)
                {
                    delay = Math.Min(delay, expiryInterval);
                }

                session.WillDueAt = now.AddSeconds(delay);
            }

            if (expiryInterval == 0 && session.WillDueAt == null)
            {
                removeSession = true;
            }
        }

        await session.DrainLock.WaitAsync().ConfigureAwait(false);
        session.DrainLock.Release();

        if (removeSession)
        {
            ReleaseSessionMessages(session);
            lock (_sessionGate)
            {
                if (_sessions.TryGetValue(session.ClientId, out MqttZeroSessionState? current) &&
                    ReferenceEquals(current, session))
                {
                    _sessions.TryRemove(session.ClientId, out _);
                    RebuildSubscriptionRouteSnapshot();
                }
            }
        }
    }

    private async Task RunMaintenanceAsync(CancellationToken cancellationToken)
    {
        using PeriodicTimer timer = new(_options.KeepAliveCheckInterval, _timeProvider);
        while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
        {
            DateTimeOffset now = _timeProvider.GetUtcNow();
            List<MqttZeroClientConnection> keepAliveExpired = [];
            List<MqttZeroWillMessage> dueWills = [];
            List<MqttZeroSessionState> expiredSessions = [];
            List<string> expiredRetainedTopics = [];

            foreach (MqttZeroSessionState session in _sessions.Values)
            {
                lock (session.SyncRoot)
                {
                    if (session.Connection != null && session.Connection.KeepAliveSeconds > 0)
                    {
                        double secondsWithoutPacket =
                            (now - session.Connection.LastPacketReceivedAt).TotalSeconds;
                        if (secondsWithoutPacket > session.Connection.KeepAliveSeconds * 1.5)
                        {
                            keepAliveExpired.Add(session.Connection);
                        }
                    }

                    if (session.WillDueAt.HasValue && session.WillDueAt.Value <= now)
                    {
                        if (session.WillMessage != null)
                        {
                            dueWills.Add(session.WillMessage);
                        }

                        session.WillMessage = null;
                        session.WillDueAt = null;
                    }

                    if (IsSessionExpired(session, now) && !session.WillDueAt.HasValue)
                    {
                        expiredSessions.Add(session);
                    }
                }
            }

            foreach (MqttZeroClientConnection connection in keepAliveExpired)
            {
                try
                {
                    await connection.CloseAsync(MqttReasonCode.KeepAliveTimeout, cancellationToken)
                        .ConfigureAwait(false);
                }
                catch (Exception)
                {
                }
            }

            foreach (MqttZeroWillMessage will in dueWills)
            {
                await RoutePublishAsync(null, will.CreatePacket(), cancellationToken).ConfigureAwait(false);
                Interlocked.Increment(ref _publishedWillCount);
            }

            foreach (KeyValuePair<string, MqttZeroRetainedMessage> pair in _retainedMessages)
            {
                if (pair.Value.ExpiresAt.HasValue && pair.Value.ExpiresAt.Value <= now)
                {
                    expiredRetainedTopics.Add(pair.Key);
                }
            }

            foreach (string topic in expiredRetainedTopics)
            {
                RemoveRetainedMessage(topic);
            }

            foreach (MqttZeroSessionState session in expiredSessions)
            {
                ReleaseSessionMessages(session);
                lock (_sessionGate)
                {
                    if (_sessions.TryGetValue(session.ClientId, out MqttZeroSessionState? current) &&
                        ReferenceEquals(current, session))
                    {
                        _sessions.TryRemove(session.ClientId, out _);
                        RebuildSubscriptionRouteSnapshot();
                    }
                }
            }
        }
    }

    private void RebuildSubscriptionRouteSnapshot()
    {
        List<MqttZeroRouteEntry> entries = [];
        lock (_sessionGate)
        {
            foreach (MqttZeroSessionState session in _sessions.Values)
            {
                lock (session.SyncRoot)
                {
                    foreach (MqttZeroSubscription subscription in session.Subscriptions)
                    {
                        entries.Add(new MqttZeroRouteEntry(session, subscription));
                    }
                }
            }
        }

        Volatile.Write(ref _subscriptionRouteSnapshot, entries.ToArray());
    }

    private static bool IsSessionExpired(MqttZeroSessionState session, DateTimeOffset now)
    {
        if (session.Connection != null || !session.DisconnectedAt.HasValue)
        {
            return false;
        }

        return session.ExpiryInterval == 0 ||
            now >= session.DisconnectedAt.Value.AddSeconds(session.ExpiryInterval);
    }

    private int GetInflightLimit(MqttZeroClientConnection? connection)
    {
        if (connection == null)
        {
            return _options.MaxInflight;
        }

        return Math.Min(_options.MaxInflight, connection.ReceiveMaximum);
    }

    /// <summary>
    /// Stops the broker and releases maintenance resources.
    /// </summary>
    /// <returns>A task representing disposal.</returns>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await StopAsync().ConfigureAwait(false);
        _routeScopes.Dispose();
        _bufferPool.Dispose();
    }

    private sealed class ConnectionAttachment
    {
        public ConnectionAttachment(
            MqttZeroSessionState session,
            MqttZeroClientConnection connection)
        {
            Session = session;
            Connection = connection;
        }

        public MqttZeroSessionState Session { get; }

        public MqttZeroClientConnection Connection { get; }
    }

}
