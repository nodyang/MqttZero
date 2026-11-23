using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Text;

namespace MqttZero;

/// <summary>
/// Provides the in-memory MQTT 5 broker pipeline.
/// </summary>
public sealed class MqttZeroBroker
{
    private readonly MqttZeroOptions _options;
    private readonly IMqttZeroPublishInterceptor[] _publishInterceptors;
    private readonly ConcurrentDictionary<string, MqttZeroClientSession> _sessions = new(StringComparer.Ordinal);
    private readonly TimeProvider _timeProvider;
    private int _connectionCount;
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
        _publishInterceptors = publishInterceptors.ToArray();
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
    /// Gets a point-in-time snapshot of connected clients.
    /// </summary>
    /// <returns>The connected client snapshots.</returns>
    public IReadOnlyList<MqttZeroClientSnapshot> GetClients()
    {
        List<MqttZeroClientSnapshot> clients = [];
        foreach (MqttZeroClientSession session in _sessions.Values)
        {
            int subscriptionCount;
            lock (session.SubscriptionLock)
            {
                subscriptionCount = session.Subscriptions.Count;
            }

            clients.Add(new MqttZeroClientSnapshot
            {
                ClientId = session.ClientId,
                RemoteEndPoint = session.RemoteEndPoint,
                IsSecure = session.IsSecure,
                IsWebSocket = session.IsWebSocket,
                SubscriptionCount = subscriptionCount
            });
        }

        return clients;
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
    /// Starts the broker.
    /// </summary>
    public ValueTask StartAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Volatile.Write(ref _state, (int)MqttZeroBrokerState.Running);
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Stops the broker and closes active sessions.
    /// </summary>
    public async ValueTask StopAsync(CancellationToken cancellationToken = default)
    {
        Volatile.Write(ref _state, (int)MqttZeroBrokerState.Stopping);
        foreach (KeyValuePair<string, MqttZeroClientSession> pair in _sessions)
        {
            if (_sessions.TryRemove(pair.Key, out MqttZeroClientSession? session))
            {
                try
                {
                    await session.SendAsync(
                        new MqttDisconnectPacket
                        {
                            ReasonCode = MqttReasonCode.ServerShuttingDown
                        },
                        cancellationToken).ConfigureAwait(false);
                }
                catch (Exception) when (cancellationToken.IsCancellationRequested)
                {
                }
                finally
                {
                    await session.DisposeAsync().ConfigureAwait(false);
                }
            }
        }

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

        MqttZeroClientSession? session = null;
        bool cleanDisconnect = false;
        try
        {
            using CancellationTokenSource connectTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            connectTimeout.CancelAfter(_options.ConnectTimeout);

            while (!cancellationToken.IsCancellationRequested)
            {
                ReadResult readResult = session == null
                    ? await input.ReadAsync(connectTimeout.Token).ConfigureAwait(false)
                    : await input.ReadAsync(cancellationToken).ConfigureAwait(false);
                ReadOnlySequence<byte> buffer = readResult.Buffer;
                SequencePosition examined = readResult.Buffer.End;

                while (MqttPacketCodec.TryDecode(buffer, out MqttPacket? packet, out long consumed) &&
                    packet != null)
                {
                    if (session == null)
                    {
                        if (packet is not MqttConnectPacket connectPacket)
                        {
                            return;
                        }

                        session = await ConnectAsync(connectPacket, output, remoteEndPoint, isSecure, isWebSocket).ConfigureAwait(false);
                        buffer = buffer.Slice(consumed);
                        continue;
                    }

                    await ProcessPacketAsync(session, packet, cancellationToken).ConfigureAwait(false);
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
        finally
        {
            if (session != null)
            {
                _sessions.TryRemove(session.ClientId, out _);
                await session.DisposeAsync().ConfigureAwait(false);
            }

            Interlocked.Decrement(ref _connectionCount);
            await input.CompleteAsync().ConfigureAwait(false);

            if (!cleanDisconnect && session != null)
            {
                await PublishWillAsync().ConfigureAwait(false);
            }
        }
    }

    private async ValueTask<MqttZeroClientSession> ConnectAsync(
        MqttConnectPacket connectPacket,
        PipeWriter output,
        string? remoteEndPoint,
        bool isSecure,
        bool isWebSocket)
    {
        string clientId;
        if (connectPacket.ClientId.IsEmpty)
        {
            clientId = $"mqttzero-{Guid.NewGuid():N}";
        }
        else
        {
            clientId = Encoding.UTF8.GetString(connectPacket.ClientId.ToArray());
        }

        MqttZeroClientSession session = new(clientId, output, remoteEndPoint, isSecure, isWebSocket);
        if (_sessions.Count >= _options.MaxSessions)
        {
            await session.SendAsync(
                new MqttConnAckPacket
                {
                    ReasonCode = MqttReasonCode.QuotaExceeded
                },
                CancellationToken.None).ConfigureAwait(false);
            await session.DisposeAsync().ConfigureAwait(false);
            throw new InvalidOperationException("The maximum number of MQTT sessions was reached.");
        }

        _sessions.AddOrUpdate(clientId, session, static (_, previous) => previous);
        await session.SendAsync(
            new MqttConnAckPacket
            {
                ReasonCode = MqttReasonCode.Success
            },
            CancellationToken.None).ConfigureAwait(false);
        return session;
    }

    private async ValueTask ProcessPacketAsync(
        MqttZeroClientSession session,
        MqttPacket packet,
        CancellationToken cancellationToken)
    {
        switch (packet)
        {
            case MqttSubscribePacket subscribePacket:
                await ProcessSubscribeAsync(session, subscribePacket, cancellationToken).ConfigureAwait(false);
                break;
            case MqttPublishPacket publishPacket:
                await RoutePublishAsync(session, publishPacket, cancellationToken).ConfigureAwait(false);
                break;
            case MqttPingReqPacket:
                await session.SendAsync(new MqttPingRespPacket(), cancellationToken).ConfigureAwait(false);
                break;
            case MqttPubAckPacket:
                break;
            case MqttDisconnectPacket:
                break;
        }
    }

    private async ValueTask ProcessSubscribeAsync(
        MqttZeroClientSession session,
        MqttSubscribePacket packet,
        CancellationToken cancellationToken)
    {
        byte[] reasonCodes = new byte[packet.Subscriptions.Count];
        for (int index = 0; index < packet.Subscriptions.Count; index++)
        {
            MqttSubscriptionRequest request = packet.Subscriptions[index];
            if (request.QualityOfServiceLevel == MqttQualityOfServiceLevel.ExactlyOnce)
            {
                reasonCodes[index] = (byte)MqttReasonCode.QosNotSupported;
                continue;
            }

            lock (session.SubscriptionLock)
            {
                if (session.Subscriptions.Count >= _options.MaxSubscriptionsPerSession)
                {
                    reasonCodes[index] = (byte)MqttReasonCode.QuotaExceeded;
                    continue;
                }

                byte[] topicFilter = request.TopicFilter.ToArray();
                session.Subscriptions.Add(new MqttZeroSubscription(
                    topicFilter,
                    request.QualityOfServiceLevel,
                    request.NoLocal,
                    request.RetainAsPublished));
            }

            reasonCodes[index] = request.QualityOfServiceLevel == MqttQualityOfServiceLevel.AtLeastOnce
                ? (byte)MqttReasonCode.GrantedQos1
                : (byte)MqttReasonCode.Success;
        }

        await session.SendAsync(
            new MqttSubAckPacket
            {
                PacketIdentifier = packet.PacketIdentifier,
                ReasonCodes = reasonCodes
            },
            cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask RoutePublishAsync(
        MqttZeroClientSession? sender,
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

        byte[] topic = packet.TopicName.ToArray();
        foreach (KeyValuePair<string, MqttZeroClientSession> pair in _sessions)
        {
            MqttZeroClientSession receiver = pair.Value;
            MqttZeroSubscription? matchedSubscription = null;
            lock (receiver.SubscriptionLock)
            {
                foreach (MqttZeroSubscription subscription in receiver.Subscriptions)
                {
                    if (subscription.NoLocal && ReferenceEquals(receiver, sender))
                    {
                        continue;
                    }

                    if (MqttZeroTopicMatcher.Matches(subscription.TopicFilter, topic))
                    {
                        matchedSubscription = subscription;
                        break;
                    }
                }
            }

            if (matchedSubscription == null)
            {
                continue;
            }

            MqttQualityOfServiceLevel qos = (MqttQualityOfServiceLevel)Math.Min(
                (byte)packet.QualityOfServiceLevel,
                (byte)matchedSubscription.QualityOfServiceLevel);
            MqttPublishPacket outgoing = new()
            {
                TopicName = packet.TopicName,
                Properties = packet.Properties,
                Payload = packet.Payload,
                QualityOfServiceLevel = qos,
                PacketIdentifier = qos == MqttQualityOfServiceLevel.AtLeastOnce
                    ? receiver.GetNextPacketIdentifier()
                    : (ushort)0,
                Retain = matchedSubscription.RetainAsPublished && packet.Retain
            };

            await receiver.SendAsync(outgoing, cancellationToken).ConfigureAwait(false);
        }

        if (sender != null &&
            packet.QualityOfServiceLevel == MqttQualityOfServiceLevel.AtLeastOnce)
        {
            await sender.SendAsync(
                new MqttPubAckPacket
                {
                    PacketIdentifier = packet.PacketIdentifier,
                    ReasonCode = MqttReasonCode.Success
                },
                cancellationToken).ConfigureAwait(false);
        }
    }

    private static ValueTask PublishWillAsync()
    {
        return ValueTask.CompletedTask;
    }
}
