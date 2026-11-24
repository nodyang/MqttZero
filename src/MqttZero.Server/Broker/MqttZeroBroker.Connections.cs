using MqttZero.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Routing;
using MqttZero.Sessions;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Net;
using System.Text;
using System.Security.Cryptography.X509Certificates;
using MqttZero.Events;
using MqttZero.Observers;

namespace MqttZero.Broker;

/// <summary>
/// Implements connection acceptance, CONNECT processing, and takeover.
/// </summary>
public sealed partial class MqttZeroBroker
{
    /// <summary>
    /// Processes one accepted Kestrel connection until it disconnects.
    /// </summary>
    /// <param name="input">The inbound byte pipe.</param>
    /// <param name="output">The outbound byte pipe.</param>
    /// <param name="remoteEndPoint">The optional remote network endpoint.</param>
    /// <param name="isSecure">Whether the connection uses TLS.</param>
    /// <param name="isWebSocket">Whether the connection uses WebSocket.</param>
    /// <param name="cancellationToken">A token that terminates the connection.</param>
    /// <returns>A task that completes when the connection closes.</returns>
    public async ValueTask AcceptConnectionAsync(
        PipeReader input,
        PipeWriter output,
        EndPoint? remoteEndPoint,
        bool isSecure,
        bool isWebSocket,
        CancellationToken cancellationToken)
    {
        await AcceptConnectionAsync(
            input,
            output,
            remoteEndPoint,
            isSecure,
            isWebSocket,
            true,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Processes one accepted Kestrel connection until it disconnects.
    /// </summary>
    /// <param name="input">The inbound byte pipe.</param>
    /// <param name="output">The outbound byte pipe.</param>
    /// <param name="remoteEndPoint">The optional remote network endpoint.</param>
    /// <param name="isSecure">Whether the connection uses TLS.</param>
    /// <param name="isWebSocket">Whether the connection uses WebSocket.</param>
    /// <param name="allowPacketFragmentation">Whether outbound PUBLISH packets may be split across writes.</param>
    /// <param name="cancellationToken">A token that terminates the connection.</param>
    /// <returns>A task that completes when the connection closes.</returns>
    public async ValueTask AcceptConnectionAsync(
        PipeReader input,
        PipeWriter output,
        EndPoint? remoteEndPoint,
        bool isSecure,
        bool isWebSocket,
        bool allowPacketFragmentation,
        CancellationToken cancellationToken)
    {
        await AcceptConnectionAsync(
            input,
            output,
            remoteEndPoint,
            isSecure,
            isWebSocket,
            null,
            allowPacketFragmentation,
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Processes one accepted connection with client certificate data.</summary>
    public async ValueTask AcceptConnectionAsync(
        PipeReader input,
        PipeWriter output,
        EndPoint? remoteEndPoint,
        bool isSecure,
        bool isWebSocket,
        X509Certificate2? clientCertificate,
        bool allowPacketFragmentation,
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
                            isWebSocket,
                            clientCertificate,
                            allowPacketFragmentation).ConfigureAwait(false);
                        if (attachment == null)
                        {
                            return;
                        }
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
                        connection.DisconnectType = MqttZeroDisconnectType.ClientRequest;
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
                MqttZeroClientDisconnectedContext disconnectedContext = new()
                {
                    Client = connection.ClientInfo,
                    DisconnectType = connection.DisconnectType,
                    ReasonCode = connection.CloseReasonCode
                };
                foreach (IMqttZeroConnectionObserver observer in _connectionObservers)
                {
                    try
                    {
                        await observer.OnDisconnectedAsync(
                            disconnectedContext,
                            CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (Exception)
                    {
                    }
                }

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
        EndPoint? remoteEndPoint,
        bool isSecure,
        bool isWebSocket,
        X509Certificate2? clientCertificate,
        bool allowPacketFragmentation)
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
            _bufferPool,
            allowPacketFragmentation);
        MqttZeroClientInfo clientInfo = new()
        {
            ClientId = clientId,
            UserName = connectPacket.HasUserName
                ? Encoding.UTF8.GetString(connectPacket.UserName.ToArray())
                : null,
            RemoteEndPoint = remoteEndPoint,
            IsSecure = isSecure,
            IsWebSocket = isWebSocket,
            ProtocolVersion = connectPacket.ProtocolVersion,
            ClientCertificate = clientCertificate
        };
        newConnection.ClientInfo = clientInfo;
        MqttZeroConnectionContext connectionContext = new()
        {
            Client = clientInfo,
            ConnectPacket = connectPacket
        };
        foreach (IMqttZeroConnectionInterceptor interceptor in _connectionInterceptors)
        {
            MqttZeroConnectionInterceptionResult result;
            try
            {
                result = await interceptor.InterceptAsync(connectionContext, CancellationToken.None)
                    .ConfigureAwait(false);
            }
            catch (Exception)
            {
                result = MqttZeroConnectionInterceptionResult.Reject;
                connectionContext.RejectReasonCode = MqttReasonCode.ImplementationSpecificError;
            }

            if (result == MqttZeroConnectionInterceptionResult.Drop)
            {
                await newConnection.DisposeAsync().ConfigureAwait(false);
                return null!;
            }

            if (result == MqttZeroConnectionInterceptionResult.Reject)
            {
                await newConnection.SendConnAckAsync(
                    new MqttConnAckPacket
                    {
                        ReasonCode = connectionContext.RejectReasonCode,
                        ReasonString = connectionContext.ReasonString
                    },
                    CancellationToken.None).ConfigureAwait(false);
                await newConnection.DisposeAsync().ConfigureAwait(false);
                return null!;
            }
        }
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

                session = new MqttZeroSessionState(clientId, _options.MaxMessageQueueLength);
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

        await newConnection.SendConnAckAsync(
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
        MqttZeroClientConnectedContext connectedContext = new()
        {
            Client = clientInfo,
            SessionPresent = sessionPresent
        };
        foreach (IMqttZeroConnectionObserver observer in _connectionObservers)
        {
            try
            {
                await observer.OnConnectedAsync(connectedContext, CancellationToken.None)
                    .ConfigureAwait(false);
            }
            catch (Exception)
            {
            }
        }
        if (takeoverWill != null)
        {
            await RoutePublishAsync(null, takeoverWill.CreatePacket(), CancellationToken.None)
                .ConfigureAwait(false);
            Interlocked.Increment(ref _publishedWillCount);
        }

        return new ConnectionAttachment(session, newConnection);
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
