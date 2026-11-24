using MqttZero.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Publishing;
using MqttZero.Routing;
using MqttZero.Sessions;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Text;

namespace MqttZero.Broker;

/// <summary>
/// Implements broker management and server-originated publishing.
/// </summary>
public sealed partial class MqttZeroBroker
{
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
    /// <param name="message">The message to publish.</param>
    /// <param name="cancellationToken">A token that cancels the operation.</param>
    /// <returns>A task representing the publish operation.</returns>
    public async ValueTask PublishAsync(
        MqttZeroPublishMessage message,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentException.ThrowIfNullOrWhiteSpace(message.Topic);
        if (State != MqttZeroBrokerState.Running)
        {
            throw new InvalidOperationException("The MQTTZero broker is not running.");
        }

        ReadOnlySequence<byte> topic = new(Encoding.UTF8.GetBytes(message.Topic));
        MqttPublishPacket packet = new()
        {
            TopicName = topic,
            Payload = message.Payload,
            QualityOfServiceLevel = message.QualityOfServiceLevel,
            Retain = message.Retain
        };
        MqttZeroPublishView view = new(
            topic,
            ReadOnlySequence<byte>.Empty,
            message.Payload,
            message.QualityOfServiceLevel,
            0,
            false,
            message.Retain,
            null,
            null,
            null);
        if (MqttPublishPacketCodec.GetEncodedLength(view) > _options.MaxPacketSize)
        {
            throw new InvalidOperationException(
                "The outbound MQTT packet exceeds the broker maximum packet size.");
        }

        await RoutePublishAsync(null, packet, cancellationToken).ConfigureAwait(false);
    }

    internal int MaximumPublishPayloadSize
    {
        get
        {
            return _options.MaxPacketSize;
        }
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
}
