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
/// Implements keep-alive, will, and periodic broker maintenance.
/// </summary>
public sealed partial class MqttZeroBroker
{
    private async Task RunMaintenanceAsync(CancellationToken cancellationToken)
    {
        using PeriodicTimer timer = new(_options.KeepAliveCheckInterval, _timeProvider);
        while (await timer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
        {
            DateTimeOffset now = _timeProvider.GetUtcNow();
            _keepAliveExpired.Clear();
            _dueWills.Clear();
            _expiredSessions.Clear();
            _expiredRetainedTopics.Clear();

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
                            _keepAliveExpired.Add(session.Connection);
                        }
                    }

                    if (session.WillDueAt.HasValue && session.WillDueAt.Value <= now)
                    {
                        if (session.WillMessage != null)
                        {
                            _dueWills.Add(session.WillMessage);
                        }

                        session.WillMessage = null;
                        session.WillDueAt = null;
                    }

                    if (IsSessionExpired(session, now) && !session.WillDueAt.HasValue)
                    {
                        _expiredSessions.Add(session);
                    }
                }
            }

            foreach (MqttZeroClientConnection connection in _keepAliveExpired)
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

            foreach (MqttZeroWillMessage will in _dueWills)
            {
                await RoutePublishAsync(null, will.CreatePacket(), cancellationToken).ConfigureAwait(false);
                Interlocked.Increment(ref _publishedWillCount);
            }

            foreach (KeyValuePair<string, MqttZeroRetainedMessage> pair in _retainedMessages)
            {
                if (pair.Value.ExpiresAt.HasValue && pair.Value.ExpiresAt.Value <= now)
                {
                    _expiredRetainedTopics.Add(pair.Key);
                }
            }

            foreach (string topic in _expiredRetainedTopics)
            {
                RemoveRetainedMessage(topic);
                await _persistenceWriter.EnqueueRetainedDeleteAsync(topic, cancellationToken)
                    .ConfigureAwait(false);
            }

            foreach (MqttZeroSessionState session in _expiredSessions)
            {
                ReleaseSessionMessages(session);
                bool removed = false;
                lock (_sessionGate)
                {
                    if (_sessions.TryGetValue(session.ClientId, out MqttZeroSessionState? current) &&
                        ReferenceEquals(current, session))
                    {
                        _sessions.TryRemove(session.ClientId, out _);
                        RebuildSubscriptionRouteSnapshot();
                        removed = true;
                    }
                }

                if (removed)
                {
                    await _persistenceWriter.EnqueueSessionDeleteAsync(
                        session.ClientId,
                        cancellationToken).ConfigureAwait(false);
                }
            }
        }
    }

}
