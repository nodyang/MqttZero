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
/// Implements broker startup and shutdown.
/// </summary>
public sealed partial class MqttZeroBroker
{
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
}
