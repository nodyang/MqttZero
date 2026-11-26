using MqttZero.Server.Scheduling;

namespace MqttZero.Server.Broker;

/// <summary>
/// Implements periodic broker maintenance driven by scheduled deadlines.
/// </summary>
public sealed partial class MqttZeroBroker
{
    private async Task RunMaintenanceAsync(
        CancellationToken cancellationToken)
    {
        using PeriodicTimer timer = new(
            _options.KeepAliveCheckInterval,
            _timeProvider);
        while (await timer.WaitForNextTickAsync(cancellationToken)
            .ConfigureAwait(false))
        {
            try
            {
                DateTimeOffset now = _timeProvider.GetUtcNow();
                while (_deadlines.TryTakeDue(
                    now.UtcTicks,
                    out MqttZeroDeadline deadline))
                {
                    try
                    {
                        await ProcessDeadlineAsync(
                            deadline,
                            now,
                            cancellationToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                        when (cancellationToken.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (Exception exception)
                    {
                        MqttZeroBrokerLogging.LogMaintenanceFailure(
                            _logger,
                            exception);
                        ScheduleAfterTick(
                            deadline.Key.Kind,
                            deadline.Key.Id,
                            now);
                    }
                }
            }
            catch (OperationCanceledException)
                when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception exception)
            {
                MqttZeroBrokerLogging.LogMaintenanceFailure(
                    _logger,
                    exception);
            }
        }
    }
}
