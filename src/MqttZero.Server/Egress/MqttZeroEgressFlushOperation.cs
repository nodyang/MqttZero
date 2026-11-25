using System.IO.Pipelines;
using System.Runtime.CompilerServices;

namespace MqttZero.Server.Egress;

internal sealed class MqttZeroEgressFlushOperation
{
    private readonly Action<Exception?> _completed;
    private readonly Action _continuation;
    private ConfiguredValueTaskAwaitable<FlushResult>.
        ConfiguredValueTaskAwaiter _awaiter;
    private int _running;

    public MqttZeroEgressFlushOperation(
        Action<Exception?> completed)
    {
        _completed = completed ??
            throw new ArgumentNullException(nameof(completed));
        _continuation = Complete;
    }

    public void Start(ValueTask<FlushResult> flush)
    {
        if (Interlocked.CompareExchange(ref _running, 1, 0) != 0)
        {
            throw new InvalidOperationException(
                "The flush operation is already running.");
        }

        ConfiguredValueTaskAwaitable<FlushResult>.
            ConfiguredValueTaskAwaiter awaiter = flush
                .ConfigureAwait(false)
                .GetAwaiter();
        _awaiter = awaiter;
        if (awaiter.IsCompleted)
        {
            Complete();
            return;
        }

        try
        {
            awaiter.UnsafeOnCompleted(_continuation);
        }
        catch
        {
            _awaiter = default;
            Volatile.Write(ref _running, 0);
            throw;
        }
    }

    private void Complete()
    {
        Exception? failure = null;
        try
        {
            FlushResult result = _awaiter.GetResult();
            if (result.IsCompleted)
            {
                failure = new IOException(
                    "The MQTT output writer completed during flush.");
            }
        }
        catch (Exception exception)
        {
            failure = exception;
        }
        finally
        {
            _awaiter = default;
            Volatile.Write(ref _running, 0);
        }

        _completed(failure);
    }
}
