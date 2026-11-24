using System.Threading.Tasks.Sources;

namespace MqttZero.Server.Sessions;

internal sealed class MqttZeroAsyncSignal : IValueTaskSource
{
    private readonly object _gate = new();
    private ManualResetValueTaskSourceCore<bool> _source;
    private bool _signaled;
    private bool _waiting;

    public MqttZeroAsyncSignal()
    {
        _source.RunContinuationsAsynchronously = true;
    }

    public void Signal()
    {
        lock (_gate)
        {
            if (_signaled)
            {
                return;
            }

            _signaled = true;
            _source.SetResult(true);
        }
    }

    public ValueTask WaitAsync()
    {
        lock (_gate)
        {
            if (_signaled)
            {
                _signaled = false;
                _source.Reset();
                return ValueTask.CompletedTask;
            }

            if (_waiting)
            {
                throw new InvalidOperationException(
                    "The MQTTZero asynchronous signal already has an active waiter.");
            }

            _waiting = true;
            return new ValueTask(this, _source.Version);
        }
    }

    public ValueTaskSourceStatus GetStatus(short token)
    {
        lock (_gate)
        {
            return _source.GetStatus(token);
        }
    }

    public void OnCompleted(
        Action<object?> continuation,
        object? state,
        short token,
        ValueTaskSourceOnCompletedFlags flags)
    {
        lock (_gate)
        {
            _source.OnCompleted(continuation, state, token, flags);
        }
    }

    public void GetResult(short token)
    {
        lock (_gate)
        {
            _source.GetResult(token);
            _source.Reset();
            _signaled = false;
            _waiting = false;
        }
    }
}
