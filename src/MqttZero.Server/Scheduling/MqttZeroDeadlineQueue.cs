namespace MqttZero.Server.Scheduling;

/// <summary>
/// Orders broker deadlines in a min-heap and drops stale entries lazily.
/// </summary>
internal sealed class MqttZeroDeadlineQueue
{
    private readonly object _gate = new();
    private readonly PriorityQueue<MqttZeroDeadline, long> _heap = new();
    private readonly Dictionary<MqttZeroDeadlineKey, long> _scheduled = [];

    public int Count
    {
        get
        {
            lock (_gate)
            {
                return _scheduled.Count;
            }
        }
    }

    public void Schedule(MqttZeroDeadlineKey key, long dueTicks)
    {
        lock (_gate)
        {
            _scheduled[key] = dueTicks;
            _heap.Enqueue(
                new MqttZeroDeadline(key, dueTicks),
                dueTicks);
        }
    }

    public void Remove(MqttZeroDeadlineKey key)
    {
        lock (_gate)
        {
            _scheduled.Remove(key);
        }
    }

    public bool TryTakeDue(long nowTicks, out MqttZeroDeadline deadline)
    {
        lock (_gate)
        {
            while (_heap.TryPeek(
                out MqttZeroDeadline candidate,
                out long dueTicks) &&
                dueTicks <= nowTicks)
            {
                _heap.Dequeue();
                if (_scheduled.TryGetValue(
                    candidate.Key,
                    out long current) &&
                    current == candidate.DueTicks)
                {
                    _scheduled.Remove(candidate.Key);
                    deadline = candidate;
                    return true;
                }
            }

            deadline = default;
            return false;
        }
    }

    public void Clear()
    {
        lock (_gate)
        {
            _heap.Clear();
            _scheduled.Clear();
        }
    }
}
