namespace MqttZero.BrokerBenchmarks;

/// <summary>
/// Collects broker memory probe lines and tracks per-wave peaks.
/// </summary>
internal sealed class BenchmarkMemoryProbeCollector
{
    private readonly object _gate = new();
    private BrokerMemoryProbeSnapshot? _latest;
    private MemoryWavePeak _peak = Empty;

    public BrokerMemoryProbeSnapshot? Latest
    {
        get
        {
            lock (_gate)
            {
                return _latest;
            }
        }
    }

    public MemoryWavePeak Peak
    {
        get
        {
            lock (_gate)
            {
                return _peak;
            }
        }
    }

    public void Observe(BrokerMemoryProbeSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        lock (_gate)
        {
            _latest = snapshot;
            _peak = new MemoryWavePeak(
                Math.Max(_peak.BufferRentedBytes, snapshot.BufferRentedBytes),
                Math.Max(_peak.BufferCachedBytes, snapshot.BufferCachedBytes),
                Math.Max(
                    _peak.BufferCommittedBytes,
                    snapshot.BufferCommittedBytes),
                Math.Max(
                    _peak.RoutePlanInUseCount,
                    snapshot.RoutePlanInUseCount),
                Math.Max(
                    _peak.RoutePlanRetainedBytes,
                    snapshot.RoutePlanRetainedBytes),
                Math.Max(
                    _peak.SharedCursorCount,
                    snapshot.SharedCursorCount),
                Math.Max(_peak.ManagedHeapBytes, snapshot.ManagedHeapBytes),
                Math.Max(_peak.WorkingSetBytes, snapshot.WorkingSetBytes));
        }
    }

    public void ResetPeak()
    {
        lock (_gate)
        {
            _peak = Empty;
        }
    }

    private static MemoryWavePeak Empty
    {
        get
        {
            return new MemoryWavePeak(0, 0, 0, 0, 0, 0, 0, 0);
        }
    }
}
