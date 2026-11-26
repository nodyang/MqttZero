namespace MqttZero.BrokerBenchmarks;

/// <summary>
/// Coordinates out-of-process memory probe requests.
/// </summary>
internal sealed class BrokerMemoryProbeState
{
    private int _gcRequested;

    public void RequestGarbageCollection()
    {
        Interlocked.Exchange(ref _gcRequested, 1);
    }

    public bool TakeGarbageCollectionRequest()
    {
        return Interlocked.Exchange(ref _gcRequested, 0) != 0;
    }
}
