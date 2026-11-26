namespace MqttZero.BrokerBenchmarks;

/// <summary>
/// Peak broker-owned counters observed during one memory wave.
/// </summary>
internal sealed record MemoryWavePeak(
    long BufferRentedBytes,
    long BufferCachedBytes,
    long BufferCommittedBytes,
    int RoutePlanInUseCount,
    long RoutePlanRetainedBytes,
    int SharedCursorCount,
    long ManagedHeapBytes,
    long WorkingSetBytes);
