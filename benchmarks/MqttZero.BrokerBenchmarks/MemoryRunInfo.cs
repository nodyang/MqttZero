namespace MqttZero.BrokerBenchmarks;

/// <summary>
/// Describes how a memory stability run was executed.
/// </summary>
internal sealed record MemoryRunInfo(
    string Mode,
    string? BrokerAssembly,
    string? BrokerCommit,
    long BufferQuotaBytes,
    int ProbeMilliseconds,
    int Qos,
    int Subscribers,
    int PayloadSize,
    int MessagesPerWave,
    long PeakBufferCommittedBytes,
    long PeakManagedHeapBytes,
    long PeakWorkingSetBytes,
    BenchmarkMemorySnapshot? IdleSnapshot = null);
