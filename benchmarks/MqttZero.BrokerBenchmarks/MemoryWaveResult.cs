namespace MqttZero.BrokerBenchmarks;

internal sealed record MemoryWaveResult(
    int Wave,
    long PublishedMessages,
    long ReceivedMessages,
    long FailedOperations,
    double MeasurementDrainMilliseconds,
    BenchmarkMemorySnapshot Snapshot);
