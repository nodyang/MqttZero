namespace MqttZero.BrokerBenchmarks;

internal sealed record MemoryBenchmarkResult(
    MemoryWaveResult[] Waves,
    MemoryRunInfo? Run = null);
