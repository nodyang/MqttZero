namespace MqttZero.BrokerBenchmarks;

internal sealed record BenchmarkRunProgress(
    string Stage,
    long PublishedMessages,
    long ReceivedMessages,
    long FailedOperations);
