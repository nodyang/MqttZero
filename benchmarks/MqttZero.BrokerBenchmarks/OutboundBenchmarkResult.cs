namespace MqttZero.BrokerBenchmarks;

internal sealed record OutboundBenchmarkResult(
    long PublishedMessages,
    long ReceivedMessages,
    long ExpectedReceivedMessages,
    long FailedOperations,
    double ThroughputPerSecond,
    double EndToEndThroughputPerSecond,
    double MeasurementDrainMilliseconds,
    double WaitP50Microseconds,
    double WaitP99Microseconds,
    double PublishP50Microseconds,
    double PublishP99Microseconds,
    string? FirstError,
    BenchmarkFanoutDiagnostics Diagnostics);
