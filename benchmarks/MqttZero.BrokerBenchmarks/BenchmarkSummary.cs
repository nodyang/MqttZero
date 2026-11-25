namespace MqttZero.BrokerBenchmarks;

internal sealed record BenchmarkSummary(
    int Qos,
    int Subscribers,
    int PayloadSize,
    double MqttZeroEndToEndThroughput,
    double MqttNetEndToEndThroughput,
    double ThroughputRatio,
    double MqttZeroPublishCallP99Microseconds,
    double MqttNetPublishCallP99Microseconds,
    double P99Ratio,
    long MqttZeroFailures,
    long MqttNetFailures,
    bool Passed,
    string? FailureReason);
