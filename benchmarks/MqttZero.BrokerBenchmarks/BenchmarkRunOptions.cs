namespace MqttZero.BrokerBenchmarks;

internal sealed record BenchmarkRunOptions(
    int Subscribers,
    int Publishers,
    int Qos,
    int PayloadSize,
    int WarmupSeconds,
    int MeasurementSeconds,
    int MaxPendingMessages,
    int MessageExpirySeconds = 0,
    int SubscriberDelayMicroseconds = 0);
