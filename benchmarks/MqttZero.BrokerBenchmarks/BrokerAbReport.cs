namespace MqttZero.BrokerBenchmarks;

internal sealed record BrokerAbReport(
    BenchmarkEnvironmentInfo Environment,
    BrokerAbVariantInfo Baseline,
    BrokerAbVariantInfo Candidate,
    int Qos,
    int Subscribers,
    int PayloadSize,
    IReadOnlyList<BrokerAbRoundResult> Rounds,
    BrokerAbSummary Summary);
