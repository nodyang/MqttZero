namespace MqttZero.BrokerBenchmarks;

internal sealed record BrokerAbVariantInfo(
    string Name,
    string AssemblyPath,
    string? Commit,
    BenchmarkTopologyOptions? Topology = null,
    BenchmarkEgressOptions? Egress = null);
