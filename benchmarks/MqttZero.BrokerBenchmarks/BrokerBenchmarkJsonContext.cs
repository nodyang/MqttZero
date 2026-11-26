using System.Text.Json.Serialization;

namespace MqttZero.BrokerBenchmarks;

[JsonSourceGenerationOptions(WriteIndented = true)]
[JsonSerializable(typeof(BenchmarkReport))]
[JsonSerializable(typeof(BrokerAbReport))]
[JsonSerializable(typeof(BrokerBenchmarkResult))]
[JsonSerializable(typeof(MemoryBenchmarkResult))]
[JsonSerializable(typeof(
    MqttZero.Server.Broker.MqttZeroObjectPoolSnapshot))]
[JsonSerializable(typeof(OutboundBenchmarkResult))]
internal sealed partial class BrokerBenchmarkJsonContext :
    JsonSerializerContext
{
}
