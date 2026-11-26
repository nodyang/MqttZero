using System.Text.Json.Serialization;

namespace MqttZero.BrokerBenchmarks;

[JsonSourceGenerationOptions(WriteIndented = false)]
[JsonSerializable(typeof(BrokerMemoryProbeSnapshot))]
internal sealed partial class BrokerMemoryProbeJsonContext :
    JsonSerializerContext
{
}
