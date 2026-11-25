using System.Text.Json;

namespace MqttZero.BrokerBenchmarks.Tests;

public sealed class BrokerBenchmarkJsonContractTests
{
    [Fact]
    public void ResultJsonSeparatesAcceptanceAndDeliveryMetrics()
    {
        BrokerBenchmarkResult result = new()
        {
            LoadObservation = new BenchmarkLoadObservation(
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0)
        };

        string json = JsonSerializer.Serialize(
            result,
            BrokerBenchmarkJsonContext.Default.BrokerBenchmarkResult);
        using JsonDocument document = JsonDocument.Parse(json);
        JsonElement root = document.RootElement;

        Assert.True(
            root.TryGetProperty(
                "EndToEndThroughputPerSecond",
                out _));
        Assert.True(
            root.TryGetProperty(
                "PublishCallLatencyP99Microseconds",
                out _));
        Assert.True(
            root.TryGetProperty(
                "MeasurementReceivedMessages",
                out _));
        Assert.True(
            root.TryGetProperty(
                "MeasurementDrainMilliseconds",
                out _));
    }
}
