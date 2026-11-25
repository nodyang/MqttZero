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

    [Fact]
    public void OutboundJsonIncludesFinalDrainContract()
    {
        OutboundBenchmarkResult result = new(
            PublishedMessages: 1,
            ReceivedMessages: 1,
            ExpectedReceivedMessages: 1,
            FailedOperations: 0,
            ThroughputPerSecond: 1,
            EndToEndThroughputPerSecond: 1,
            MeasurementDrainMilliseconds: 0,
            WaitP50Microseconds: 0,
            WaitP99Microseconds: 0,
            PublishP50Microseconds: 0,
            PublishP99Microseconds: 0,
            FirstError: null,
            Diagnostics: null!);

        string json = JsonSerializer.Serialize(
            result,
            BrokerBenchmarkJsonContext.Default.OutboundBenchmarkResult);
        using JsonDocument document = JsonDocument.Parse(json);
        JsonElement root = document.RootElement;

        Assert.True(
            root.TryGetProperty(
                "ExpectedReceivedMessages",
                out _));
        Assert.True(
            root.TryGetProperty(
                "EndToEndThroughputPerSecond",
                out _));
        Assert.True(
            root.TryGetProperty(
                "MeasurementDrainMilliseconds",
                out _));
    }
}
