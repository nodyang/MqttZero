namespace MqttZero.BrokerBenchmarks.Tests;

public sealed class BenchmarkRepetitionPolicyTests
{
    [Fact]
    public void StableTwoRunCellDoesNotRequireThirdRepetition()
    {
        BrokerComparisonResult[] results =
        [
            Create("mqttzero", repetition: 1, throughput: 100),
            Create("mqttzero", repetition: 2, throughput: 105),
            Create("mqttnet", repetition: 1, throughput: 90),
            Create("mqttnet", repetition: 2, throughput: 95)
        ];

        Assert.False(
            BenchmarkRepetitionPolicy.RequiresThirdRepetition(
                results));
    }

    [Fact]
    public void ThroughputSpreadAboveTenPercentRequiresThirdRepetition()
    {
        BrokerComparisonResult[] results =
        [
            Create("mqttzero", repetition: 1, throughput: 100),
            Create("mqttzero", repetition: 2, throughput: 105),
            Create("mqttnet", repetition: 1, throughput: 90),
            Create("mqttnet", repetition: 2, throughput: 101)
        ];

        Assert.True(
            BenchmarkRepetitionPolicy.RequiresThirdRepetition(
                results));
    }

    [Fact]
    public void IncompleteDeliveryRequiresThirdRepetition()
    {
        BrokerComparisonResult[] results =
        [
            Create(
                "mqttzero",
                repetition: 1,
                throughput: 100,
                receivedMessages: 99),
            Create("mqttzero", repetition: 2, throughput: 101),
            Create("mqttnet", repetition: 1, throughput: 90),
            Create("mqttnet", repetition: 2, throughput: 91)
        ];

        Assert.True(
            BenchmarkRepetitionPolicy.RequiresThirdRepetition(
                results));
    }

    private static BrokerComparisonResult Create(
        string broker,
        int repetition,
        double throughput,
        long receivedMessages = 100)
    {
        return new BrokerComparisonResult
        {
            Broker = broker,
            Qos = 1,
            Subscribers = 100,
            PayloadSize = 64 * 1024,
            Repetition = repetition,
            BrokerObservation = new BenchmarkProcessObservation(
                0,
                0,
                0,
                0),
            Result = new BrokerBenchmarkResult
            {
                PublishedMessages = 1,
                MeasurementReceivedMessages = receivedMessages,
                EndToEndThroughputPerSecond = throughput,
                LoadObservation = new BenchmarkLoadObservation(
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0)
            }
        };
    }
}
