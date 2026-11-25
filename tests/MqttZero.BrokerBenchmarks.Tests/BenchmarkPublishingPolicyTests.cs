namespace MqttZero.BrokerBenchmarks.Tests;

public sealed class BenchmarkPublishingPolicyTests
{
    [Fact]
    public void PausesWhenFanoutBacklogExceedsPendingBudget()
    {
        Assert.False(
            BenchmarkLoadRunner.ShouldPause(
                publishedMessages: 10,
                receivedMessages: 900,
                subscribers: 100,
                maxPendingMessages: 100));
        Assert.True(
            BenchmarkLoadRunner.ShouldPause(
                publishedMessages: 11,
                receivedMessages: 999,
                subscribers: 100,
                maxPendingMessages: 100));
    }
}
