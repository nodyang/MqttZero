namespace MqttZero.BrokerBenchmarks;

internal sealed class BrokerBenchmarkResult
{
    public long PublishedMessages { get; init; }

    public long ReceivedMessages { get; init; }

    public long FailedOperations { get; init; }

    public long WarmupPublishedMessages { get; init; }

    public long WarmupReceivedMessages { get; init; }

    public long MeasurementReceivedMessages { get; init; }

    public double PublishWindowThroughputPerSecond { get; init; }

    public double EndToEndThroughputPerSecond { get; init; }

    public double PublishCallLatencyP50Microseconds { get; init; }

    public double PublishCallLatencyP95Microseconds { get; init; }

    public double PublishCallLatencyP99Microseconds { get; init; }

    public required BenchmarkLoadObservation LoadObservation { get; init; }

    public double ConnectionSetupMilliseconds { get; init; }

    public double WarmupMilliseconds { get; init; }

    public double MeasurementMilliseconds { get; init; }

    public double WarmupDrainMilliseconds { get; init; }

    public double MeasurementDrainMilliseconds { get; init; }

    public string? FirstError { get; init; }
}
