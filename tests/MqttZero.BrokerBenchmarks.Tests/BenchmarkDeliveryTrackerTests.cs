using MqttZero.BrokerBenchmarks;
using System.Buffers;
using System.Diagnostics;

namespace MqttZero.BrokerBenchmarks.Tests;

public sealed class BenchmarkDeliveryTrackerTests
{
    [Fact]
    public void TracksOrderedDeliveriesWithoutRegressions()
    {
        BenchmarkDeliveryTracker tracker = new();

        Receive(tracker, sequence: 1);
        Receive(tracker, sequence: 2);
        Receive(tracker, sequence: 3);

        Assert.Equal(3, tracker.ReceivedMessages);
        Assert.Equal(3, tracker.MaxSequence);
        Assert.Equal(0, tracker.SequenceRegressions);
        Assert.Equal(0, tracker.UnsequencedMessages);
    }

    [Fact]
    public void DuplicateCannotCancelOutAMissingDelivery()
    {
        BenchmarkDeliveryTracker tracker = new();

        Receive(tracker, sequence: 1);
        Receive(tracker, sequence: 2);
        Receive(tracker, sequence: 3);
        Receive(tracker, sequence: 3);

        Assert.Equal(4, tracker.ReceivedMessages);
        Assert.Equal(3, tracker.MaxSequence);
        Assert.Equal(1, tracker.SequenceRegressions);
        Assert.Equal(
            1,
            tracker.ReceivedMessages - tracker.MaxSequence);
    }

    [Fact]
    public void RecordsDeliveryLatencyWhenHistogramIsAttached()
    {
        using MqttZeroLatencyHistogram histogram = new();
        BenchmarkDeliveryTracker tracker = new();
        tracker.AttachLatency(histogram);

        Receive(
            tracker,
            sequence: 1,
            publishTimestamp:
                Stopwatch.GetTimestamp() -
                (Stopwatch.Frequency / 100));

        double p99 = histogram.GetPercentileMicroseconds(0.99);
        Assert.InRange(p99, 5_000, 1_000_000);
    }

    [Fact]
    public void SkipsLatencyAfterHistogramIsDetached()
    {
        using MqttZeroLatencyHistogram histogram = new();
        BenchmarkDeliveryTracker tracker = new();
        tracker.AttachLatency(histogram);
        tracker.AttachLatency(null);

        Receive(
            tracker,
            sequence: 1,
            publishTimestamp: Stopwatch.GetTimestamp() - 1_000);

        Assert.Equal(0, histogram.Count);
    }

    [Fact]
    public void CountsPayloadsWithoutAnEnvelope()
    {
        BenchmarkDeliveryTracker tracker = new();

        tracker.OnMessageReceived(
            new ReadOnlySequence<byte>(new byte[8]));

        Assert.Equal(1, tracker.ReceivedMessages);
        Assert.Equal(1, tracker.UnsequencedMessages);
        Assert.Equal(0, tracker.MaxSequence);
    }

    private static void Receive(
        BenchmarkDeliveryTracker tracker,
        long sequence,
        long? publishTimestamp = null)
    {
        byte[] payload = new byte[32];
        BenchmarkDeliveryEnvelope.Write(
            payload,
            sequence,
            publishTimestamp ?? Stopwatch.GetTimestamp());
        tracker.OnMessageReceived(
            new ReadOnlySequence<byte>(payload));
    }
}
