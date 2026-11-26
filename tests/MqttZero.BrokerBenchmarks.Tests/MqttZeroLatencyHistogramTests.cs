using System.Diagnostics;
using System.Buffers;

namespace MqttZero.BrokerBenchmarks.Tests;

public sealed class MqttZeroLatencyHistogramTests
{
    [Fact]
    public void PercentilesIncludeSamplesAfterFirstHundredThousand()
    {
        using MqttZeroLatencyHistogram histogram = new();
        long tenMicroseconds = TicksForMicroseconds(10);
        long oneMillisecond = TicksForMicroseconds(1_000);

        for (int index = 0; index < 145_000; index++)
        {
            histogram.RecordTicks(tenMicroseconds);
        }

        for (int index = 0; index < 5_000; index++)
        {
            histogram.RecordTicks(oneMillisecond);
        }

        Assert.Equal(150_000, histogram.Count);
        Assert.Equal(10, histogram.GetPercentileMicroseconds(0.95));
        Assert.Equal(1_000, histogram.GetPercentileMicroseconds(0.99));
    }

    [Fact]
    public void ClearsBucketsRentedFromSharedPool()
    {
        DirtyArrayPool source = new();
        using MqttZeroLatencyHistogram histogram = new(source);

        histogram.RecordTicks(TicksForMicroseconds(10));

        Assert.Equal(10, histogram.GetPercentileMicroseconds(0.99));
    }

    [Fact]
    public void KeepsExactMicrosecondsUpToOneSecond()
    {
        using MqttZeroLatencyHistogram histogram = new();

        histogram.RecordTicks(TicksForMicroseconds(999_999));

        Assert.Equal(
            999_999,
            histogram.GetPercentileMicroseconds(0.99));
    }

    [Fact]
    public void RecordsTailValuesWithoutClippingAtOneSecond()
    {
        using MqttZeroLatencyHistogram histogram = new();

        histogram.RecordTicks(TicksForMicroseconds(5_000_000));

        double p99 = histogram.GetPercentileMicroseconds(0.99);
        Assert.InRange(p99, 5_000_000, 5_100_000);
        Assert.Equal(5_000_000, histogram.MaxMicroseconds);
    }

    [Fact]
    public void TailBucketsStayWithinTwoPercent()
    {
        using MqttZeroLatencyHistogram histogram = new();
        int[] samples = [1_500_000, 7_000_000, 45_000_000];

        foreach (int sample in samples)
        {
            histogram.RecordTicks(TicksForMicroseconds(sample));
            double p99 = histogram.GetPercentileMicroseconds(0.99);
            Assert.InRange(
                p99,
                sample,
                Math.Ceiling(sample * 1.02));
        }
    }

    [Fact]
    public void ClampsValuesAboveSixtySeconds()
    {
        using MqttZeroLatencyHistogram histogram = new();

        histogram.RecordTicks(TicksForMicroseconds(120_000_000));

        Assert.Equal(
            60_000_000,
            histogram.GetPercentileMicroseconds(0.99));
        Assert.Equal(120_000_000, histogram.MaxMicroseconds);
    }

    private static long TicksForMicroseconds(int microseconds)
    {
        return checked(
            (microseconds * Stopwatch.Frequency + 999_999) /
            1_000_000);
    }

    private sealed class DirtyArrayPool : ArrayPool<int>
    {
        public override int[] Rent(int minimumLength)
        {
            int[] buffer = new int[minimumLength];
            Array.Fill(buffer, int.MaxValue);
            return buffer;
        }

        public override void Return(
            int[] array,
            bool clearArray = false)
        {
        }
    }
}
