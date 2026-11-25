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
