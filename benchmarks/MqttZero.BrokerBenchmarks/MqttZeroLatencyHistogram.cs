using System.Buffers;
using System.Diagnostics;

namespace MqttZero.BrokerBenchmarks;

internal sealed class MqttZeroLatencyHistogram : IDisposable
{
    private const int BucketCount = 1_000_001;

    private readonly ArrayPool<int> _source;
    private int[]? _buckets;
    private long _count;
    private int _disposed;

    public MqttZeroLatencyHistogram()
        : this(ArrayPool<int>.Shared)
    {
    }

    internal MqttZeroLatencyHistogram(ArrayPool<int> source)
    {
        _source = source ??
            throw new ArgumentNullException(nameof(source));
        int[] buckets = source.Rent(BucketCount);
        _buckets = buckets;
        Array.Clear(buckets, 0, BucketCount);
    }

    public int Count
    {
        get
        {
            return (int)Math.Min(
                Interlocked.Read(ref _count),
                int.MaxValue);
        }
    }

    public void RecordTicks(long elapsedTimestampTicks)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(
            elapsedTimestampTicks);
        ObjectDisposedException.ThrowIf(
            Volatile.Read(ref _disposed) != 0,
            this);
        long microseconds = (long)Math.Ceiling(
            elapsedTimestampTicks * 1_000_000d /
            Stopwatch.Frequency);
        int bucketIndex = (int)Math.Min(
            microseconds,
            BucketCount - 1);
        int[] buckets = _buckets ??
            throw new ObjectDisposedException(
                nameof(MqttZeroLatencyHistogram));
        Interlocked.Increment(ref buckets[bucketIndex]);
        Interlocked.Increment(ref _count);
    }

    public double GetPercentileMicroseconds(double percentile)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(percentile, 0);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(percentile, 1);
        ObjectDisposedException.ThrowIf(
            Volatile.Read(ref _disposed) != 0,
            this);
        long count = Interlocked.Read(ref _count);
        if (count == 0)
        {
            return 0;
        }

        long target = Math.Max(
            1,
            (long)Math.Ceiling(percentile * count));
        long cumulative = 0;
        int[] buckets = _buckets ??
            throw new ObjectDisposedException(
                nameof(MqttZeroLatencyHistogram));
        for (int index = 0; index < BucketCount; index++)
        {
            cumulative += Volatile.Read(ref buckets[index]);
            if (cumulative >= target)
            {
                return index;
            }
        }

        return BucketCount - 1;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        int[]? buckets = Interlocked.Exchange(ref _buckets, null);
        if (buckets != null)
        {
            Array.Clear(buckets, 0, BucketCount);
            _source.Return(buckets);
        }
    }
}
