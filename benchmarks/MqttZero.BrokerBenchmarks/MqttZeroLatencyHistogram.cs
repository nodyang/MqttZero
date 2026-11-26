using System.Buffers;
using System.Diagnostics;

namespace MqttZero.BrokerBenchmarks;

internal sealed class MqttZeroLatencyHistogram : IDisposable
{
    private const int LinearBucketCount = 1_000_001;
    private const int LinearMaximumMicroseconds = LinearBucketCount - 1;
    private const int TailMaximumMicroseconds = 60_000_000;
    private const double TailGrowth = 1.02;

    private static readonly int[] TailBounds = CreateTailBounds();
    private static readonly int BucketCount =
        LinearBucketCount + TailBounds.Length;

    private readonly ArrayPool<int> _source;
    private int[]? _buckets;
    private long _count;
    private long _maximumMicroseconds;
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

    public long MaxMicroseconds
    {
        get
        {
            return Interlocked.Read(ref _maximumMicroseconds);
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
        int bucketIndex = GetBucketIndex(microseconds);
        int[] buckets = _buckets ??
            throw new ObjectDisposedException(
                nameof(MqttZeroLatencyHistogram));
        Interlocked.Increment(ref buckets[bucketIndex]);
        Interlocked.Increment(ref _count);
        UpdateMaximum(microseconds);
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
                return GetBucketMicroseconds(index);
            }
        }

        return TailMaximumMicroseconds;
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

    private static int[] CreateTailBounds()
    {
        List<int> bounds = [];
        double value = LinearMaximumMicroseconds;
        while (value < TailMaximumMicroseconds)
        {
            value *= TailGrowth;
            bounds.Add((int)Math.Min(
                Math.Ceiling(value),
                TailMaximumMicroseconds));
            if (bounds[^1] == TailMaximumMicroseconds)
            {
                break;
            }
        }

        return [.. bounds];
    }

    private static int GetBucketIndex(long microseconds)
    {
        if (microseconds <= LinearMaximumMicroseconds)
        {
            return (int)microseconds;
        }

        int clamped = (int)Math.Min(
            microseconds,
            TailMaximumMicroseconds);
        int tailIndex = Array.BinarySearch(TailBounds, clamped);
        if (tailIndex < 0)
        {
            tailIndex = ~tailIndex;
        }

        if (tailIndex >= TailBounds.Length)
        {
            tailIndex = TailBounds.Length - 1;
        }

        return LinearBucketCount + tailIndex;
    }

    private static long GetBucketMicroseconds(int bucketIndex)
    {
        if (bucketIndex < LinearBucketCount)
        {
            return bucketIndex;
        }

        int tailIndex = Math.Min(
            bucketIndex - LinearBucketCount,
            TailBounds.Length - 1);
        return TailBounds[tailIndex];
    }

    private void UpdateMaximum(long microseconds)
    {
        long current = Volatile.Read(ref _maximumMicroseconds);
        while (microseconds > current)
        {
            long observed = Interlocked.CompareExchange(
                ref _maximumMicroseconds,
                microseconds,
                current);
            if (observed == current)
            {
                return;
            }

            current = observed;
        }
    }
}
