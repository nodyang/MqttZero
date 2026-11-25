using MqttZero.Protocol;
using System.Buffers;
using System.Collections.Concurrent;

namespace MqttZero.Memory;

internal sealed class MqttZeroBufferPool : IDisposable
{
    private const int BucketCount = 23;
    private const int MinimumBucketSize = 256;
    private const int MaximumCachedBuffersPerBucket = 1024;

    private static readonly MqttZeroBufferPool SharedPool = new(512L * 1024 * 1024);
    private static readonly int[] BucketSizes = CreateBucketSizes();

    private readonly ConcurrentQueue<byte[]>[] _buckets;
    private readonly long[] _cachedBucketBytes;
    private readonly int[] _cachedCounts;
    private readonly SemaphoreSlim _capacityReleased = new(0, int.MaxValue);
    private readonly long _maximumBytes;
    private readonly ArrayPool<byte> _source;
    private int _capacityWaiters;
    private long _cachedBytes;
    private long _committedBytes;
    private long _rentedBytes;
    private long _returnGeneration;
    private int _disposed;

    public MqttZeroBufferPool(long maximumBytes)
        : this(maximumBytes, MqttZeroNoCacheArrayPool.Instance)
    {
    }

    public MqttZeroBufferPool(long maximumBytes, ArrayPool<byte> source)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximumBytes);
        _maximumBytes = maximumBytes;
        _source = source ?? throw new ArgumentNullException(nameof(source));
        _buckets = new ConcurrentQueue<byte[]>[BucketCount];
        _cachedBucketBytes = new long[BucketCount];
        _cachedCounts = new int[BucketCount];
        for (int index = 0; index < _buckets.Length; index++)
        {
            _buckets[index] = new ConcurrentQueue<byte[]>();
        }
    }

    public static MqttZeroBufferPool Shared
    {
        get
        {
            return SharedPool;
        }
    }

    public long RentedBytes
    {
        get
        {
            return Volatile.Read(ref _rentedBytes);
        }
    }

    public byte[] Rent(int minimumLength)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(minimumLength);
        if (TryRent(minimumLength, out byte[]? buffer))
        {
            return buffer;
        }

        throw new MqttZeroBufferPoolExhaustedException();
    }

    public MqttZeroBufferPoolSnapshot GetSnapshot()
    {
        MqttZeroBufferBucketSnapshot[] buckets =
            new MqttZeroBufferBucketSnapshot[BucketCount];
        for (int index = 0; index < buckets.Length; index++)
        {
            buckets[index] = new MqttZeroBufferBucketSnapshot(
                BucketSizes[index],
                Volatile.Read(ref _cachedCounts[index]),
                Volatile.Read(ref _cachedBucketBytes[index]));
        }

        long rentedBytes = Volatile.Read(ref _rentedBytes);
        long cachedBytes = Volatile.Read(ref _cachedBytes);
        return new MqttZeroBufferPoolSnapshot(
            rentedBytes,
            cachedBytes,
            Volatile.Read(ref _committedBytes),
            buckets);
    }

    public bool TryRent(int minimumLength, out byte[] buffer)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        ArgumentOutOfRangeException.ThrowIfNegative(minimumLength);

        if (TryRentCached(minimumLength, out buffer))
        {
            return true;
        }

        int bucketIndex = GetBucketIndex(minimumLength);
        int bucketSize = BucketSizes[bucketIndex];
        if (!TryReserveCommitted(bucketSize))
        {
            buffer = [];
            return false;
        }

        try
        {
            buffer = _source.Rent(bucketSize);
            int additionalBytes = buffer.Length - bucketSize;
            if (additionalBytes > 0 &&
                !TryReserveCommitted(additionalBytes))
            {
                _source.Return(buffer);
                Interlocked.Add(ref _committedBytes, -bucketSize);
                buffer = [];
                return false;
            }

            Interlocked.Add(ref _rentedBytes, buffer.Length);
            return true;
        }
        catch
        {
            Interlocked.Add(ref _committedBytes, -bucketSize);
            throw;
        }
    }

    public void Return(byte[] buffer)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        if (buffer.Length == 0)
        {
            return;
        }

        Interlocked.Add(ref _rentedBytes, -buffer.Length);

        int bucketIndex = GetBucketIndex(buffer.Length);
        if (Volatile.Read(ref _disposed) == 0 &&
            Interlocked.Increment(ref _cachedCounts[bucketIndex]) <=
                MaximumCachedBuffersPerBucket)
        {
            Interlocked.Add(
                ref _cachedBucketBytes[bucketIndex],
                buffer.Length);
            Interlocked.Add(ref _cachedBytes, buffer.Length);
            _buckets[bucketIndex].Enqueue(buffer);
            SignalCapacityRelease();
            return;
        }

        Interlocked.Decrement(ref _cachedCounts[bucketIndex]);
        Interlocked.Add(ref _committedBytes, -buffer.Length);

        _source.Return(buffer);
        SignalCapacityRelease();
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        foreach (ConcurrentQueue<byte[]> bucket in _buckets)
        {
            while (bucket.TryDequeue(out byte[]? buffer))
            {
                _source.Return(buffer);
            }
        }

        Array.Clear(_cachedCounts);
        Array.Clear(_cachedBucketBytes);
        Volatile.Write(ref _cachedBytes, 0);
        Volatile.Write(ref _committedBytes, 0);
        _capacityReleased.Dispose();
    }

    public async ValueTask WaitForCapacityAsync(CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _capacityWaiters);
        try
        {
            long generation = Volatile.Read(ref _returnGeneration);
            while (Volatile.Read(ref _returnGeneration) == generation)
            {
                await _capacityReleased
                    .WaitAsync(cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        finally
        {
            Interlocked.Decrement(ref _capacityWaiters);
        }
    }

    private static int[] CreateBucketSizes()
    {
        int[] sizes = new int[BucketCount];
        int size = MinimumBucketSize;
        for (int index = 0; index < sizes.Length; index++)
        {
            sizes[index] = size;
            if (index < sizes.Length - 1)
            {
                size = checked(size * 2);
            }
        }

        return sizes;
    }

    private static int GetBucketIndex(int length)
    {
        if (length <= MinimumBucketSize)
        {
            return 0;
        }

        int index = 32 - int.LeadingZeroCount(length - 1) - 8;
        return index >= BucketCount ? BucketCount - 1 : index;
    }

    private void SignalCapacityRelease()
    {
        Interlocked.Increment(ref _returnGeneration);
        if (Volatile.Read(ref _capacityWaiters) > 0)
        {
            _capacityReleased.Release();
        }
    }

    private bool TryRentCached(
        int minimumLength,
        out byte[] buffer)
    {
        int startIndex = GetBucketIndex(minimumLength);
        for (int index = startIndex; index < BucketCount; index++)
        {
            while (_buckets[index].TryDequeue(out byte[]? cached))
            {
                Interlocked.Decrement(ref _cachedCounts[index]);
                Interlocked.Add(
                    ref _cachedBucketBytes[index],
                    -cached.Length);
                Interlocked.Add(ref _cachedBytes, -cached.Length);
                if (cached.Length < minimumLength)
                {
                    Interlocked.Increment(ref _cachedCounts[index]);
                    Interlocked.Add(
                        ref _cachedBucketBytes[index],
                        cached.Length);
                    Interlocked.Add(ref _cachedBytes, cached.Length);
                    _buckets[index].Enqueue(cached);
                    continue;
                }

                Interlocked.Add(ref _rentedBytes, cached.Length);
                buffer = cached;
                return true;
            }
        }

        buffer = [];
        return false;
    }

    private bool TryReserveCommitted(int byteCount)
    {
        while (true)
        {
            long current = Volatile.Read(ref _committedBytes);
            if (byteCount > _maximumBytes - current)
            {
                return false;
            }

            if (Interlocked.CompareExchange(
                ref _committedBytes,
                current + byteCount,
                current) == current)
            {
                return true;
            }
        }
    }
}
