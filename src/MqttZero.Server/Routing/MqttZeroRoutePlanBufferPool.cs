using System.Collections.Concurrent;

namespace MqttZero.Server.Routing;

internal sealed class MqttZeroRoutePlanBufferPool : IDisposable
{
    private readonly ConcurrentQueue<MqttZeroRoutePlanBuffer> _buffers =
        new();
    private readonly int _maximumBufferCount;
    private readonly long _maximumRetainedBytes;
    private readonly int _shardCount;
    private int _availableCount;
    private int _disposed;
    private int _inUseCount;
    private long _retainedBytes;

    public MqttZeroRoutePlanBufferPool(
        int shardCount,
        long maxBufferedBytes = 512L * 1024 * 1024)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(shardCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxBufferedBytes);
        _shardCount = shardCount;
        _maximumBufferCount = Math.Clamp(shardCount * 2, 8, 64);
        _maximumRetainedBytes = Math.Clamp(
            maxBufferedBytes / 8,
            8L * 1024 * 1024,
            64L * 1024 * 1024);
        ReturnCallback = Return;
    }

    public Action<MqttZeroRoutePlanBuffer> ReturnCallback { get; }

    public int AvailableCount
    {
        get
        {
            return Volatile.Read(ref _availableCount);
        }
    }

    public int InUseCount
    {
        get
        {
            return Volatile.Read(ref _inUseCount);
        }
    }

    public MqttZeroRoutePlanBufferPoolSnapshot GetSnapshot()
    {
        return new MqttZeroRoutePlanBufferPoolSnapshot(
            AvailableCount,
            InUseCount,
            Volatile.Read(ref _retainedBytes));
    }

    internal static bool ShouldRetainBuffer(
        int availableCount,
        long retainedBytes,
        long incomingBytes,
        int maximumBufferCount,
        long maximumRetainedBytes)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(availableCount);
        ArgumentOutOfRangeException.ThrowIfNegative(retainedBytes);
        ArgumentOutOfRangeException.ThrowIfNegative(incomingBytes);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(
            maximumBufferCount);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(
            maximumRetainedBytes);
        return availableCount < maximumBufferCount &&
            retainedBytes <= maximumRetainedBytes - incomingBytes;
    }

    public MqttZeroRoutePlanBuffer Rent()
    {
        ObjectDisposedException.ThrowIf(
            Volatile.Read(ref _disposed) != 0,
            this);
        MqttZeroRoutePlanBuffer buffer =
            _buffers.TryDequeue(out MqttZeroRoutePlanBuffer? pooled)
                ? pooled
                : new MqttZeroRoutePlanBuffer(_shardCount);
        if (pooled != null)
        {
            Interlocked.Decrement(ref _availableCount);
        }

        if (pooled is null)
        {
            Interlocked.Add(
                ref _retainedBytes,
                buffer.GetRetainedByteEstimate());
        }

        Interlocked.Increment(ref _inUseCount);
        return buffer;
    }

    public void Return(MqttZeroRoutePlanBuffer buffer)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        Interlocked.Decrement(ref _inUseCount);
        long retainedBytes = Volatile.Read(ref _retainedBytes);
        long bufferBytes = buffer.GetRetainedByteEstimate();
        if (Volatile.Read(ref _disposed) != 0 ||
            !ShouldRetainBuffer(
                AvailableCount,
                retainedBytes,
                bufferBytes,
                _maximumBufferCount,
                _maximumRetainedBytes))
        {
            Interlocked.Add(
                ref _retainedBytes,
                -bufferBytes);
            buffer.Dispose();
            return;
        }

        _buffers.Enqueue(buffer);
        Interlocked.Increment(ref _availableCount);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        while (_buffers.TryDequeue(out MqttZeroRoutePlanBuffer? buffer))
        {
            Interlocked.Decrement(ref _availableCount);
            Interlocked.Add(
                ref _retainedBytes,
                -buffer.GetRetainedByteEstimate());
            buffer.Dispose();
        }
    }
}
