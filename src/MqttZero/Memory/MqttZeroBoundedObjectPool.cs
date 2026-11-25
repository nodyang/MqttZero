using System.Collections.Concurrent;

namespace MqttZero.Memory;

internal sealed class MqttZeroBoundedObjectPool<T>
    where T : class
{
    public static int DefaultCapacity { get; } = Math.Clamp(
        Environment.ProcessorCount * 256,
        1_024,
        4_096);

    private readonly ConcurrentQueue<T> _items = new();
    private int _count;

    public MqttZeroBoundedObjectPool(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        Capacity = capacity;
    }

    public int Capacity { get; }

    public int Count
    {
        get
        {
            return Volatile.Read(ref _count);
        }
    }

    public bool TryRent(out T? item)
    {
        if (_items.TryDequeue(out item))
        {
            Interlocked.Decrement(ref _count);
            return true;
        }

        item = null;
        return false;
    }

    public bool Return(T item)
    {
        ArgumentNullException.ThrowIfNull(item);
        int count = Interlocked.Increment(ref _count);
        if (count <= Capacity)
        {
            _items.Enqueue(item);
            return true;
        }

        Interlocked.Decrement(ref _count);
        return false;
    }
}
