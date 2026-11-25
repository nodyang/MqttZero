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
            return _items.Count;
        }
    }

    public bool TryRent(out T? item)
    {
        if (_items.TryDequeue(out item))
        {
            return true;
        }

        item = null;
        return false;
    }

    public bool Return(T item)
    {
        ArgumentNullException.ThrowIfNull(item);
        if (_items.Count < Capacity)
        {
            _items.Enqueue(item);
            return true;
        }

        return false;
    }
}
