namespace MqttZero.Sessions;

internal sealed class MqttZeroMessageQueue
{
    private readonly MqttZeroQueuedMessage[] _items;
    private int _count;
    private int _expiringCount;
    private int _head;

    public MqttZeroMessageQueue(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(capacity);
        _items = new MqttZeroQueuedMessage[capacity];
    }

    public int Count
    {
        get
        {
            return _count;
        }
    }

    public bool HasExpiringMessages
    {
        get
        {
            return _expiringCount > 0;
        }
    }

    public MqttZeroQueuedMessage this[int index]
    {
        get
        {
            ArgumentOutOfRangeException.ThrowIfNegative(index);
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, _count);
            return _items[(_head + index) % _items.Length];
        }
    }

    public void Add(MqttZeroQueuedMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        if (_count == _items.Length)
        {
            throw new InvalidOperationException("The message queue is full.");
        }

        _items[(_head + _count) % _items.Length] = message;
        _count++;
        if (message.ExpiresAt.HasValue)
        {
            _expiringCount++;
        }
    }

    public MqttZeroQueuedMessage Peek()
    {
        if (_count == 0)
        {
            throw new InvalidOperationException("The message queue is empty.");
        }

        return _items[_head];
    }

    public MqttZeroQueuedMessage Dequeue()
    {
        MqttZeroQueuedMessage message = Peek();
        _items[_head] = null!;
        _head = (_head + 1) % _items.Length;
        _count--;
        if (message.ExpiresAt.HasValue)
        {
            _expiringCount--;
        }
        return message;
    }

    public int FindIndex(Predicate<MqttZeroQueuedMessage> predicate)
    {
        for (int index = 0; index < _count; index++)
        {
            if (predicate(this[index]))
            {
                return index;
            }
        }

        return -1;
    }

    public void RemoveAt(int index)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(index, _count);
        MqttZeroQueuedMessage removed =
            _items[(_head + index) % _items.Length];
        for (int current = index; current < _count - 1; current++)
        {
            _items[(_head + current) % _items.Length] =
                _items[(_head + current + 1) % _items.Length];
        }

        if (removed.ExpiresAt.HasValue)
        {
            _expiringCount--;
        }

        _items[(_head + _count - 1) % _items.Length] = null!;
        _count--;
    }

    public void Clear()
    {
        Array.Clear(_items);
        _head = 0;
        _count = 0;
        _expiringCount = 0;
    }

    public void RemoveExpired(DateTimeOffset now)
    {
        if (_expiringCount == 0)
        {
            return;
        }

        for (int index = _count - 1; index >= 0; index--)
        {
            if (index < _count && this[index].IsExpired(now))
            {
                MqttZeroQueuedMessage message = this[index];
                RemoveAt(index);
                message.Release();
            }
        }
    }

    public Enumerator GetEnumerator()
    {
        return new Enumerator(this);
    }

    internal struct Enumerator
    {
        private readonly MqttZeroMessageQueue _queue;
        private int _index;

        public Enumerator(MqttZeroMessageQueue queue)
        {
            _queue = queue;
            _index = -1;
        }

        public MqttZeroQueuedMessage Current
        {
            get
            {
                return _queue[_index];
            }
        }

        public bool MoveNext()
        {
            _index++;
            return _index < _queue.Count;
        }
    }
}
