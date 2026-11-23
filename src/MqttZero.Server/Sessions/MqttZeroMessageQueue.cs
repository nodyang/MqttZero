namespace MqttZero.Sessions;

internal sealed class MqttZeroMessageQueue
{
    private readonly MqttZeroQueuedMessage[] _items;
    private int _count;
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
        for (int current = index; current < _count - 1; current++)
        {
            _items[(_head + current) % _items.Length] =
                _items[(_head + current + 1) % _items.Length];
        }

        _items[(_head + _count - 1) % _items.Length] = null!;
        _count--;
    }

    public void Clear()
    {
        Array.Clear(_items);
        _head = 0;
        _count = 0;
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
