using MqttZero.Protocol;
using MqttZero.Sessions;
using System.Buffers;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace MqttZero.Routing;

internal sealed class MqttZeroRouteScope : IDisposable
{
    private MqttZeroDeliveryCandidate[] _deliveries = [];
    private int[] _bestDeliveryIndexes = [];
    private int _bestCapacity;
    private int[] _bestEpochs = [];
    private MqttZeroSessionState?[] _bestSessions = [];
    private int _bestCount;
    private int _epoch;
    private MqttZeroSessionState[] _sharedSessions = [];
    private MqttZeroSubscription[] _sharedSubscriptions = [];
    private string[] _sharedGroups = [];
    private int _deliveryCount;
    private MqttZeroRouteScopePool? _owner;
    private int _sharedCount;

    public MqttZeroRouteScope()
    {
    }

    public int Count
    {
        get
        {
            return _deliveryCount;
        }
    }

    public MqttZeroDeliveryCandidate this[int index]
    {
        get
        {
            return _deliveries[index];
        }
    }

    public void SetOwner(MqttZeroRouteScopePool owner)
    {
        _owner = owner;
        _epoch++;
        if (_epoch == 0)
        {
            Array.Clear(_bestEpochs);
            _epoch = 1;
        }

        _bestCount = 0;
    }

    public void AddDelivery(MqttZeroDeliveryCandidate candidate)
    {
        EnsureDeliveryCapacity(_deliveryCount + 1);
        _deliveries[_deliveryCount++] = candidate;
    }

    public void AddRegular(
        MqttZeroSessionState session,
        MqttZeroSubscription subscription)
    {
        EnsureBestCapacity();
        int mask = _bestCapacity - 1;
        int slot = RuntimeHelpers.GetHashCode(session) & mask;
        while (true)
        {
            if (_bestEpochs[slot] != _epoch ||
                _bestSessions[slot] == null)
            {
                int deliveryIndex = _deliveryCount;
                AddDelivery(new MqttZeroDeliveryCandidate(session, subscription));
                _bestSessions[slot] = session;
                _bestDeliveryIndexes[slot] = deliveryIndex;
                _bestEpochs[slot] = _epoch;
                _bestCount++;
                if (_bestCount * 2 >= _bestSessions.Length)
                {
                    GrowBestTable();
                }

                return;
            }

            if (ReferenceEquals(_bestSessions[slot], session))
            {
                int deliveryIndex = _bestDeliveryIndexes[slot];
                if (subscription.QualityOfServiceLevel >
                    _deliveries[deliveryIndex].QualityOfServiceLevel)
                {
                    _deliveries[deliveryIndex] =
                        new MqttZeroDeliveryCandidate(session, subscription);
                }

                return;
            }

            slot = (slot + 1) & mask;
        }
    }

    public void AddShared(
        MqttZeroSessionState session,
        MqttZeroSubscription subscription,
        string group)
    {
        for (int index = 0; index < _sharedCount; index++)
        {
            if (ReferenceEquals(_sharedSessions[index], session) &&
                string.Equals(_sharedGroups[index], group, StringComparison.Ordinal))
            {
                if (subscription.QualityOfServiceLevel >
                    _sharedSubscriptions[index].QualityOfServiceLevel)
                {
                    _sharedSubscriptions[index] = subscription;
                }

                return;
            }
        }

        EnsureSharedCapacity(_sharedCount + 1);
        _sharedSessions[_sharedCount] = session;
        _sharedSubscriptions[_sharedCount] = subscription;
        _sharedGroups[_sharedCount] = group;
        _sharedCount++;
    }

    public void SelectSharedGroups(ConcurrentDictionary<string, int> cursors)
    {
        for (int first = 0; first < _sharedCount; first++)
        {
            string group = _sharedGroups[first];
            bool alreadyProcessed = false;
            for (int earlier = 0; earlier < first; earlier++)
            {
                if (string.Equals(_sharedGroups[earlier], group, StringComparison.Ordinal))
                {
                    alreadyProcessed = true;
                    break;
                }
            }

            if (alreadyProcessed)
            {
                continue;
            }

            int groupCount = 0;
            for (int candidateIndex = first; candidateIndex < _sharedCount; candidateIndex++)
            {
                if (string.Equals(_sharedGroups[candidateIndex], group, StringComparison.Ordinal))
                {
                    groupCount++;
                }
            }

            int cursor = cursors.AddOrUpdate(
                group,
                0,
                static (_, current) => current == int.MaxValue ? 0 : current + 1);
            int selectedOffset = cursor % groupCount;
            for (int candidateIndex = first; candidateIndex < _sharedCount; candidateIndex++)
            {
                if (!string.Equals(_sharedGroups[candidateIndex], group, StringComparison.Ordinal))
                {
                    continue;
                }

                if (selectedOffset == 0)
                {
                    AddDelivery(new MqttZeroDeliveryCandidate(
                        _sharedSessions[candidateIndex],
                        _sharedSubscriptions[candidateIndex]));
                    break;
                }

                selectedOffset--;
            }
        }
    }

    public void Dispose()
    {
        Array.Clear(_deliveries, 0, _deliveryCount);
        Array.Clear(_sharedSessions, 0, _sharedCount);
        Array.Clear(_sharedSubscriptions, 0, _sharedCount);
        Array.Clear(_sharedGroups, 0, _sharedCount);
        _deliveryCount = 0;
        _bestCount = 0;
        _sharedCount = 0;
        MqttZeroRouteScopePool owner = _owner ??
            throw new InvalidOperationException("The route scope has no owner pool.");
        _owner = null;
        owner.Return(this);
    }

    public void ReleaseBuffers()
    {
        if (_deliveries.Length > 0)
        {
            ArrayPool<MqttZeroDeliveryCandidate>.Shared.Return(_deliveries, true);
            _deliveries = [];
        }

        ReturnSharedArrays();
        ReturnBestArrays();
        _sharedSessions = [];
        _sharedSubscriptions = [];
        _sharedGroups = [];
        _deliveryCount = 0;
        _sharedCount = 0;
        _owner = null;
    }

    private void EnsureDeliveryCapacity(int minimumLength)
    {
        if (_deliveries.Length >= minimumLength)
        {
            return;
        }

        MqttZeroDeliveryCandidate[] replacement =
            ArrayPool<MqttZeroDeliveryCandidate>.Shared.Rent(minimumLength);
        if (_deliveryCount > 0)
        {
            Array.Copy(_deliveries, replacement, _deliveryCount);
        }

        if (_deliveries.Length > 0)
        {
            ArrayPool<MqttZeroDeliveryCandidate>.Shared.Return(_deliveries);
        }

        _deliveries = replacement;
    }

    private void EnsureSharedCapacity(int minimumLength)
    {
        if (_sharedSessions.Length >= minimumLength)
        {
            return;
        }

        MqttZeroSessionState[] sessions =
            ArrayPool<MqttZeroSessionState>.Shared.Rent(minimumLength);
        MqttZeroSubscription[] subscriptions =
            ArrayPool<MqttZeroSubscription>.Shared.Rent(minimumLength);
        string[] groups = ArrayPool<string>.Shared.Rent(minimumLength);
        if (_sharedCount > 0)
        {
            Array.Copy(_sharedSessions, sessions, _sharedCount);
            Array.Copy(_sharedSubscriptions, subscriptions, _sharedCount);
            Array.Copy(_sharedGroups, groups, _sharedCount);
        }

        ReturnSharedArrays();
        _sharedSessions = sessions;
        _sharedSubscriptions = subscriptions;
        _sharedGroups = groups;
    }

    private void ReturnSharedArrays()
    {
        if (_sharedSessions.Length > 0)
        {
            ArrayPool<MqttZeroSessionState>.Shared.Return(_sharedSessions, true);
        }

        if (_sharedSubscriptions.Length > 0)
        {
            ArrayPool<MqttZeroSubscription>.Shared.Return(_sharedSubscriptions, true);
        }

        if (_sharedGroups.Length > 0)
        {
            ArrayPool<string>.Shared.Return(_sharedGroups, true);
        }
    }

    private void EnsureBestCapacity()
    {
        if (_bestSessions.Length == 0)
        {
            _bestCapacity = 16;
            _bestSessions = ArrayPool<MqttZeroSessionState?>.Shared.Rent(16);
            _bestDeliveryIndexes = ArrayPool<int>.Shared.Rent(16);
            _bestEpochs = ArrayPool<int>.Shared.Rent(16);
            Array.Clear(_bestSessions, 0, _bestSessions.Length);
            Array.Clear(_bestEpochs, 0, _bestEpochs.Length);
        }
    }

    private void GrowBestTable()
    {
        int capacity = _bestCapacity * 2;
        MqttZeroSessionState?[] sessions =
            ArrayPool<MqttZeroSessionState?>.Shared.Rent(capacity);
        int[] deliveryIndexes = ArrayPool<int>.Shared.Rent(capacity);
        int[] epochs = ArrayPool<int>.Shared.Rent(capacity);
        Array.Clear(sessions, 0, sessions.Length);
        Array.Clear(epochs, 0, epochs.Length);
        int epoch = _epoch;
        if (epoch == int.MaxValue)
        {
            _epoch = 1;
            epoch = 1;
            Array.Clear(_bestEpochs);
        }

        int mask = capacity - 1;
        for (int source = 0; source < _bestCapacity; source++)
        {
            MqttZeroSessionState? session = _bestSessions[source];
            if (session == null || _bestEpochs[source] != epoch)
            {
                continue;
            }

            int slot = RuntimeHelpers.GetHashCode(session) & mask;
            while (sessions[slot] != null)
            {
                slot = (slot + 1) & mask;
            }

            sessions[slot] = session;
            deliveryIndexes[slot] = _bestDeliveryIndexes[source];
            epochs[slot] = epoch;
        }

        ReturnBestArrays();
        _bestSessions = sessions;
        _bestDeliveryIndexes = deliveryIndexes;
        _bestEpochs = epochs;
        _bestCapacity = capacity;
    }

    private void ReturnBestArrays()
    {
        if (_bestSessions.Length > 0)
        {
            ArrayPool<MqttZeroSessionState?>.Shared.Return(_bestSessions, true);
            ArrayPool<int>.Shared.Return(_bestDeliveryIndexes);
            ArrayPool<int>.Shared.Return(_bestEpochs);
            _bestSessions = [];
            _bestDeliveryIndexes = [];
            _bestEpochs = [];
            _bestCapacity = 0;
        }
    }
}
