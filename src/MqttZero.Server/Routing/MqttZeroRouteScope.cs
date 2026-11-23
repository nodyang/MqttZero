using MqttZero.Protocol;
using MqttZero.Sessions;
using System.Buffers;
using System.Collections.Concurrent;

namespace MqttZero.Routing;

internal sealed class MqttZeroRouteScope : IDisposable
{
    private MqttZeroDeliveryCandidate[] _deliveries = [];
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
    }

    public void AddDelivery(MqttZeroDeliveryCandidate candidate)
    {
        EnsureDeliveryCapacity(_deliveryCount + 1);
        _deliveries[_deliveryCount++] = candidate;
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
}
