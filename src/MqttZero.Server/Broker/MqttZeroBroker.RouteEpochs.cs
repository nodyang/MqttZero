using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;

namespace MqttZero.Server.Broker;

public sealed partial class MqttZeroBroker
{
    private async ValueTask CommitSubscriptionsAsync(
        MqttZeroSessionState session,
        List<MqttZeroSubscription> original,
        List<MqttZeroSubscription> candidate,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(original);
        ArgumentNullException.ThrowIfNull(candidate);
        List<MqttZeroSubscription> removed = [];
        List<MqttZeroSubscription> added = [];
        BuildSubscriptionDiff(original, candidate, removed, added);
        await SaveSessionSubscriptionsAsync(
            session,
            candidate,
            cancellationToken).ConfigureAwait(false);
        await _routeMutationGate.WaitAsync(cancellationToken)
            .ConfigureAwait(false);
        try
        {
            MqttZeroRouteEpoch current = _routeEpochs.Current;
            MqttZeroRouteEpoch replacement = current;
            for (int index = 0; index < removed.Count; index++)
            {
                MqttZeroSubscription subscription = removed[index];
                replacement = replacement.Remove(session, subscription);
                ReleaseSharedGroupReference(subscription);
            }

            for (int index = 0; index < added.Count; index++)
            {
                MqttZeroSubscription subscription = added[index];
                replacement = replacement.Add(session, subscription);
                RetainSharedGroupReference(subscription);
            }

            lock (session.SyncRoot)
            {
                session.Subscriptions.Clear();
                session.Subscriptions.AddRange(candidate);
            }

            _routeEpochs.Publish(current, replacement);
        }
        finally
        {
            _routeMutationGate.Release();
        }
    }

    private async ValueTask RemoveSessionSubscriptionsFromRouteAsync(
        MqttZeroSessionState session)
    {
        MqttZeroSubscription[] subscriptions;
        lock (session.SyncRoot)
        {
            if (session.Subscriptions.Count == 0)
            {
                return;
            }

            subscriptions = [.. session.Subscriptions];
        }

        await _routeMutationGate.WaitAsync(CancellationToken.None)
            .ConfigureAwait(false);
        try
        {
            MqttZeroRouteEpoch current = _routeEpochs.Current;
            MqttZeroRouteEpoch replacement = current;
            for (int index = 0; index < subscriptions.Length; index++)
            {
                replacement = replacement.Remove(
                    session,
                    subscriptions[index]);
                ReleaseSharedGroupReference(subscriptions[index]);
            }

            _routeEpochs.Publish(current, replacement);
        }
        finally
        {
            _routeMutationGate.Release();
        }
    }

    private async ValueTask AddSessionSubscriptionsToRouteAsync(
        MqttZeroSessionState session)
    {
        MqttZeroSubscription[] subscriptions;
        lock (session.SyncRoot)
        {
            if (session.Subscriptions.Count == 0)
            {
                return;
            }

            subscriptions = [.. session.Subscriptions];
        }

        await _routeMutationGate.WaitAsync(CancellationToken.None)
            .ConfigureAwait(false);
        try
        {
            MqttZeroRouteEpoch current = _routeEpochs.Current;
            MqttZeroRouteEpoch replacement = current;
            for (int index = 0; index < subscriptions.Length; index++)
            {
                replacement = replacement.Add(
                    session,
                    subscriptions[index]);
                RetainSharedGroupReference(subscriptions[index]);
            }

            _routeEpochs.Publish(current, replacement);
        }
        finally
        {
            _routeMutationGate.Release();
        }
    }

    internal void ResetRouteEpochFromSessions()
    {
        MqttZeroRouteEpoch epoch =
            MqttZeroRouteEpochManager.BuildFromSessions(
                _sessions.Values);
        _routeMutationGate.Wait();
        try
        {
            _routeEpochs.Reset(epoch);
            RebuildSharedGroupReferences();
        }
        finally
        {
            _routeMutationGate.Release();
        }
    }

    private void RebuildSharedGroupReferences()
    {
        _sharedGroupReferences.Clear();
        foreach (MqttZeroSessionState session in _sessions.Values)
        {
            lock (session.SyncRoot)
            {
                for (int index = 0;
                    index < session.Subscriptions.Count;
                    index++)
                {
                    RetainSharedGroupReference(
                        session.Subscriptions[index]);
                }
            }
        }

        HashSet<MqttZeroSharedGroupKey> keys =
            [.. _sharedGroupReferences.Keys];
        _sharedGroupCursors.PruneTo(keys);
    }

    private void RetainSharedGroupReference(
        MqttZeroSubscription subscription)
    {
        if (!subscription.IsShared)
        {
            return;
        }

        MqttZeroSharedGroupKey key =
            MqttZeroSharedGroupKeys.Create(subscription);
        _sharedGroupReferences[key] =
            _sharedGroupReferences.TryGetValue(key, out int count)
                ? count + 1
                : 1;
    }

    private void ReleaseSharedGroupReference(
        MqttZeroSubscription subscription)
    {
        if (!subscription.IsShared)
        {
            return;
        }

        MqttZeroSharedGroupKey key =
            MqttZeroSharedGroupKeys.Create(subscription);
        if (!_sharedGroupReferences.TryGetValue(key, out int count))
        {
            return;
        }

        if (count <= 1)
        {
            _sharedGroupReferences.Remove(key);
            _sharedGroupCursors.Remove(key);
            return;
        }

        _sharedGroupReferences[key] = count - 1;
    }

    private static void BuildSubscriptionDiff(
        List<MqttZeroSubscription> original,
        List<MqttZeroSubscription> candidate,
        List<MqttZeroSubscription> removed,
        List<MqttZeroSubscription> added)
    {
        for (int index = 0; index < original.Count; index++)
        {
            MqttZeroSubscription subscription = original[index];
            MqttZeroSubscription? match = FindByFilter(
                candidate,
                subscription.TopicFilter);
            if (match == null ||
                !HasSameRouteSettings(match, subscription))
            {
                removed.Add(subscription);
            }
        }

        for (int index = 0; index < candidate.Count; index++)
        {
            MqttZeroSubscription subscription = candidate[index];
            MqttZeroSubscription? match = FindByFilter(
                original,
                subscription.TopicFilter);
            if (match == null ||
                !HasSameRouteSettings(match, subscription))
            {
                added.Add(subscription);
            }
        }
    }

    private static MqttZeroSubscription? FindByFilter(
        List<MqttZeroSubscription> subscriptions,
        byte[] topicFilter)
    {
        for (int index = 0; index < subscriptions.Count; index++)
        {
            if (subscriptions[index].TopicFilter
                .AsSpan()
                .SequenceEqual(topicFilter))
            {
                return subscriptions[index];
            }
        }

        return null;
    }

    private static bool HasSameRouteSettings(
        MqttZeroSubscription left,
        MqttZeroSubscription right)
    {
        return left.QualityOfServiceLevel == right.QualityOfServiceLevel &&
            left.NoLocal == right.NoLocal &&
            left.RetainAsPublished == right.RetainAsPublished &&
            left.SubscriptionIdentifier == right.SubscriptionIdentifier;
    }
}
