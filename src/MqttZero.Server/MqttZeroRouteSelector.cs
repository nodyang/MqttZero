using System.Collections.Concurrent;

namespace MqttZero;

internal static class MqttZeroRouteSelector
{
    public static MqttZeroRouteScope Select(
        IReadOnlyList<MqttZeroSessionState> sessions,
        ReadOnlySpan<byte> topic,
        MqttZeroSessionState? sender,
        ConcurrentDictionary<string, int> sharedCursors,
        MqttZeroRouteScopePool scopePool)
    {
        MqttZeroRouteScope route = scopePool.Rent();
        for (int index = 0; index < sessions.Count; index++)
        {
            AppendSession(route, sessions[index], topic, sender);
        }

        route.SelectSharedGroups(sharedCursors);
        return route;
    }

    private static void AppendSession(
        MqttZeroRouteScope route,
        MqttZeroSessionState session,
        ReadOnlySpan<byte> topic,
        MqttZeroSessionState? sender)
    {
        MqttZeroSubscription? bestRegular = null;
        lock (session.SyncRoot)
        {
            foreach (MqttZeroSubscription subscription in session.Subscriptions)
            {
                if (subscription.NoLocal && ReferenceEquals(session, sender))
                {
                    continue;
                }

                if (!MqttZeroTopicMatcher.Matches(subscription.MatchFilter, topic))
                {
                    continue;
                }

                if (subscription.IsShared)
                {
                    route.AddShared(session, subscription, subscription.SharedGroup!);
                }
                else if (bestRegular == null ||
                    subscription.QualityOfServiceLevel > bestRegular.QualityOfServiceLevel)
                {
                    bestRegular = subscription;
                }
            }
        }

        if (bestRegular != null)
        {
            route.AddDelivery(new MqttZeroDeliveryCandidate(session, bestRegular));
        }
    }
}
