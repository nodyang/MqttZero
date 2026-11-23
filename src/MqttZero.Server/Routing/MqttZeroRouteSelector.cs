using MqttZero.Protocol;
using MqttZero.Sessions;
using System.Collections.Concurrent;

namespace MqttZero.Routing;

internal static class MqttZeroRouteSelector
{
    public static MqttZeroRouteScope Select(
        MqttZeroRouteEntry[] entries,
        ReadOnlySpan<byte> topic,
        MqttZeroSessionState? sender,
        ConcurrentDictionary<string, int> sharedCursors,
        MqttZeroRouteScopePool scopePool)
    {
        MqttZeroRouteScope route = scopePool.Rent();
        int index = 0;
        while (index < entries.Length)
        {
            MqttZeroSessionState session = entries[index].Session;
            MqttZeroSubscription? bestRegular = null;
            while (index < entries.Length && ReferenceEquals(entries[index].Session, session))
            {
                AppendSubscription(
                    route,
                    session,
                    entries[index].Subscription,
                    topic,
                    sender,
                    ref bestRegular);
                index++;
            }

            if (bestRegular != null)
            {
                route.AddDelivery(new MqttZeroDeliveryCandidate(session, bestRegular));
            }
        }

        route.SelectSharedGroups(sharedCursors);
        return route;
    }

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

                AppendSubscription(
                    route,
                    session,
                    subscription,
                    topic,
                    sender,
                    ref bestRegular);
            }
        }

        if (bestRegular != null)
        {
            route.AddDelivery(new MqttZeroDeliveryCandidate(session, bestRegular));
        }
    }

    private static void AppendSubscription(
        MqttZeroRouteScope route,
        MqttZeroSessionState session,
        MqttZeroSubscription subscription,
        ReadOnlySpan<byte> topic,
        MqttZeroSessionState? sender,
        ref MqttZeroSubscription? bestRegular)
    {
        if (subscription.NoLocal && ReferenceEquals(session, sender))
        {
            return;
        }

        if (!MqttZeroTopicMatcher.Matches(subscription.MatchFilter, topic))
        {
            return;
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
