using MqttZero.Protocol;
using MqttZero.Sessions;
using System.Collections.Concurrent;

namespace MqttZero.Routing;

internal static class MqttZeroRouteSelector
{
    public static MqttZeroRouteScope Select(
        MqttZeroSubscriptionRouteSnapshot snapshot,
        ReadOnlySpan<byte> topic,
        MqttZeroSessionState? sender,
        ConcurrentDictionary<string, int> sharedCursors,
        MqttZeroRouteScopePool scopePool)
    {
        MqttZeroRouteScope route = scopePool.Rent();
        for (int groupIndex = 0; groupIndex < snapshot.RegularGroups.Length; groupIndex++)
        {
            MqttZeroRouteFilterGroup group = snapshot.RegularGroups[groupIndex];
            if (!MqttZeroTopicMatcher.Matches(group.Filter, topic))
            {
                continue;
            }

            for (int entryIndex = 0; entryIndex < group.Entries.Length; entryIndex++)
            {
                MqttZeroRouteEntry entry = group.Entries[entryIndex];
                if (!entry.Subscription.NoLocal || !ReferenceEquals(entry.Session, sender))
                {
                    route.AddRegular(entry.Session, entry.Subscription);
                }
            }
        }

        for (int index = 0; index < snapshot.SharedGroups.Length; index++)
        {
            route.AddSharedGroup(
                snapshot.SharedGroups[index],
                topic,
                sender,
                sharedCursors);
        }

        return route;
    }

}
