using MqttZero.Broker;
using MqttZero.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Routing;
using MqttZero.Sessions;
using System.Collections.Concurrent;

namespace MqttZero.Tests.Routing
{
    public sealed class RouteAllocationTests
    {
        [Fact]
        public void RoutesOneSubscriberWithoutAllocatingAfterWarmup()
        {
            AssertZeroAllocationForSubscriberCount(1);
        }

        [Fact]
        public void RoutesOneHundredSubscribersWithoutAllocatingAfterWarmup()
        {
            AssertZeroAllocationForSubscriberCount(100);
        }

        [Fact]
        public void RoutesOneThousandSubscribersWithoutAllocatingAfterWarmup()
        {
            AssertZeroAllocationForSubscriberCount(1000);
        }

        [Fact]
        public void SharedSubscriptionSelectsOneSessionPerGroup()
        {
            List<MqttZeroSessionState> sessions =
            [
                CreateSession("a", "$share/workers/jobs/#"),
                CreateSession("b", "$share/workers/jobs/#"),
                CreateSession("c", "$share/audit/jobs/#")
            ];
            ConcurrentDictionary<string, int> cursors = new(StringComparer.Ordinal);
            MqttZeroRouteScopePool scopePool = new();

            using MqttZeroRouteScope route = MqttZeroRouteSelector.Select(
                sessions,
                "jobs/one"u8,
                null,
                cursors,
                scopePool);

            Assert.Equal(2, route.Count);
            Assert.Equal("a", route[0].Session.ClientId);
            Assert.Equal("c", route[1].Session.ClientId);
        }

        private static void AssertZeroAllocationForSubscriberCount(int subscriberCount)
        {
            List<MqttZeroSessionState> sessions = new(subscriberCount);
            for (int index = 0; index < subscriberCount; index++)
            {
                sessions.Add(CreateSession(
                    index.ToString(System.Globalization.CultureInfo.InvariantCulture),
                    "route/#"));
            }

            ConcurrentDictionary<string, int> cursors = new(StringComparer.Ordinal);
            MqttZeroRouteScopePool scopePool = new();
            for (int index = 0; index < 10; index++)
            {
                using (MqttZeroRouteScope warmup = MqttZeroRouteSelector.Select(
                    sessions,
                    "route/topic"u8,
                    null,
                    cursors,
                    scopePool))
                {
                    Assert.Equal(subscriberCount, warmup.Count);
                }
            }

            int delivered = 0;
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int index = 0; index < 100; index++)
            {
                using MqttZeroRouteScope route = MqttZeroRouteSelector.Select(
                    sessions,
                    "route/topic"u8,
                    null,
                    cursors,
                    scopePool);
                delivered += route.Count;
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.Equal(subscriberCount * 100, delivered);
            Assert.Equal(0, allocated);
        }

        private static MqttZeroSessionState CreateSession(string clientId, string topicFilter)
        {
            MqttZeroSessionState session = new(clientId);
            session.Subscriptions.Add(new MqttZeroSubscription(
                System.Text.Encoding.UTF8.GetBytes(topicFilter),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                false,
                null));
            return session;
        }
    }
}
