using MqttZero.Server.Sessions;

namespace MqttZero.Server.Routing;

internal sealed class MqttZeroSharedRouteEntry
{
    public MqttZeroSharedRouteEntry(
        MqttZeroSessionState session,
        MqttZeroSubscription[] subscriptions)
    {
        Session = session;
        Subscriptions = subscriptions;
    }

    public MqttZeroSessionState Session { get; }

    public MqttZeroSubscription[] Subscriptions { get; }
}
