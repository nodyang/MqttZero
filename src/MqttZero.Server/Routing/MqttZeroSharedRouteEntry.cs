using MqttZero.Sessions;

namespace MqttZero.Routing;

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
