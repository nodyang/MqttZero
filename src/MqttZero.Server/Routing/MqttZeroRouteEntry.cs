using MqttZero.Protocol;
using MqttZero.Server.Sessions;
namespace MqttZero.Server.Routing;

internal readonly struct MqttZeroRouteEntry
{
    public MqttZeroRouteEntry(
        MqttZeroSessionState session,
        MqttZeroSubscription subscription)
    {
        Session = session;
        Subscription = subscription;
    }

    public MqttZeroSessionState Session { get; }

    public MqttZeroSubscription Subscription { get; }
}
