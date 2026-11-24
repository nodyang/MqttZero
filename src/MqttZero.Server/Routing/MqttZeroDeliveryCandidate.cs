using MqttZero.Protocol;
using MqttZero.Server.Sessions;
namespace MqttZero.Server.Routing;

internal readonly struct MqttZeroDeliveryCandidate
{
    public MqttZeroDeliveryCandidate(
        MqttZeroSessionState session,
        MqttZeroSubscription subscription,
        ReadOnlyMemory<uint> subscriptionIdentifiers)
    {
        Session = session;
        Subscription = subscription;
        SubscriptionIdentifiers = subscriptionIdentifiers;
    }

    public MqttZeroSessionState Session { get; }

    public MqttZeroSubscription Subscription { get; }

    public ReadOnlyMemory<uint> SubscriptionIdentifiers { get; }

    public MqttQualityOfServiceLevel QualityOfServiceLevel
    {
        get
        {
            return Subscription.QualityOfServiceLevel;
        }
    }
}
