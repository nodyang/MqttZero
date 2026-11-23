namespace MqttZero;

internal readonly struct MqttZeroDeliveryCandidate
{
    public MqttZeroDeliveryCandidate(
        MqttZeroSessionState session,
        MqttZeroSubscription subscription)
    {
        Session = session;
        Subscription = subscription;
    }

    public MqttZeroSessionState Session { get; }

    public MqttZeroSubscription Subscription { get; }

    public MqttQualityOfServiceLevel QualityOfServiceLevel
    {
        get
        {
            return Subscription.QualityOfServiceLevel;
        }
    }
}
