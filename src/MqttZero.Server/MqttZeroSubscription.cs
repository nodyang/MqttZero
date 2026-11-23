namespace MqttZero;

internal sealed class MqttZeroSubscription
{
    public MqttZeroSubscription(
        byte[] topicFilter,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        bool noLocal,
        bool retainAsPublished)
    {
        TopicFilter = topicFilter;
        QualityOfServiceLevel = qualityOfServiceLevel;
        NoLocal = noLocal;
        RetainAsPublished = retainAsPublished;
    }

    public byte[] TopicFilter { get; }

    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; }

    public bool NoLocal { get; }

    public bool RetainAsPublished { get; }
}
