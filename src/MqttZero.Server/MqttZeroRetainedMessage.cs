namespace MqttZero;

internal sealed class MqttZeroRetainedMessage
{
    public MqttZeroRetainedMessage(
        byte[] topic,
        byte[] payload,
        MqttQualityOfServiceLevel qualityOfServiceLevel)
    {
        Topic = topic;
        Payload = payload;
        QualityOfServiceLevel = qualityOfServiceLevel;
    }

    public byte[] Topic { get; }

    public byte[] Payload { get; }

    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; }
}
