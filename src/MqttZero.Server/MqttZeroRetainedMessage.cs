namespace MqttZero;

internal sealed class MqttZeroRetainedMessage
{
    public MqttZeroRetainedMessage(
        byte[] topic,
        byte[] payload,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        DateTimeOffset? expiresAt)
    {
        Topic = topic;
        Payload = payload;
        QualityOfServiceLevel = qualityOfServiceLevel;
        ExpiresAt = expiresAt;
    }

    public byte[] Topic { get; }

    public byte[] Payload { get; }

    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; }

    public DateTimeOffset? ExpiresAt { get; }
}
