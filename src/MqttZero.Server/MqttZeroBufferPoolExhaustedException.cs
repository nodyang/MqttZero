namespace MqttZero;

internal sealed class MqttZeroBufferPoolExhaustedException : InvalidOperationException
{
    public MqttZeroBufferPoolExhaustedException()
        : base("The MQTTZero buffer pool byte limit was reached.")
    {
    }
}
