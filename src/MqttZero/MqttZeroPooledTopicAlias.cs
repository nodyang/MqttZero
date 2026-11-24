namespace MqttZero;

internal sealed class MqttZeroPooledTopicAlias
{
    public MqttZeroPooledTopicAlias(byte[] buffer, int length)
    {
        Buffer = buffer;
        Length = length;
    }

    public byte[] Buffer { get; }

    public int Length { get; set; }
}
