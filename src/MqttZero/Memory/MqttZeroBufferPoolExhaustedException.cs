using MqttZero.Protocol;
namespace MqttZero.Memory;

internal sealed class MqttZeroBufferPoolExhaustedException : InvalidOperationException
{
    public MqttZeroBufferPoolExhaustedException()
        : this(0, 0)
    {
    }

    public MqttZeroBufferPoolExhaustedException(
        int requiredBytes,
        long returnGeneration)
        : base("The MQTTZero buffer pool byte limit was reached.")
    {
        RequiredBytes = requiredBytes;
        ReturnGeneration = returnGeneration;
    }

    public int RequiredBytes { get; }

    public long ReturnGeneration { get; }
}
