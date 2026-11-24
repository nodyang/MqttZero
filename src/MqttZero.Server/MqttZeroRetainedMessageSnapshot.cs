using MqttZero.Protocol;
namespace MqttZero.Server;

/// <summary>
/// Represents retained message metadata.
/// </summary>
public sealed class MqttZeroRetainedMessageSnapshot
{
    /// <summary>
    /// Gets the retained topic.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// Gets the payload length in bytes.
    /// </summary>
    public int PayloadLength { get; init; }

    /// <summary>
    /// Gets the retained message quality of service.
    /// </summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; init; }
}
