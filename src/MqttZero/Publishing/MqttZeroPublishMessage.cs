using System.Buffers;
using MqttZero.Protocol;

namespace MqttZero.Publishing;

/// <summary>
/// Describes one server-originated MQTT PUBLISH message.
/// </summary>
public sealed class MqttZeroPublishMessage
{
    /// <summary>
    /// Gets the MQTT topic name.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// Gets the payload sequence.
    /// </summary>
    public required ReadOnlySequence<byte> Payload { get; init; }

    /// <summary>
    /// Gets the requested quality of service.
    /// </summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; init; } =
        MqttQualityOfServiceLevel.AtMostOnce;

    /// <summary>
    /// Gets a value indicating whether the message should be retained.
    /// </summary>
    public bool Retain { get; init; }
}
