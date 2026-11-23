using System.Buffers;

namespace MqttZero;

/// <summary>
/// Contains mutable publish data for an interceptor.
/// </summary>
public sealed class MqttZeroPublishContext
{
    /// <summary>
    /// Gets or sets the UTF-8 topic name.
    /// </summary>
    public ReadOnlySequence<byte> Topic { get; set; }

    /// <summary>
    /// Gets or sets the payload sequence.
    /// </summary>
    public ReadOnlySequence<byte> Payload { get; set; }

    /// <summary>
    /// Gets or sets the MQTT 5 property section.
    /// </summary>
    public ReadOnlySequence<byte> Properties { get; set; }

    /// <summary>
    /// Gets or sets the quality of service.
    /// </summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the message is retained.
    /// </summary>
    public bool Retain { get; set; }

    /// <summary>
    /// Gets the originating client identifier, or <see langword="null"/> for server-originated messages.
    /// </summary>
    public string? SenderClientId { get; internal set; }
}
