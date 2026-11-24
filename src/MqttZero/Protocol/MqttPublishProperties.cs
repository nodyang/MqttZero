using System.Buffers;

namespace MqttZero.Protocol;

/// <summary>
/// Describes end-to-end MQTT properties carried by a PUBLISH or Will message.
/// </summary>
public sealed class MqttPublishProperties
{
    /// <summary>
    /// Gets the shared empty property instance.
    /// </summary>
    public static MqttPublishProperties Empty { get; } = new();

    /// <summary>
    /// Gets the payload format indicator.
    /// </summary>
    public MqttPayloadFormatIndicator? PayloadFormatIndicator { get; init; }

    /// <summary>
    /// Gets the remaining message expiry interval in seconds.
    /// </summary>
    public uint? MessageExpiryInterval { get; init; }

    /// <summary>
    /// Gets the content type.
    /// </summary>
    public string? ContentType { get; init; }

    /// <summary>
    /// Gets the response topic.
    /// </summary>
    public string? ResponseTopic { get; init; }

    /// <summary>
    /// Gets the opaque correlation data.
    /// </summary>
    public ReadOnlyMemory<byte>? CorrelationData { get; init; }

    /// <summary>
    /// Gets the ordered user properties. Duplicate names are preserved.
    /// </summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; init; } =
        Array.Empty<MqttUserProperty>();
}
