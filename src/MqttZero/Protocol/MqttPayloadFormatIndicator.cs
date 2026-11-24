namespace MqttZero.Protocol;

/// <summary>
/// Identifies the payload encoding declared by an MQTT PUBLISH or Will message.
/// </summary>
public enum MqttPayloadFormatIndicator : byte
{
    /// <summary>The payload uses an unspecified byte format.</summary>
    UnspecifiedUtf8 = 0,

    /// <summary>The payload contains well-formed UTF-8 data.</summary>
    Utf8 = 1
}
