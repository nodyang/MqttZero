namespace MqttZero.Protocol;

/// <summary>
/// MQTT quality of service levels.
/// </summary>
public enum MqttQualityOfServiceLevel : byte
{
    /// <summary>At most once delivery.</summary>
    AtMostOnce = 0,

    /// <summary>At least once delivery.</summary>
    AtLeastOnce = 1,

    /// <summary>Exactly once delivery.</summary>
    ExactlyOnce = 2
}
