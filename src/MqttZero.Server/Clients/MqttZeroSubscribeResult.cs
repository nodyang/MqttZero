using MqttZero.Protocol;

namespace MqttZero.Clients;

/// <summary>Describes one server-originated subscription result.</summary>
public sealed class MqttZeroSubscribeResult
{
    /// <summary>Gets the topic filter.</summary>
    public required string TopicFilter { get; init; }
    /// <summary>Gets the granted QoS.</summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; init; }
    /// <summary>Gets the result reason code.</summary>
    public MqttReasonCode ReasonCode { get; init; }
}
