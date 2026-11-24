using MqttZero.Protocol;

namespace MqttZero.Clients;

/// <summary>Describes one server-originated unsubscribe result.</summary>
public sealed class MqttZeroUnsubscribeResult
{
    /// <summary>Gets the topic filter.</summary>
    public required string TopicFilter { get; init; }
    /// <summary>Gets the result reason code.</summary>
    public MqttReasonCode ReasonCode { get; init; }
}
