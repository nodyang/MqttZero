using MqttZero.Events;
using MqttZero.Protocol;
using System.Buffers;

namespace MqttZero.Interceptors;

/// <summary>Contains unsubscribe authorization data.</summary>
public sealed class MqttZeroUnsubscribeContext
{
    /// <summary>Gets the client information.</summary>
    public required MqttZeroClientInfo Client { get; init; }
    /// <summary>Gets the topic filter.</summary>
    public required ReadOnlySequence<byte> TopicFilter { get; init; }
    /// <summary>Gets or sets the denial reason code.</summary>
    public MqttReasonCode ReasonCode { get; set; } = MqttReasonCode.NotAuthorized;
    /// <summary>Gets or sets an optional reason string.</summary>
    public string? ReasonString { get; set; }
}
