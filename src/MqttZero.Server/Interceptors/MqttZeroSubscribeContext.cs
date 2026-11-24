using MqttZero.Events;
using MqttZero.Protocol;
using System.Buffers;

namespace MqttZero.Interceptors;

/// <summary>Contains subscription authorization data.</summary>
public sealed class MqttZeroSubscribeContext
{
    /// <summary>Gets the client information.</summary>
    public required MqttZeroClientInfo Client { get; init; }
    /// <summary>Gets the topic filter.</summary>
    public required ReadOnlySequence<byte> TopicFilter { get; init; }
    /// <summary>Gets the requested QoS.</summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; init; }
    /// <summary>Gets whether local publications are suppressed.</summary>
    public bool NoLocal { get; init; }
    /// <summary>Gets whether RETAIN is preserved.</summary>
    public bool RetainAsPublished { get; init; }
    /// <summary>Gets the retained-message handling mode.</summary>
    public MqttRetainHandling RetainHandling { get; init; }
    /// <summary>Gets or sets the denial reason code.</summary>
    public MqttReasonCode ReasonCode { get; set; } = MqttReasonCode.NotAuthorized;
    /// <summary>Gets or sets an optional reason string.</summary>
    public string? ReasonString { get; set; }
}
