using MqttZero.Server.Clients;
using MqttZero.Protocol;
using System.Buffers;

namespace MqttZero.Server.Interceptors;

/// <summary>Contains unsubscribe authorization data.</summary>
public sealed class MqttZeroUnsubscribeContext
{
    /// <summary>Gets the connected client.</summary>
    public required MqttZeroConnectedClient ConnectedClient { get; init; }
    /// <summary>Gets the topic filter.</summary>
    public required ReadOnlySequence<byte> TopicFilter { get; init; }
    /// <summary>Gets or sets the denial reason code.</summary>
    public MqttReasonCode ReasonCode { get; set; } = MqttReasonCode.NotAuthorized;
    /// <summary>Gets or sets an optional reason string.</summary>
    public string? ReasonString { get; set; }
}
