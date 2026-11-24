using MqttZero.Events;
using MqttZero.Protocol;

namespace MqttZero.Observers;

/// <summary>Describes a disconnected client.</summary>
public sealed class MqttZeroClientDisconnectedContext
{
    /// <summary>Gets the client information.</summary>
    public required MqttZeroClientInfo Client { get; init; }
    /// <summary>Gets the disconnect type.</summary>
    public MqttZeroDisconnectType DisconnectType { get; init; }
    /// <summary>Gets the MQTT reason code.</summary>
    public MqttReasonCode? ReasonCode { get; init; }
}
