using MqttZero.Server.Clients;
using MqttZero.Protocol;

namespace MqttZero.Server.Observers;

/// <summary>Describes a disconnected client.</summary>
public sealed class MqttZeroClientDisconnectedContext
{
    /// <summary>Gets the client information.</summary>
    public required MqttZeroClientConnectionInfo Client { get; init; }
    /// <summary>Gets the disconnect type.</summary>
    public MqttZeroDisconnectType DisconnectType { get; init; }
    /// <summary>Gets the MQTT reason code.</summary>
    public MqttReasonCode? ReasonCode { get; init; }
}
