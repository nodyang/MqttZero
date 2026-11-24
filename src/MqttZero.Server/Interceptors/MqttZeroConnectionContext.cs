using MqttZero.Events;
using MqttZero.Protocol;
using System.Buffers;

namespace MqttZero.Interceptors;

/// <summary>Contains CONNECT authentication data.</summary>
public sealed class MqttZeroConnectionContext
{
    /// <summary>Gets the resolved client information.</summary>
    public required MqttZeroClientInfo Client { get; init; }
    /// <summary>Gets the raw CONNECT packet.</summary>
    public required MqttConnectPacket ConnectPacket { get; init; }
    /// <summary>Gets the raw password sequence.</summary>
    public ReadOnlySequence<byte> Password => ConnectPacket.Password;
    /// <summary>Gets or sets the rejection reason code.</summary>
    public MqttReasonCode RejectReasonCode { get; set; } = MqttReasonCode.NotAuthorized;
    /// <summary>Gets or sets the optional rejection reason string.</summary>
    public string? ReasonString { get; set; }
}
