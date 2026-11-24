namespace MqttZero.Protocol;

/// <summary>
/// Represents an MQTT 5 DISCONNECT packet.
/// </summary>
public sealed class MqttDisconnectPacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType
    {
        get
        {
            return MqttPacketType.Disconnect;
        }
    }

    /// <summary>
    /// Gets or sets the disconnect reason code.
    /// </summary>
    public MqttReasonCode ReasonCode { get; set; }

    /// <summary>
    /// Gets or sets a session expiry override supplied by the client.
    /// </summary>
    public uint? SessionExpiryInterval { get; set; }

    /// <summary>Gets or sets the optional reason string.</summary>
    public string? ReasonString { get; set; }

    /// <summary>Gets or sets the optional server reference.</summary>
    public string? ServerReference { get; set; }

    /// <summary>Gets or sets the ordered disconnect user properties.</summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; set; } =
        Array.Empty<MqttUserProperty>();
}
