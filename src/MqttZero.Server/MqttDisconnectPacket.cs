using System.Buffers;

namespace MqttZero;

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
    /// Gets or sets the disconnect properties payload section.
    /// </summary>
    public ReadOnlySequence<byte> Properties { get; set; }

    /// <summary>
    /// Gets or sets a session expiry override supplied by the client.
    /// </summary>
    public uint? SessionExpiryInterval { get; set; }
}
