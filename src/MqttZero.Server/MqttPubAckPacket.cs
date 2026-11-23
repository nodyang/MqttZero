using System.Buffers;

namespace MqttZero;

/// <summary>
/// Represents an MQTT 5 PUBACK packet.
/// </summary>
public sealed class MqttPubAckPacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType
    {
        get
        {
            return MqttPacketType.PubAck;
        }
    }

    /// <summary>
    /// Gets or sets the acknowledged packet identifier.
    /// </summary>
    public ushort PacketIdentifier { get; set; }

    /// <summary>
    /// Gets or sets the acknowledgement reason code.
    /// </summary>
    public MqttReasonCode ReasonCode { get; set; }

    /// <summary>
    /// Gets or sets the acknowledgement properties payload section.
    /// </summary>
    public ReadOnlySequence<byte> Properties { get; set; }
}
