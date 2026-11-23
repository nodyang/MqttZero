using System.Buffers;

namespace MqttZero.Protocol;

/// <summary>
/// Represents an MQTT 5 SUBACK packet.
/// </summary>
public sealed class MqttSubAckPacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType
    {
        get
        {
            return MqttPacketType.SubAck;
        }
    }

    /// <summary>
    /// Gets or sets the acknowledged packet identifier.
    /// </summary>
    public ushort PacketIdentifier { get; set; }

    /// <summary>
    /// Gets or sets the suback properties payload section.
    /// </summary>
    public ReadOnlySequence<byte> Properties { get; set; }

    /// <summary>
    /// Gets or sets the reason code for each subscription request.
    /// </summary>
    public ReadOnlyMemory<byte> ReasonCodes { get; set; }
}
