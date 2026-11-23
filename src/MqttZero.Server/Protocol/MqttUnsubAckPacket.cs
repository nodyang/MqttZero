using System.Buffers;

namespace MqttZero.Protocol;

/// <summary>
/// Represents an MQTT 5 UNSUBACK packet.
/// </summary>
public sealed class MqttUnsubAckPacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType
    {
        get
        {
            return MqttPacketType.UnsubAck;
        }
    }

    /// <summary>
    /// Gets or sets the packet identifier.
    /// </summary>
    public ushort PacketIdentifier { get; set; }

    /// <summary>
    /// Gets or sets the UNSUBACK properties payload section.
    /// </summary>
    public ReadOnlySequence<byte> Properties { get; set; }

    /// <summary>
    /// Gets or sets one reason code per unsubscribed filter.
    /// </summary>
    public ReadOnlyMemory<byte> ReasonCodes { get; set; }
}
