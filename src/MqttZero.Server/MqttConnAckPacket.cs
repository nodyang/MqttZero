using System.Buffers;

namespace MqttZero;

/// <summary>
/// Represents an MQTT 5 CONNACK packet.
/// </summary>
public sealed class MqttConnAckPacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType
    {
        get
        {
            return MqttPacketType.ConnAck;
        }
    }

    /// <summary>
    /// Gets or sets a value indicating whether the server restored an existing session.
    /// </summary>
    public bool SessionPresent { get; set; }

    /// <summary>
    /// Gets or sets the connection reason code.
    /// </summary>
    public MqttReasonCode ReasonCode { get; set; }

    /// <summary>
    /// Gets or sets the CONNACK properties payload section.
    /// </summary>
    public ReadOnlySequence<byte> Properties { get; set; }
}
