using System.Buffers;

namespace MqttZero.Protocol;

/// <summary>Represents an MQTT 5 PUBREC packet.</summary>
public sealed class MqttPubRecPacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType => MqttPacketType.PubRec;

    /// <summary>Gets or sets the packet identifier.</summary>
    public ushort PacketIdentifier { get; set; }

    /// <summary>Gets or sets the reason code.</summary>
    public MqttReasonCode ReasonCode { get; set; }

    /// <summary>Gets or sets the raw public properties.</summary>
    public ReadOnlySequence<byte> Properties { get; set; }
}
