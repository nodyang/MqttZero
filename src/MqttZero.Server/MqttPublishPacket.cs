using System.Buffers;

namespace MqttZero;

/// <summary>
/// Represents an MQTT 5 PUBLISH packet.
/// </summary>
public sealed class MqttPublishPacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType
    {
        get
        {
            return MqttPacketType.Publish;
        }
    }

    /// <summary>
    /// Gets or sets the UTF-8 topic name.
    /// </summary>
    public ReadOnlySequence<byte> TopicName { get; set; }

    /// <summary>
    /// Gets or sets the publish properties payload section.
    /// </summary>
    public ReadOnlySequence<byte> Properties { get; set; }

    /// <summary>
    /// Gets or sets the payload sequence.
    /// </summary>
    public ReadOnlySequence<byte> Payload { get; set; }

    /// <summary>
    /// Gets or sets the delivery quality of service.
    /// </summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; set; }

    /// <summary>
    /// Gets or sets the QoS packet identifier.
    /// </summary>
    public ushort PacketIdentifier { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether this is a duplicate delivery.
    /// </summary>
    public bool Duplicate { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the retained flag is set.
    /// </summary>
    public bool Retain { get; set; }
}
