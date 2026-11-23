using System.Buffers;

namespace MqttZero;

/// <summary>
/// Represents an MQTT 5 SUBSCRIBE packet.
/// </summary>
public sealed class MqttSubscribePacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType
    {
        get
        {
            return MqttPacketType.Subscribe;
        }
    }

    /// <summary>
    /// Gets or sets the packet identifier.
    /// </summary>
    public ushort PacketIdentifier { get; set; }

    /// <summary>
    /// Gets or sets the subscribe properties payload section.
    /// </summary>
    public ReadOnlySequence<byte> Properties { get; set; }

    /// <summary>
    /// Gets or sets the requested subscriptions.
    /// </summary>
    public List<MqttSubscriptionRequest> Subscriptions { get; set; } = [];
}
