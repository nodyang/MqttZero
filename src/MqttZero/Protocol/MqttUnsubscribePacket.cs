using System.Buffers;

namespace MqttZero.Protocol;

/// <summary>
/// Represents an MQTT 5 UNSUBSCRIBE packet.
/// </summary>
public sealed class MqttUnsubscribePacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType
    {
        get
        {
            return MqttPacketType.Unsubscribe;
        }
    }

    /// <summary>
    /// Gets or sets the packet identifier.
    /// </summary>
    public ushort PacketIdentifier { get; set; }

    /// <summary>Gets or sets the ordered unsubscribe user properties.</summary>
    public IReadOnlyList<MqttUserProperty> UserProperties { get; set; } =
        Array.Empty<MqttUserProperty>();

    /// <summary>
    /// Gets or sets the topic filters to remove.
    /// </summary>
    public List<ReadOnlySequence<byte>> TopicFilters { get; set; } = [];
}
