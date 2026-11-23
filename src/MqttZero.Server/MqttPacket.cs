namespace MqttZero;

/// <summary>
/// Represents a decoded MQTT packet.
/// </summary>
public abstract class MqttPacket
{
    /// <summary>
    /// Gets the MQTT control packet type.
    /// </summary>
    public abstract MqttPacketType PacketType { get; }
}
