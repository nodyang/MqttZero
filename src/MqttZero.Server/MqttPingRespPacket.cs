namespace MqttZero;

/// <summary>
/// Represents an MQTT PINGRESP packet.
/// </summary>
public sealed class MqttPingRespPacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType
    {
        get
        {
            return MqttPacketType.PingResp;
        }
    }
}
