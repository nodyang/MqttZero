namespace MqttZero.Protocol;

/// <summary>
/// Represents an MQTT PINGREQ packet.
/// </summary>
public sealed class MqttPingReqPacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType
    {
        get
        {
            return MqttPacketType.PingReq;
        }
    }
}
