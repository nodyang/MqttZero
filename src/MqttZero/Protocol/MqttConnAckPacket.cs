using System.Buffers;

namespace MqttZero.Protocol;

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

    /// <summary>
    /// Gets or sets the maximum inbound topic alias accepted by the server.
    /// </summary>
    public ushort? TopicAliasMaximum { get; set; }

    /// <summary>
    /// Gets or sets the server receive maximum.
    /// </summary>
    public ushort? ReceiveMaximum { get; set; }

    /// <summary>
    /// Gets or sets the maximum packet size accepted by the server.
    /// </summary>
    public uint? MaximumPacketSize { get; set; }

    /// <summary>
    /// Gets or sets the optional reason string.
    /// </summary>
    public string? ReasonString { get; set; }

    /// <summary>
    /// Gets or sets the session expiry interval accepted by the server.
    /// </summary>
    public uint? SessionExpiryInterval { get; set; }
}
