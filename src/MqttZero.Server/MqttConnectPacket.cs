using System.Buffers;

namespace MqttZero;

/// <summary>
/// Represents an MQTT 5 CONNECT packet.
/// </summary>
public sealed class MqttConnectPacket : MqttPacket
{
    /// <inheritdoc />
    public override MqttPacketType PacketType
    {
        get
        {
            return MqttPacketType.Connect;
        }
    }

    /// <summary>
    /// Gets or sets the negotiated protocol version.
    /// </summary>
    public byte ProtocolVersion { get; set; } = 5;

    /// <summary>
    /// Gets or sets a value indicating whether the client requests a clean session.
    /// </summary>
    public bool CleanStart { get; set; }

    /// <summary>
    /// Gets or sets the keep-alive interval in seconds.
    /// </summary>
    public ushort KeepAliveSeconds { get; set; }

    /// <summary>
    /// Gets or sets the UTF-8 client identifier.
    /// </summary>
    public ReadOnlySequence<byte> ClientId { get; set; }

    /// <summary>
    /// Gets or sets the connect-property payload section.
    /// </summary>
    public ReadOnlySequence<byte> Properties { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether a username is present.
    /// </summary>
    public bool HasUserName { get; set; }

    /// <summary>
    /// Gets or sets the UTF-8 username.
    /// </summary>
    public ReadOnlySequence<byte> UserName { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether a password is present.
    /// </summary>
    public bool HasPassword { get; set; }

    /// <summary>
    /// Gets or sets the password bytes.
    /// </summary>
    public ReadOnlySequence<byte> Password { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether a will message is present.
    /// </summary>
    public bool HasWill { get; set; }

    /// <summary>
    /// Gets or sets the will QoS.
    /// </summary>
    public MqttQualityOfServiceLevel WillQualityOfServiceLevel { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the will message is retained.
    /// </summary>
    public bool WillRetain { get; set; }

    /// <summary>
    /// Gets or sets the will property payload section.
    /// </summary>
    public ReadOnlySequence<byte> WillProperties { get; set; }

    /// <summary>
    /// Gets or sets the UTF-8 will topic.
    /// </summary>
    public ReadOnlySequence<byte> WillTopic { get; set; }

    /// <summary>
    /// Gets or sets the will payload.
    /// </summary>
    public ReadOnlySequence<byte> WillPayload { get; set; }
}
