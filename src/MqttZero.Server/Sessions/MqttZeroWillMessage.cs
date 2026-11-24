using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Storage;
using System.Buffers;

namespace MqttZero.Server.Sessions;

internal sealed class MqttZeroWillMessage
{
    public MqttZeroWillMessage(MqttConnectPacket packet)
    {
        Topic = packet.WillTopic.ToArray();
        Payload = packet.WillPayload.ToArray();
        Properties = MqttPublishPropertiesCodec.Encode(packet.WillProperties).ToArray();
        QualityOfServiceLevel = packet.WillQualityOfServiceLevel;
        Retain = packet.WillRetain;
        DelayInterval = packet.WillDelayInterval;
        MessageExpiryInterval = packet.WillProperties.MessageExpiryInterval ?? 0;
    }

    public MqttZeroWillMessage(MqttZeroPersistedWill will)
    {
        Topic = will.Topic.ToArray();
        Payload = will.Payload.ToArray();
        MqttConnectPacket persistedProperties = new();
        if (!Mqtt5PropertyReader.TryReadWill(
                new ReadOnlySequence<byte>(will.Properties),
                persistedProperties))
        {
            throw new InvalidOperationException("The persisted MQTT Will properties are invalid.");
        }

        Properties = MqttPublishPropertiesCodec.Encode(
            persistedProperties.WillProperties).ToArray();
        QualityOfServiceLevel = will.QualityOfServiceLevel;
        Retain = will.Retain;
        DelayInterval = will.DelayInterval;
        MessageExpiryInterval = persistedProperties.WillProperties.MessageExpiryInterval ??
            will.MessageExpiryInterval;
    }

    public byte[] Topic { get; }

    public byte[] Payload { get; }

    public byte[] Properties { get; }

    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; }

    public bool Retain { get; }

    public uint DelayInterval { get; }

    public uint MessageExpiryInterval { get; }

    public MqttPublishPacket CreatePacket()
    {
        bool decoded = MqttPublishPropertiesCodec.TryDecode(
            Properties.Length == 0
                ? ReadOnlySequence<byte>.Empty
                : new ReadOnlySequence<byte>(Properties),
            out MqttPublishProperties properties,
            out _,
            out _);
        if (!decoded)
        {
            throw new InvalidOperationException("The MQTT Will properties are invalid.");
        }

        MqttPublishPacket packet = new()
        {
            TopicName = new ReadOnlySequence<byte>(Topic),
            Payload = new ReadOnlySequence<byte>(Payload),
            Properties = properties,
            QualityOfServiceLevel = QualityOfServiceLevel,
            Retain = Retain
        };

        return packet;
    }
}
