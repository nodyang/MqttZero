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
        Properties = packet.WillProperties.ToArray();
        QualityOfServiceLevel = packet.WillQualityOfServiceLevel;
        Retain = packet.WillRetain;
        DelayInterval = packet.WillDelayInterval;
        MessageExpiryInterval = packet.WillMessageExpiryInterval;
    }

    public MqttZeroWillMessage(MqttZeroPersistedWill will)
    {
        Topic = will.Topic.ToArray();
        Payload = will.Payload.ToArray();
        Properties = will.Properties.ToArray();
        QualityOfServiceLevel = will.QualityOfServiceLevel;
        Retain = will.Retain;
        DelayInterval = will.DelayInterval;
        MessageExpiryInterval = will.MessageExpiryInterval;
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
        MqttPublishPacket packet = new()
        {
            TopicName = new ReadOnlySequence<byte>(Topic),
            Payload = new ReadOnlySequence<byte>(Payload),
            Properties = Properties.Length == 0
                ? ReadOnlySequence<byte>.Empty
                : new ReadOnlySequence<byte>(Properties),
            QualityOfServiceLevel = QualityOfServiceLevel,
            Retain = Retain
        };
        if (MessageExpiryInterval > 0)
        {
            packet.MessageExpiryInterval = MessageExpiryInterval;
        }

        return packet;
    }
}
