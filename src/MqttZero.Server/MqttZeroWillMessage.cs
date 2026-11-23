using System.Buffers;

namespace MqttZero;

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
