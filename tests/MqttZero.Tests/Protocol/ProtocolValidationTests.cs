using MqttZero.Protocol;
using MqttZero.Tests.Support;
using System.Buffers;

namespace MqttZero.Tests.Protocol
{
    public sealed class ProtocolValidationTests
    {
        [Fact]
        public void FastPublishDecoderRejectsQosThree()
        {
            byte[] bytes =
            [
                0x36, 0x06,
                0x00, 0x01, (byte)'a',
                0x00, 0x01,
                0x00
            ];

            Assert.False(MqttPublishPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out _,
                out _));
        }

        [Fact]
        public void PublishDecoderRejectsInvalidUtf8Topic()
        {
            byte[] bytes =
            [
                0x30, 0x04,
                0x00, 0x01, 0x80,
                0x00
            ];

            Assert.False(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out _,
                out _));
        }

        [Fact]
        public void PublishDecoderRejectsWildcardTopicName()
        {
            byte[] bytes =
            [
                0x30, 0x06,
                0x00, 0x03, (byte)'a', (byte)'/', (byte)'+',
                0x00
            ];

            Assert.False(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out _,
                out _));
        }

        [Fact]
        public void QosOnePublishRejectsZeroPacketIdentifier()
        {
            byte[] bytes =
            [
                0x32, 0x08,
                0x00, 0x03, (byte)'a', (byte)'/', (byte)'b',
                0x00, 0x00,
                0x00
            ];

            Assert.False(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out _,
                out _));
        }

        [Fact]
        public void SubscribeRejectsInvalidWildcardFilter()
        {
            byte[] bytes =
            [
                0x82, 0x0B,
                0x00, 0x01,
                0x00,
                0x00, 0x05, (byte)'a', (byte)'/', (byte)'#', (byte)'/', (byte)'b',
                0x00
            ];

            Assert.False(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out _,
                out _));
        }

        [Fact]
        public void PublishEncoderPreservesMultiSegmentTopic()
        {
            SequenceSegment first = new("devices/"u8.ToArray());
            SequenceSegment second = new("a"u8.ToArray());
            first.SetNext(second);
            ReadOnlySequence<byte> topic = new(
                first,
                0,
                second,
                second.Memory.Length);
            MqttPublishPacket packet = new()
            {
                TopicName = topic,
                Payload = new ReadOnlySequence<byte>("value"u8.ToArray())
            };

            byte[] encoded = ProtocolPacketTestHelper.Encode(packet);

            Assert.True(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(encoded),
                out MqttPacket? decoded,
                out _));
            Assert.Equal(
                "devices/a",
                System.Text.Encoding.UTF8.GetString(
                    Assert.IsType<MqttPublishPacket>(decoded).TopicName.ToArray()));
        }

        [Fact]
        public void ConnectEncoderPreservesMultiSegmentClientId()
        {
            SequenceSegment first = new("client-"u8.ToArray());
            SequenceSegment second = new("id"u8.ToArray());
            first.SetNext(second);
            ReadOnlySequence<byte> clientId = new(
                first,
                0,
                second,
                second.Memory.Length);
            MqttConnectPacket packet = new()
            {
                ClientId = clientId,
                CleanStart = true
            };

            byte[] encoded = ProtocolPacketTestHelper.Encode(packet);

            Assert.True(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(encoded),
                out MqttPacket? decoded,
                out _));
            Assert.Equal(
                "client-id",
                System.Text.Encoding.UTF8.GetString(
                    Assert.IsType<MqttConnectPacket>(decoded).ClientId.ToArray()));
        }

        [Fact]
        public void ConnAckPacketExposesAssignedClientIdentifier()
        {
            Assert.NotNull(typeof(MqttConnAckPacket).GetProperty(
                "AssignedClientIdentifier"));
        }

        [Fact]
        public void PubAckRejectsReasonCodeInvalidForThePacket()
        {
            byte[] bytes = [0x40, 0x04, 0x00, 0x01, 0x01, 0x00];

            Assert.False(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out _,
                out _));
        }

        [Fact]
        public void PubRelRejectsReasonCodeInvalidForThePacket()
        {
            byte[] bytes = [0x62, 0x04, 0x00, 0x01, 0x01, 0x00];

            Assert.False(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out _,
                out _));
        }

        [Fact]
        public void ConnectDecoderRejectsWildcardWillTopic()
        {
            byte[] bytes =
            [
                0x10, 0x16,
                0x00, 0x04, (byte)'M', (byte)'Q', (byte)'T', (byte)'T',
                0x05,
                0x06,
                0x00, 0x00,
                0x00,
                0x00, 0x01, (byte)'c',
                0x00,
                0x00, 0x03, (byte)'a', (byte)'/', (byte)'+',
                0x00, 0x00
            ];

            Assert.False(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out _,
                out _));
        }

        [Fact]
        public void ConnectEncoderRejectsWildcardWillTopic()
        {
            MqttConnectPacket packet = new()
            {
                ClientId = new ReadOnlySequence<byte>("client"u8.ToArray()),
                CleanStart = true,
                HasWill = true,
                WillTopic = new ReadOnlySequence<byte>("will/+"u8.ToArray())
            };

            Assert.Throws<ArgumentException>(
                () => ProtocolPacketTestHelper.Encode(packet));
        }

        [Theory]
        [InlineData((byte)0x03, (ushort)1)]
        [InlineData((byte)0x01, (ushort)0)]
        [InlineData((byte)0x02, (ushort)0)]
        public void PublishEncoderRejectsInvalidQosAndPacketIdentifier(
            byte qos,
            ushort packetIdentifier)
        {
            MqttPublishPacket packet = new()
            {
                TopicName = new ReadOnlySequence<byte>("devices/a"u8.ToArray()),
                Payload = new ReadOnlySequence<byte>("value"u8.ToArray()),
                QualityOfServiceLevel = (MqttQualityOfServiceLevel)qos,
                PacketIdentifier = packetIdentifier
            };

            Assert.Throws<ArgumentException>(
                () => ProtocolPacketTestHelper.Encode(packet));
        }

        [Fact]
        public void SubscriptionAcknowledgementEncodersRejectInvalidReasonCodes()
        {
            MqttSubAckPacket subAck = new()
            {
                PacketIdentifier = 1,
                ReasonCodes = new ReadOnlyMemory<byte>([0x03])
            };
            MqttUnsubAckPacket unsubAck = new()
            {
                PacketIdentifier = 1,
                ReasonCodes = new ReadOnlyMemory<byte>([0x01])
            };

            Assert.Throws<ArgumentException>(
                () => ProtocolPacketTestHelper.Encode(subAck));
            Assert.Throws<ArgumentException>(
                () => ProtocolPacketTestHelper.Encode(unsubAck));
        }
    }
}
