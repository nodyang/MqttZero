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
    }
}
