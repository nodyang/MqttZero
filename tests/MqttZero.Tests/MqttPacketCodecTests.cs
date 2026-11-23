using System.Buffers;
using System.Text;

namespace MqttZero.Tests
{
    public sealed class MqttPacketCodecTests
    {
        [Fact]
        public void DecodesMqtt5ConnectPacket()
        {
            byte[] bytes =
            [
                0x10, 0x10,
                0x00, 0x04, (byte)'M', (byte)'Q', (byte)'T', (byte)'T',
                0x05,
                0x02,
                0x00, 0x3C,
                0x00,
                0x00, 0x03, (byte)'a', (byte)'b', (byte)'c'
            ];

            bool decoded = MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out MqttPacket? packet,
                out long consumed);

            MqttConnectPacket connectPacket = Assert.IsType<MqttConnectPacket>(packet);
            Assert.True(decoded);
            Assert.Equal(bytes.Length, consumed);
            Assert.Equal(5, connectPacket.ProtocolVersion);
            Assert.True(connectPacket.CleanStart);
            Assert.Equal(60, connectPacket.KeepAliveSeconds);
            Assert.Equal("abc", Encoding.UTF8.GetString(connectPacket.ClientId.FirstSpan));
        }

        [Fact]
        public void RoundTripsMqtt5ConnectPacket()
        {
            MqttConnectPacket packet = new()
            {
                ClientId = new ReadOnlySequence<byte>("client-1"u8.ToArray()),
                CleanStart = true,
                KeepAliveSeconds = 30
            };

            byte[] bytes = Encode(packet);

            bool decoded = MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out MqttPacket? result,
                out long consumed);

            MqttConnectPacket connectPacket = Assert.IsType<MqttConnectPacket>(result);
            Assert.True(decoded);
            Assert.Equal(bytes.Length, consumed);
            Assert.Equal("client-1", Encoding.UTF8.GetString(connectPacket.ClientId.FirstSpan));
            Assert.True(connectPacket.CleanStart);
            Assert.Equal(30, connectPacket.KeepAliveSeconds);
        }

        [Fact]
        public void DecodesQos0PublishWithoutCopyingPayloadIntoAnArray()
        {
            byte[] bytes =
            [
                0x30, 0x09,
                0x00, 0x03, (byte)'d', (byte)'e', (byte)'v',
                0x00,
                (byte)'p', (byte)'a', (byte)'y'
            ];

            bool decoded = MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out MqttPacket? packet,
                out long consumed);

            MqttPublishPacket publishPacket = Assert.IsType<MqttPublishPacket>(packet);
            Assert.True(decoded);
            Assert.Equal(bytes.Length, consumed);
            Assert.Equal("dev", Encoding.UTF8.GetString(publishPacket.TopicName.FirstSpan));
            Assert.Equal("pay", Encoding.UTF8.GetString(publishPacket.Payload.FirstSpan));
            Assert.Equal(MqttQualityOfServiceLevel.AtMostOnce, publishPacket.QualityOfServiceLevel);
        }

        [Fact]
        public void RoundTripsQos1PublishPacket()
        {
            MqttPublishPacket packet = new()
            {
                TopicName = new ReadOnlySequence<byte>("devices/a"u8.ToArray()),
                Payload = new ReadOnlySequence<byte>("hello"u8.ToArray()),
                QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce,
                PacketIdentifier = 42
            };

            byte[] bytes = Encode(packet);

            bool decoded = MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out MqttPacket? result,
                out long consumed);

            MqttPublishPacket publishPacket = Assert.IsType<MqttPublishPacket>(result);
            Assert.True(decoded);
            Assert.Equal(bytes.Length, consumed);
            Assert.Equal("devices/a", Encoding.UTF8.GetString(publishPacket.TopicName.FirstSpan));
            Assert.Equal("hello", Encoding.UTF8.GetString(publishPacket.Payload.FirstSpan));
            Assert.Equal(MqttQualityOfServiceLevel.AtLeastOnce, publishPacket.QualityOfServiceLevel);
            Assert.Equal((ushort)42, publishPacket.PacketIdentifier);
        }

        [Fact]
        public void RoundTripsSubscribePacketWithOptions()
        {
            MqttSubscribePacket packet = new()
            {
                PacketIdentifier = 7,
                Subscriptions =
                [
                    new MqttSubscriptionRequest
                    {
                        TopicFilter = new ReadOnlySequence<byte>("devices/+"u8.ToArray()),
                        QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce,
                        NoLocal = true,
                        RetainAsPublished = true
                    }
                ]
            };

            byte[] bytes = Encode(packet);

            bool decoded = MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out MqttPacket? result,
                out long consumed);

            MqttSubscribePacket subscribePacket = Assert.IsType<MqttSubscribePacket>(result);
            Assert.True(decoded);
            Assert.Equal(bytes.Length, consumed);
            Assert.Equal((ushort)7, subscribePacket.PacketIdentifier);
            MqttSubscriptionRequest request = Assert.Single(subscribePacket.Subscriptions);
            Assert.Equal("devices/+", Encoding.UTF8.GetString(request.TopicFilter.FirstSpan));
            Assert.Equal(MqttQualityOfServiceLevel.AtLeastOnce, request.QualityOfServiceLevel);
            Assert.True(request.NoLocal);
            Assert.True(request.RetainAsPublished);
        }

        [Fact]
        public void DecodesShortQos1PubAck()
        {
            byte[] bytes = [0x40, 0x02, 0x00, 0x2A];

            bool decoded = MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out MqttPacket? packet,
                out long consumed);

            MqttPubAckPacket pubAckPacket = Assert.IsType<MqttPubAckPacket>(packet);
            Assert.True(decoded);
            Assert.Equal(bytes.Length, consumed);
            Assert.Equal((ushort)42, pubAckPacket.PacketIdentifier);
            Assert.Equal(MqttReasonCode.Success, pubAckPacket.ReasonCode);
        }

        [Fact]
        public void EncodesSubAckReasonCodes()
        {
            MqttSubAckPacket packet = new()
            {
                PacketIdentifier = 9,
                ReasonCodes = new byte[]
                {
                    (byte)MqttReasonCode.GrantedQos1,
                    (byte)MqttReasonCode.NotAuthorized
                }
            };

            byte[] bytes = Encode(packet);

            Assert.Equal(
                new byte[] { 0x90, 0x05, 0x00, 0x09, 0x00, 0x01, 0x87 },
                bytes);
        }

        private static byte[] Encode(MqttPacket packet)
        {
            ArrayBufferWriter<byte> writer = new();
            MqttPacketCodec.Encode(packet, writer);
            return writer.WrittenSpan.ToArray();
        }
    }
}
