using MqttZero.Tests.Support;
using MqttZero.Broker;
using MqttZero.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Routing;
using MqttZero.Sessions;
using System.Buffers;
using System.Text;


namespace MqttZero.Tests.Protocol
{
    public sealed class PublishPacketCodecTests
    {
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

            byte[] bytes = ProtocolPacketTestHelper.Encode(packet);

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
        public void RoundTripsPublishMessageExpiryAndSubscriptionIdentifier()
        {
            MqttPublishPacket packet = new()
            {
                TopicName = new ReadOnlySequence<byte>("expiring/topic"u8.ToArray()),
                Payload = new ReadOnlySequence<byte>("value"u8.ToArray()),
                MessageExpiryInterval = 60,
                SubscriptionIdentifier = 9
            };

            byte[] bytes = ProtocolPacketTestHelper.Encode(packet);
            bool decoded = MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out MqttPacket? result,
                out long consumed);

            MqttPublishPacket publishPacket = Assert.IsType<MqttPublishPacket>(result);
            Assert.True(decoded);
            Assert.Equal(bytes.Length, consumed);
            Assert.Equal(60U, publishPacket.MessageExpiryInterval!.Value);
            Assert.Equal(9U, publishPacket.SubscriptionIdentifier!.Value);
        }
        [Fact]
        public void RoundTripsPublishTopicAlias()
        {
            MqttPublishPacket packet = new()
            {
                TopicName = new ReadOnlySequence<byte>("alias/topic"u8.ToArray()),
                TopicAlias = 3,
                Payload = new ReadOnlySequence<byte>("value"u8.ToArray())
            };

            byte[] bytes = ProtocolPacketTestHelper.Encode(packet);
            bool decoded = MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out MqttPacket? result,
                out _);

            MqttPublishPacket publishPacket = Assert.IsType<MqttPublishPacket>(result);
            Assert.True(decoded);
            Assert.Equal((ushort)3, publishPacket.TopicAlias);
        }
    }
}
