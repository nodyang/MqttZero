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
    public sealed class SubscriptionPacketCodecTests
    {
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

            byte[] bytes = ProtocolPacketTestHelper.Encode(packet);

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

            byte[] bytes = ProtocolPacketTestHelper.Encode(packet);

            Assert.Equal(
                new byte[] { 0x90, 0x05, 0x00, 0x09, 0x00, 0x01, 0x87 },
                bytes);
        }
        [Fact]
        public void RoundTripsSubscribeSubscriptionIdentifier()
        {
            MqttSubscribePacket packet = new()
            {
                PacketIdentifier = 12,
                SubscriptionIdentifier = 77,
                Subscriptions =
                [
                    new MqttSubscriptionRequest
                    {
                        TopicFilter = new ReadOnlySequence<byte>("devices/#"u8.ToArray())
                    }
                ]
            };

            byte[] bytes = ProtocolPacketTestHelper.Encode(packet);
            bool decoded = MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out MqttPacket? result,
                out long consumed);

            MqttSubscribePacket subscribePacket = Assert.IsType<MqttSubscribePacket>(result);
            Assert.True(decoded);
            Assert.Equal(bytes.Length, consumed);
            Assert.Equal(77U, subscribePacket.SubscriptionIdentifier!.Value);
        }
        [Fact]
        public void RoundTripsUnsubscribeAndUnsubAck()
        {
            MqttUnsubscribePacket unsubscribePacket = new()
            {
                PacketIdentifier = 21,
                TopicFilters = [new ReadOnlySequence<byte>("devices/#"u8.ToArray())]
            };

            byte[] unsubscribeBytes = ProtocolPacketTestHelper.Encode(unsubscribePacket);
            bool decodedUnsubscribe = MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(unsubscribeBytes),
                out MqttPacket? decodedPacket,
                out long consumedUnsubscribe);

            MqttUnsubscribePacket decodedUnsubscribePacket =
                Assert.IsType<MqttUnsubscribePacket>(decodedPacket);
            Assert.True(decodedUnsubscribe);
            Assert.Equal(unsubscribeBytes.Length, consumedUnsubscribe);
            Assert.Equal((ushort)21, decodedUnsubscribePacket.PacketIdentifier);
            Assert.Equal(
                "devices/#",
                Encoding.UTF8.GetString(decodedUnsubscribePacket.TopicFilters[0].FirstSpan));

            MqttUnsubAckPacket unsubAckPacket = new()
            {
                PacketIdentifier = 21,
                ReasonCodes = new byte[] { (byte)MqttReasonCode.Success }
            };
            byte[] unsubAckBytes = ProtocolPacketTestHelper.Encode(unsubAckPacket);
            bool decodedUnsubAck = MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(unsubAckBytes),
                out MqttPacket? decodedAck,
                out long consumedUnsubAck);

            MqttUnsubAckPacket decodedUnsubAckPacket = Assert.IsType<MqttUnsubAckPacket>(decodedAck);
            Assert.True(decodedUnsubAck);
            Assert.Equal(unsubAckBytes.Length, consumedUnsubAck);
            Assert.Equal((ushort)21, decodedUnsubAckPacket.PacketIdentifier);
        }
    }
}
