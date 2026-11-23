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

        [Fact]
        public void RoundTripsConnectSessionProperties()
        {
            MqttConnectPacket packet = new()
            {
                ClientId = new ReadOnlySequence<byte>("properties-client"u8.ToArray()),
                CleanStart = false,
                KeepAliveSeconds = 45,
                SessionExpiryInterval = 300,
                ReceiveMaximum = 10,
                MaximumPacketSize = 4096,
                TopicAliasMaximum = 8,
                HasWill = true,
                WillTopic = new ReadOnlySequence<byte>("will/topic"u8.ToArray()),
                WillPayload = new ReadOnlySequence<byte>("offline"u8.ToArray()),
                WillDelayInterval = 12,
                WillQualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce
            };

            byte[] bytes = Encode(packet);
            bool decoded = MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out MqttPacket? result,
                out long consumed);

            MqttConnectPacket connectPacket = Assert.IsType<MqttConnectPacket>(result);
            Assert.True(decoded);
            Assert.Equal(bytes.Length, consumed);
            Assert.False(connectPacket.CleanStart);
            Assert.Equal(300U, connectPacket.SessionExpiryInterval);
            Assert.Equal((ushort)10, connectPacket.ReceiveMaximum);
            Assert.Equal(4096U, connectPacket.MaximumPacketSize);
            Assert.Equal((ushort)8, connectPacket.TopicAliasMaximum);
            Assert.Equal(12U, connectPacket.WillDelayInterval);
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

            byte[] bytes = Encode(packet);
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

            byte[] bytes = Encode(packet);
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

            byte[] unsubscribeBytes = Encode(unsubscribePacket);
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
            byte[] unsubAckBytes = Encode(unsubAckPacket);
            bool decodedUnsubAck = MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(unsubAckBytes),
                out MqttPacket? decodedAck,
                out long consumedUnsubAck);

            MqttUnsubAckPacket decodedUnsubAckPacket = Assert.IsType<MqttUnsubAckPacket>(decodedAck);
            Assert.True(decodedUnsubAck);
            Assert.Equal(unsubAckBytes.Length, consumedUnsubAck);
            Assert.Equal((ushort)21, decodedUnsubAckPacket.PacketIdentifier);
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

            byte[] bytes = Encode(packet);
            bool decoded = MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out MqttPacket? result,
                out _);

            MqttPublishPacket publishPacket = Assert.IsType<MqttPublishPacket>(result);
            Assert.True(decoded);
            Assert.Equal((ushort)3, publishPacket.TopicAlias);
        }

        [Fact]
        public void RoundTripsConnAckCapabilities()
        {
            MqttConnAckPacket packet = new()
            {
                TopicAliasMaximum = 16,
                ReceiveMaximum = 32,
                MaximumPacketSize = 8192,
                SessionExpiryInterval = 3600
            };

            byte[] bytes = Encode(packet);
            bool decoded = MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out MqttPacket? result,
                out _);

            MqttConnAckPacket connAckPacket = Assert.IsType<MqttConnAckPacket>(result);
            Assert.True(decoded);
            Assert.Equal((ushort)16, connAckPacket.TopicAliasMaximum);
            Assert.Equal((ushort)32, connAckPacket.ReceiveMaximum);
            Assert.Equal(8192U, connAckPacket.MaximumPacketSize);
            Assert.Equal(3600U, connAckPacket.SessionExpiryInterval!.Value);
        }

        [Fact]
        public void RejectsDuplicateSessionExpiryProperty()
        {
            byte[] bytes =
            [
                0x10, 0x18,
                0x00, 0x04, (byte)'M', (byte)'Q', (byte)'T', (byte)'T',
                0x05, 0x02, 0x00, 0x3C,
                0x0A,
                0x11, 0x00, 0x00, 0x00, 0x01,
                0x11, 0x00, 0x00, 0x00, 0x02,
                0x00, 0x01, (byte)'a'
            ];

            Assert.False(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out _,
                out _));
        }

        [Fact]
        public void RejectsZeroReceiveMaximum()
        {
            byte[] bytes =
            [
                0x10, 0x11,
                0x00, 0x04, (byte)'M', (byte)'Q', (byte)'T', (byte)'T',
                0x05, 0x02, 0x00, 0x3C,
                0x03, 0x21, 0x00, 0x00,
                0x00, 0x01, (byte)'a'
            ];

            Assert.False(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out _,
                out _));
        }

        [Fact]
        public void ReadsPacketLengthBeforeFullPacketArrives()
        {
            byte[] bytes = [0x30, 0x09, 0x00, 0x03, (byte)'d', (byte)'e', (byte)'v'];

            bool found = MqttPacketCodec.TryGetPacketLength(
                new ReadOnlySequence<byte>(bytes),
                out long packetLength,
                out bool complete);

            Assert.True(found);
            Assert.False(complete);
            Assert.Equal(11, packetLength);
        }

        [Fact]
        public void RoundTripsDisconnectSessionExpiryOverride()
        {
            MqttDisconnectPacket packet = new()
            {
                SessionExpiryInterval = 0
            };

            byte[] bytes = Encode(packet);
            bool decoded = MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out MqttPacket? result,
                out _);

            MqttDisconnectPacket disconnectPacket = Assert.IsType<MqttDisconnectPacket>(result);
            Assert.True(decoded);
            Assert.Equal(0U, disconnectPacket.SessionExpiryInterval!.Value);
        }

        private static byte[] Encode(MqttPacket packet)
        {
            ArrayBufferWriter<byte> writer = new();
            MqttPacketCodec.Encode(packet, writer);
            return writer.WrittenSpan.ToArray();
        }
    }
}
