using MqttZero.Tests.Support;
using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using System.Buffers;
using System.Text;


namespace MqttZero.Tests.Protocol
{
    public sealed class SubscriptionPacketCodecTests
    {
        [Fact]
        public void RoundTripsSubscriptionUserProperties()
        {
            MqttSubscribePacket subscribe = new()
            {
                PacketIdentifier = 1,
                SubscriptionIdentifier = 77,
                UserProperties = [new MqttUserProperty("tenant", "east")],
                Subscriptions =
                [
                    new MqttSubscriptionRequest
                    {
                        TopicFilter = new ReadOnlySequence<byte>("a/b"u8.ToArray())
                    }
                ]
            };
            MqttUnsubscribePacket unsubscribe = new()
            {
                PacketIdentifier = 2,
                UserProperties = [new MqttUserProperty("tenant", "east")],
                TopicFilters = [new ReadOnlySequence<byte>("a/b"u8.ToArray())]
            };

            foreach (MqttPacket packet in new MqttPacket[] { subscribe, unsubscribe })
            {
                byte[] bytes = ProtocolPacketTestHelper.Encode(packet);
                Assert.True(MqttPacketCodec.TryDecode(
                    new ReadOnlySequence<byte>(bytes),
                    out MqttPacket? decoded,
                    out _));

                IReadOnlyList<MqttUserProperty> properties = decoded switch
                {
                    MqttSubscribePacket value => value.UserProperties,
                    MqttUnsubscribePacket value => value.UserProperties,
                    _ => throw new InvalidOperationException("Unexpected packet type.")
                };
                Assert.Single(properties);
                Assert.Equal("tenant", properties[0].Name);
            }
        }

        [Fact]
        public void RoundTripsSubscriptionAcknowledgementProblemInformation()
        {
            MqttPacket[] packets =
            [
                new MqttSubAckPacket
                {
                    PacketIdentifier = 3,
                    ReasonCodes = new byte[] { (byte)MqttReasonCode.Success },
                    ReasonString = "subscribed",
                    UserProperties = [new MqttUserProperty("stage", "suback")]
                },
                new MqttUnsubAckPacket
                {
                    PacketIdentifier = 4,
                    ReasonCodes = new byte[] { (byte)MqttReasonCode.Success },
                    ReasonString = "unsubscribed",
                    UserProperties = [new MqttUserProperty("stage", "unsuback")]
                }
            ];

            foreach (MqttPacket packet in packets)
            {
                byte[] bytes = ProtocolPacketTestHelper.Encode(packet);
                Assert.True(MqttPacketCodec.TryDecode(
                    new ReadOnlySequence<byte>(bytes),
                    out MqttPacket? decoded,
                    out _));

                (string? reasonString, IReadOnlyList<MqttUserProperty> properties) =
                    decoded switch
                    {
                        MqttSubAckPacket value =>
                            (value.ReasonString, value.UserProperties),
                        MqttUnsubAckPacket value =>
                            (value.ReasonString, value.UserProperties),
                        _ => throw new InvalidOperationException("Unexpected packet type.")
                    };
                Assert.NotNull(reasonString);
                Assert.Single(properties);
            }
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
                        RetainAsPublished = true,
                        RetainHandling =
                            MqttRetainHandling.SendAtSubscribeIfNewSubscriptionOnly
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
            Assert.Equal(
                MqttRetainHandling.SendAtSubscribeIfNewSubscriptionOnly,
                request.RetainHandling);
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

        [Fact]
        public void RejectsInvalidRetainHandling()
        {
            byte[] bytes =
            [
                0x82, 0x07,
                0x00, 0x01,
                0x00,
                0x00, 0x01, (byte)'a',
                0x30
            ];

            Assert.False(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out _,
                out _));
        }

        [Fact]
        public void RoundTripsAllRetainHandlingValues()
        {
            MqttRetainHandling[] expected =
            [
                MqttRetainHandling.SendAtSubscribe,
                MqttRetainHandling.SendAtSubscribeIfNewSubscriptionOnly,
                MqttRetainHandling.DoNotSendOnSubscribe
            ];
            MqttSubscribePacket packet = new()
            {
                PacketIdentifier = 13
            };
            for (int index = 0; index < expected.Length; index++)
            {
                packet.Subscriptions.Add(new MqttSubscriptionRequest
                {
                    TopicFilter = new ReadOnlySequence<byte>(
                        Encoding.UTF8.GetBytes($"retain-{index}")),
                    RetainHandling = expected[index]
                });
            }

            byte[] bytes = ProtocolPacketTestHelper.Encode(packet);
            bool decoded = MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out MqttPacket? result,
                out _);

            MqttSubscribePacket subscribePacket = Assert.IsType<MqttSubscribePacket>(result);
            Assert.True(decoded);
            Assert.Equal(expected.Length, subscribePacket.Subscriptions.Count);
            for (int index = 0; index < expected.Length; index++)
            {
                Assert.Equal(expected[index], subscribePacket.Subscriptions[index].RetainHandling);
            }
        }
    }
}
