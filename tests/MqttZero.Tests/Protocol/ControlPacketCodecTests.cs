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
    public sealed class ControlPacketCodecTests
    {
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
        public void RoundTripsQos2AcknowledgementPackets()
        {
            MqttPacket[] packets =
            [
                new MqttPubRecPacket
                {
                    PacketIdentifier = 11,
                    ReasonCode = MqttReasonCode.PacketIdentifierInUse
                },
                new MqttPubRelPacket
                {
                    PacketIdentifier = 12
                },
                new MqttPubCompPacket
                {
                    PacketIdentifier = 13,
                    ReasonCode = MqttReasonCode.PacketIdentifierNotFound
                }
            ];

            foreach (MqttPacket packet in packets)
            {
                byte[] bytes = ProtocolPacketTestHelper.Encode(packet);
                Assert.True(MqttPacketCodec.TryDecode(
                    new ReadOnlySequence<byte>(bytes),
                    out MqttPacket? decoded,
                    out long consumed));
                Assert.Equal(bytes.Length, consumed);
                Assert.Equal(packet.PacketType, decoded!.PacketType);
            }
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

            byte[] bytes = ProtocolPacketTestHelper.Encode(packet);
            bool decoded = MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out MqttPacket? result,
                out _);

            MqttDisconnectPacket disconnectPacket = Assert.IsType<MqttDisconnectPacket>(result);
            Assert.True(decoded);
            Assert.Equal(0U, disconnectPacket.SessionExpiryInterval!.Value);
        }

        [Theory]
        [InlineData(0x40)]
        [InlineData(0x50)]
        [InlineData(0x62)]
        [InlineData(0x70)]
        public void AcknowledgementDecodersRejectZeroPacketIdentifier(byte firstByte)
        {
            byte[] bytes = [firstByte, 0x02, 0x00, 0x00];

            Assert.False(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out _,
                out _));
        }

        [Fact]
        public void AcknowledgementEncodersRejectZeroPacketIdentifier()
        {
            MqttPacket[] packets =
            [
                new MqttPubAckPacket(),
                new MqttPubRecPacket(),
                new MqttPubRelPacket(),
                new MqttPubCompPacket()
            ];

            foreach (MqttPacket packet in packets)
            {
                Assert.Throws<ArgumentException>(
                    () => ProtocolPacketTestHelper.Encode(packet));
            }
        }

        [Theory]
        [InlineData("2003020000")]
        [InlineData("2003018000")]
        [InlineData("2003000100")]
        public void ConnAckDecoderRejectsInvalidFlagsOrReasonCode(string hex)
        {
            byte[] bytes = Convert.FromHexString(hex);
            Assert.False(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out _,
                out _));
        }

        [Theory]
        [InlineData("900400000000")]
        [InlineData("900400010003")]
        [InlineData("9003000100")]
        [InlineData("B00400000000")]
        [InlineData("B00400010001")]
        [InlineData("B003000100")]
        public void SubscriptionAcknowledgementDecodersRejectInvalidPackets(string hex)
        {
            byte[] bytes = Convert.FromHexString(hex);
            Assert.False(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out _,
                out _));
        }
    }
}
