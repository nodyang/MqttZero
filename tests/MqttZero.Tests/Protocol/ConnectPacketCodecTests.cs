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
    public sealed class ConnectPacketCodecTests
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
            Assert.Equal(MqttProtocolVersion.V500, connectPacket.ProtocolVersion);
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

            byte[] bytes = ProtocolPacketTestHelper.Encode(packet);

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

            byte[] bytes = ProtocolPacketTestHelper.Encode(packet);
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
        public void RoundTripsConnAckCapabilities()
        {
            MqttConnAckPacket packet = new()
            {
                TopicAliasMaximum = 16,
                ReceiveMaximum = 32,
                MaximumPacketSize = 8192,
                SessionExpiryInterval = 3600
            };

            byte[] bytes = ProtocolPacketTestHelper.Encode(packet);
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
        public void RejectsUnsupportedProtocolVersion()
        {
            byte[] bytes =
            [
                0x10, 0x0E,
                0x00, 0x04, (byte)'M', (byte)'Q', (byte)'T', (byte)'T',
                0x04,
                0x02,
                0x00, 0x3C,
                0x00,
                0x00, 0x01, (byte)'a'
            ];

            Assert.False(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out _,
                out _));
        }

        [Fact]
        public void RoundTripsAssignedClientIdentifier()
        {
            MqttConnAckPacket packet = new()
            {
                AssignedClientIdentifier = "server-client"
            };

            byte[] bytes = ProtocolPacketTestHelper.Encode(packet);
            Assert.True(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(bytes),
                out MqttPacket? decoded,
                out _));

            Assert.Equal(
                "server-client",
                Assert.IsType<MqttConnAckPacket>(decoded).AssignedClientIdentifier);
        }
    }
}
