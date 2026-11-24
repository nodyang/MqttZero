using MqttZero.Protocol;
using MqttZero.Server.Sessions;
using System.Buffers;

namespace MqttZero.Tests.Sessions
{
    public sealed class WillMessageTests
    {
        [Fact]
        public void CreatePacketIncludesPositiveMessageExpiry()
        {
            MqttZeroWillMessage will = new(
                new MqttConnectPacket
                {
                    HasWill = true,
                    WillTopic = new ReadOnlySequence<byte>("will/topic"u8.ToArray()),
                    WillPayload = new ReadOnlySequence<byte>("value"u8.ToArray()),
                    WillProperties = new ReadOnlySequence<byte>([0x01, 0x01]),
                    WillMessageExpiryInterval = 30
                });

            MqttPublishPacket packet = will.CreatePacket();

            Assert.Equal(30U, packet.MessageExpiryInterval);
            Assert.False(packet.Properties.IsEmpty);
        }

        [Fact]
        public void CreatePacketOmitsZeroMessageExpiry()
        {
            MqttZeroWillMessage will = new(
                new MqttConnectPacket
                {
                    HasWill = true,
                    WillTopic = new ReadOnlySequence<byte>("will/topic"u8.ToArray()),
                    WillPayload = new ReadOnlySequence<byte>("value"u8.ToArray())
                });

            MqttPublishPacket packet = will.CreatePacket();

            Assert.Null(packet.MessageExpiryInterval);
            Assert.True(packet.Properties.IsEmpty);
        }
    }
}
