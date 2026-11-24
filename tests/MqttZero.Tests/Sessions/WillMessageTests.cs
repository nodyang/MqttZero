using MqttZero.Protocol;
using MqttZero.Server.Sessions;
using MqttZero.Server.Storage;
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
                    WillProperties = new MqttPublishProperties
                    {
                        PayloadFormatIndicator = MqttPayloadFormatIndicator.Utf8,
                        MessageExpiryInterval = 30
                    }
                });

            MqttPublishPacket packet = will.CreatePacket();

            Assert.Equal(30U, packet.Properties.MessageExpiryInterval);
            Assert.Equal(
                MqttPayloadFormatIndicator.Utf8,
                packet.Properties.PayloadFormatIndicator);
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

            Assert.Null(packet.Properties.MessageExpiryInterval);
            Assert.Null(packet.Properties.PayloadFormatIndicator);
        }

        [Fact]
        public void RestoreNormalizesPersistedWillProperties()
        {
            MqttZeroWillMessage will = new(
                new MqttZeroPersistedWill
                {
                    Topic = "will/topic"u8.ToArray(),
                    Payload = "value"u8.ToArray(),
                    Properties = new byte[]
                    {
                        0x18, 0x00, 0x00, 0x00, 0x0C,
                        0x01, 0x01,
                        0x02, 0x00, 0x00, 0x00, 0x1E
                    },
                    DelayInterval = 12,
                    MessageExpiryInterval = 30
                });

            MqttPublishPacket packet = will.CreatePacket();

            Assert.Equal(
                MqttPayloadFormatIndicator.Utf8,
                packet.Properties.PayloadFormatIndicator);
            Assert.Equal(30U, packet.Properties.MessageExpiryInterval);
        }

        [Fact]
        public void RestoreRejectsInvalidPersistedWillProperties()
        {
            Assert.Throws<InvalidOperationException>(
                () => new MqttZeroWillMessage(
                    new MqttZeroPersistedWill
                    {
                        Topic = "will/topic"u8.ToArray(),
                        Payload = "value"u8.ToArray(),
                        Properties = new byte[] { 0x7F }
                    }));
        }
    }
}
