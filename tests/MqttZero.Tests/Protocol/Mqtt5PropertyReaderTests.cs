using MqttZero.Protocol;
using System.Buffers;

namespace MqttZero.Tests.Protocol
{
    public sealed class Mqtt5PropertyReaderTests
    {
        [Fact]
        public void PublishReaderSkipsKnownAndUserProperties()
        {
            byte[] properties =
            [
                0x01, 0x01,
                0x03, 0x00, 0x02, (byte)'j', (byte)'s',
                0x08, 0x00, 0x02, (byte)'r', (byte)'t',
                0x26, 0x00, 0x01, (byte)'k', 0x00, 0x01, (byte)'v'
            ];
            MqttPublishPacket packet = new();

            Assert.True(Mqtt5PropertyReader.TryReadPublish(
                new ReadOnlySequence<byte>(properties),
                packet));
        }

        [Fact]
        public void PublishReaderRejectsDuplicateMessageExpiry()
        {
            byte[] properties =
            [
                0x02, 0x00, 0x00, 0x00, 0x01,
                0x02, 0x00, 0x00, 0x00, 0x02
            ];

            Assert.False(Mqtt5PropertyReader.TryReadPublish(
                new ReadOnlySequence<byte>(properties),
                new MqttPublishPacket()));
        }

        [Fact]
        public void PublishReaderRejectsUnknownProperty()
        {
            Assert.False(Mqtt5PropertyReader.TryReadPublish(
                new ReadOnlySequence<byte>([0x7F]),
                new MqttPublishPacket()));
        }

        [Fact]
        public void ConnAckReaderRejectsInvalidAssignedClientIdentifierUtf8()
        {
            byte[] properties = [0x12, 0x00, 0x02, 0xC0, 0xAF];

            Assert.False(Mqtt5PropertyReader.TryReadConnAck(
                new ReadOnlySequence<byte>(properties),
                new MqttConnAckPacket()));
        }

        [Theory]
        [InlineData("0102")]
        [InlineData("0300016A0300016B")]
        [InlineData("080003612F23")]
        [InlineData("26000161000001FF")]
        public void PublishReaderRejectsInvalidPropertySemantics(string hex)
        {
            Assert.False(Mqtt5PropertyReader.TryReadPublish(
                new ReadOnlySequence<byte>(Convert.FromHexString(hex)),
                new MqttPublishPacket()));
        }

        [Theory]
        [InlineData("1702")]
        [InlineData("2502")]
        [InlineData("2802")]
        [InlineData("2A02")]
        [InlineData("2402")]
        public void ConnAckReaderRejectsInvalidBooleanProperties(string hex)
        {
            Assert.False(Mqtt5PropertyReader.TryReadConnAck(
                new ReadOnlySequence<byte>(Convert.FromHexString(hex)),
                new MqttConnAckPacket()));
        }
    }
}
