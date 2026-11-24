using MqttZero.Protocol;
using System.Buffers;

namespace MqttZero.Tests.Protocol
{
    public sealed class MqttPublishPacketCodecBoundaryTests
    {
        [Fact]
        public void EncodesRawPropertiesAndAppendsMissingScalars()
        {
            MqttZeroPublishView view = new(
                new ReadOnlySequence<byte>("properties/topic"u8.ToArray()),
                new ReadOnlySequence<byte>([0x01, 0x01]),
                new ReadOnlySequence<byte>("value"u8.ToArray()),
                MqttQualityOfServiceLevel.AtMostOnce,
                0,
                false,
                false,
                null,
                9,
                3);
            ArrayBufferWriter<byte> writer = new();

            MqttPublishPacketCodec.Encode(view, writer);

            Assert.True(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(writer.WrittenMemory),
                out MqttPacket? packet,
                out _));
            MqttPublishPacket publish = Assert.IsType<MqttPublishPacket>(packet);
            Assert.Equal(9U, publish.SubscriptionIdentifier);
            Assert.Equal((ushort)3, publish.TopicAlias);
        }

        [Fact]
        public void EncodeToWriterUsesPreencodedHeaderForSmallPayload()
        {
            MqttZeroPublishView view = new(
                new ReadOnlySequence<byte>("small/topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>("value"u8.ToArray()),
                MqttQualityOfServiceLevel.AtLeastOnce,
                7,
                true,
                true,
                null,
                null,
                null);
            ArrayBufferWriter<byte> writer = new();
            MqttZeroPacketWriter headerWriter = new();

            int encodedLength = MqttPublishPacketCodec.EncodeToWriter(
                view,
                headerWriter,
                writer);

            Assert.Equal(writer.WrittenCount, encodedLength);
            Assert.True(MqttPacketCodec.TryDecode(
                new ReadOnlySequence<byte>(writer.WrittenMemory),
                out MqttPacket? packet,
                out _));
            MqttPublishPacket publish = Assert.IsType<MqttPublishPacket>(packet);
            Assert.True(publish.Duplicate);
            Assert.True(publish.Retain);
        }

        [Fact]
        public void EncodedLengthIncludesRewrittenExpiryAndScalars()
        {
            MqttZeroPublishView view = new(
                new ReadOnlySequence<byte>("length/topic"u8.ToArray()),
                new ReadOnlySequence<byte>(
                    [0x02, 0x00, 0x00, 0x00, 0x3C, 0x01, 0x01]),
                new ReadOnlySequence<byte>("value"u8.ToArray()),
                MqttQualityOfServiceLevel.AtMostOnce,
                0,
                false,
                false,
                30,
                5,
                null);

            int length = MqttPublishPacketCodec.GetEncodedLength(view);
            ArrayBufferWriter<byte> writer = new();
            MqttPublishPacketCodec.Encode(view, writer);

            Assert.Equal(writer.WrittenCount, length);
        }
    }
}
