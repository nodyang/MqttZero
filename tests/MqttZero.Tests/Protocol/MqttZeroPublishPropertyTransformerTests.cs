using MqttZero.Protocol;
using MqttZero.Tests.Support;
using System.Buffers;
using System.Buffers.Binary;

namespace MqttZero.Tests.Protocol
{
    public sealed class MqttZeroPublishPropertyTransformerTests
    {
        private static readonly byte[] PayloadFormat = [0x01, 0x01];
        private static readonly byte[] MessageExpiry = [0x02, 0x00, 0x00, 0x00, 0x3C];
        private static readonly byte[] ResponseTopic = [0x08, 0x00, 0x02, (byte)'r', (byte)'t'];
        private static readonly byte[] CorrelationData = [0x09, 0x00, 0x02, 0x11, 0x22];
        private static readonly byte[] UserProperty =
            [0x26, 0x00, 0x01, (byte)'k', 0x00, 0x01, (byte)'v'];

        [Fact]
        public void LengthExcludesMessageExpiryAndKeepsOtherProperties()
        {
            byte[] properties = Combine(
                PayloadFormat,
                MessageExpiry,
                ResponseTopic,
                CorrelationData,
                UserProperty);

            int length = MqttZeroPublishPropertyTransformer
                .GetLengthWithoutMessageExpiry(new ReadOnlySequence<byte>(properties));

            Assert.Equal(
                properties.Length - MessageExpiry.Length,
                length);
        }

        [Fact]
        public void WriteReplacesMessageExpiryInItsOriginalPosition()
        {
            byte[] properties = Combine(
                PayloadFormat,
                MessageExpiry,
                ResponseTopic,
                UserProperty);
            ArrayBufferWriter<byte> writer = new();

            MqttZeroPublishPropertyTransformer.WriteWithoutMessageExpiry(
                writer,
                new ReadOnlySequence<byte>(properties),
                5);

            byte[] expected = Combine(
                PayloadFormat,
                [0x02, 0x00, 0x00, 0x00, 0x05],
                ResponseTopic,
                UserProperty);
            Assert.Equal(expected, writer.WrittenSpan.ToArray());
        }

        [Fact]
        public void WriteAppendsMessageExpiryWhenPropertyWasMissing()
        {
            byte[] properties = Combine(PayloadFormat, CorrelationData);
            Span<byte> destination = stackalloc byte[properties.Length + 5];

            int written = MqttZeroPublishPropertyTransformer.WriteWithoutMessageExpiry(
                destination,
                new ReadOnlySequence<byte>(properties),
                12);

            byte[] expected = Combine(
                properties,
                [0x02, 0x00, 0x00, 0x00, 0x0C]);
            Assert.Equal(expected, destination[..written].ToArray());
        }

        [Fact]
        public void WriteOmitsMessageExpiryWhenNoRemainingIntervalExists()
        {
            byte[] properties = Combine(MessageExpiry, PayloadFormat);
            ArrayBufferWriter<byte> writer = new();

            MqttZeroPublishPropertyTransformer.WriteWithoutMessageExpiry(
                writer,
                new ReadOnlySequence<byte>(properties),
                null);

            Assert.Equal(PayloadFormat, writer.WrittenSpan.ToArray());
        }

        [Fact]
        public void ContainsPropertyHandlesSegmentedVariableLengthProperties()
        {
            SequenceSegment first = new(
                Combine(MessageExpiry, UserProperty).AsMemory(0, 6));
            SequenceSegment second = new(
                Combine(MessageExpiry, UserProperty).AsMemory(6));
            first.SetNext(second);
            ReadOnlySequence<byte> properties = new(
                first,
                0,
                second,
                second.Memory.Length);

            Assert.True(
                MqttZeroPublishPropertyTransformer.ContainsProperty(properties, 0x26));
            Assert.False(
                MqttZeroPublishPropertyTransformer.ContainsProperty(properties, 0x08));
        }

        [Fact]
        public void WriteRejectsMalformedProperties()
        {
            byte[] properties = [0x02, 0x00, 0x00];

            Assert.Throws<ArgumentException>(() =>
                MqttZeroPublishPropertyTransformer.WriteWithoutMessageExpiry(
                    new ArrayBufferWriter<byte>(),
                    new ReadOnlySequence<byte>(properties),
                    1));
        }

        private static byte[] Combine(params byte[][] values)
        {
            int length = values.Sum(static value => value.Length);
            byte[] result = new byte[length];
            int offset = 0;
            foreach (byte[] value in values)
            {
                value.CopyTo(result, offset);
                offset += value.Length;
            }

            return result;
        }
    }
}
