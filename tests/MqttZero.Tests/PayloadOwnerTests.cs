using System.Buffers;
using System.Text;

namespace MqttZero.Tests
{
    public sealed class PayloadOwnerTests
    {
        [Fact]
        public async Task SingleSegmentFactoryWritesRequestedBytes()
        {
            await using MqttZeroPayloadOwner owner = MqttZeroPayloadOwnerFactory.CreateSingleSegment(
                3,
                static payload => "abc"u8.CopyTo(payload.Span));

            Assert.Equal("abc", Encoding.UTF8.GetString(owner.Payload.FirstSpan));
        }

        [Fact]
        public async Task MultipleSegmentFactoryPreservesChunkBoundaries()
        {
            await using MqttZeroPayloadOwner owner = await MqttZeroPayloadOwnerFactory.CreateMultipleSegmentAsync(
                static async writer =>
                {
                    Memory<byte> first = writer.GetMemory(2);
                    "ab"u8.CopyTo(first.Span);
                    writer.Advance(2);
                    _ = await writer.FlushAsync(TestContext.Current.CancellationToken).ConfigureAwait(false);

                    Memory<byte> second = writer.GetMemory(2);
                    "cd"u8.CopyTo(second.Span);
                    writer.Advance(2);
                    _ = await writer.FlushAsync(TestContext.Current.CancellationToken).ConfigureAwait(false);
                },
                TestContext.Current.CancellationToken);

            Assert.Equal("abcd", Encoding.UTF8.GetString(owner.Payload.ToArray()));
        }
    }
}
