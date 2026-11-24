using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
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

        [Fact]
        public async Task EmptySingleSegmentOwnerExposesEmptyPayload()
        {
            await using MqttZeroPayloadOwner owner =
                MqttZeroPayloadOwnerFactory.CreateSingleSegment(
                    0,
                    static _ => { });

            Assert.True(owner.Payload.IsEmpty);
        }

        [Fact]
        public async Task EmptyMultipleSegmentFactoryProducesEmptyPayload()
        {
            await using MqttZeroPayloadOwner owner =
                await MqttZeroPayloadOwnerFactory.CreateMultipleSegmentAsync(
                    static _ => ValueTask.CompletedTask,
                    TestContext.Current.CancellationToken);

            Assert.True(owner.Payload.IsEmpty);
        }

        [Fact]
        public void SingleSegmentFactoryValidatesArguments()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                MqttZeroPayloadOwnerFactory.CreateSingleSegment(
                    -1,
                    static _ => { }));
            Assert.Throws<ArgumentNullException>(() =>
                MqttZeroPayloadOwnerFactory.CreateSingleSegment(
                    1,
                    null!));
        }

        [Fact]
        public async Task MultipleSegmentFactoryValidatesArguments()
        {
            await Assert.ThrowsAsync<ArgumentNullException>(async () =>
                await MqttZeroPayloadOwnerFactory.CreateMultipleSegmentAsync(
                    null!,
                    TestContext.Current.CancellationToken));
        }

        [Fact]
        public async Task PayloadOwnerDisposeIsIdempotent()
        {
            MqttZeroPayloadOwner owner =
                MqttZeroPayloadOwnerFactory.CreateSingleSegment(
                    1,
                    static payload => payload.Span[0] = 1);

            await owner.DisposeAsync();
            await owner.DisposeAsync();
        }
    }
}
