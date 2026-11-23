using System.Buffers;
using System.IO.Pipelines;

namespace MqttZero.Tests
{
    public sealed class TopicAliasAllocationTests
    {
        [Fact]
        public void ReplacingTopicAliasReusesPooledBufferWithoutAllocation()
        {
            using MqttZeroBufferPool bufferPool = new(1024 * 1024);
            MqttZeroClientConnection connection = new(
                new NullPipeWriter(),
                "memory",
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System,
                bufferPool);
            ReadOnlySequence<byte> topic = new("alias/hot/topic"u8.ToArray());
            connection.SetInboundTopicAlias(1, topic);
            for (int index = 0; index < 10; index++)
            {
                connection.SetInboundTopicAlias(1, topic);
            }

            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int index = 0; index < 1000; index++)
            {
                connection.SetInboundTopicAlias(1, topic);
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.True(connection.TryGetInboundTopicAlias(1, out ReadOnlyMemory<byte> mapped));
            Assert.True(mapped.Span.SequenceEqual(topic.FirstSpan));
            Assert.Equal(0, allocated);
        }

        private sealed class NullPipeWriter : PipeWriter
        {
            public override void Advance(int bytes)
            {
            }

            public override void CancelPendingFlush()
            {
            }

            public override void Complete(Exception? exception = null)
            {
            }

            public override ValueTask CompleteAsync(Exception? exception = null)
            {
                return ValueTask.CompletedTask;
            }

            public override ValueTask<FlushResult> FlushAsync(
                CancellationToken cancellationToken = default)
            {
                return ValueTask.FromResult(new FlushResult(false, false));
            }

            public override Memory<byte> GetMemory(int sizeHint = 0)
            {
                return Memory<byte>.Empty;
            }

            public override Span<byte> GetSpan(int sizeHint = 0)
            {
                return Span<byte>.Empty;
            }

            public override ValueTask<FlushResult> WriteAsync(
                ReadOnlyMemory<byte> source,
                CancellationToken cancellationToken = default)
            {
                return ValueTask.FromResult(new FlushResult(false, false));
            }
        }
    }
}
