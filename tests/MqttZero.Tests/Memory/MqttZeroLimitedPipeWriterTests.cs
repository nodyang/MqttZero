using MqttZero.Memory;
using System.IO.Pipelines;

namespace MqttZero.Tests.Memory
{
    public sealed class MqttZeroLimitedPipeWriterTests
    {
        [Fact]
        public async Task WriteWithinLimitDelegatesToInnerWriter()
        {
            CapturingPipeWriter inner = new();
            MqttZeroLimitedPipeWriter writer = new(inner, 3);

            await writer.WriteAsync(
                "abc"u8.ToArray(),
                TestContext.Current.CancellationToken);

            Assert.Equal("abc"u8.ToArray(), inner.WrittenBytes);
        }

        [Fact]
        public async Task WriteOverLimitDoesNotReachInnerWriter()
        {
            CapturingPipeWriter inner = new();
            MqttZeroLimitedPipeWriter writer = new(inner, 2);

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await writer.WriteAsync(
                    "abc"u8.ToArray(),
                    TestContext.Current.CancellationToken));

            Assert.Empty(inner.WrittenBytes);
        }

        [Fact]
        public async Task FailedInnerWriteDoesNotConsumeCapacity()
        {
            CapturingPipeWriter inner = new()
            {
                FailNextWrite = true
            };
            MqttZeroLimitedPipeWriter writer = new(inner, 1);

            await Assert.ThrowsAsync<IOException>(async () =>
                await writer.WriteAsync(
                    "a"u8.ToArray(),
                    TestContext.Current.CancellationToken));
            await writer.WriteAsync(
                "b"u8.ToArray(),
                TestContext.Current.CancellationToken);

            Assert.Equal("b"u8.ToArray(), inner.WrittenBytes);
        }

        [Fact]
        public void AdvanceOverRemainingLimitThrows()
        {
            MqttZeroLimitedPipeWriter writer = new(new CapturingPipeWriter(), 2);

            Assert.Throws<InvalidOperationException>(() => writer.Advance(3));
        }

        [Fact]
        public void ConstructorRejectsNullAndNegativeLimit()
        {
            Assert.Throws<ArgumentNullException>(
                () => new MqttZeroLimitedPipeWriter(null!, 1));
            Assert.Throws<ArgumentOutOfRangeException>(
                () => new MqttZeroLimitedPipeWriter(
                    new CapturingPipeWriter(),
                    -1));
        }

        [Fact]
        public async Task FlushCompleteAndCancelForwardToInnerWriter()
        {
            ForwardingPipeWriter inner = new();
            MqttZeroLimitedPipeWriter writer = new(inner, 10);

            _ = await writer.FlushAsync(TestContext.Current.CancellationToken);
            writer.CancelPendingFlush();
            await writer.CompleteAsync();
            writer.Complete();

            Assert.Equal(1, inner.FlushCount);
            Assert.Equal(1, inner.CancelCount);
            Assert.Equal(1, inner.CompleteAsyncCount);
            Assert.Equal(1, inner.CompleteCount);
        }

        [Fact]
        public void MemoryAndSpanRequestsForwardToInnerWriter()
        {
            ForwardingPipeWriter inner = new();
            MqttZeroLimitedPipeWriter writer = new(inner, 10);

            _ = writer.GetMemory(3);
            _ = writer.GetSpan(4);

            Assert.Equal(2, inner.BufferRequestCount);
        }

        [Fact]
        public void ExhaustedLimitRejectsAdditionalBufferRequests()
        {
            ForwardingPipeWriter inner = new();
            MqttZeroLimitedPipeWriter writer = new(inner, 1);
            writer.Advance(1);

            Assert.Throws<InvalidOperationException>(() => writer.GetMemory(1));
            Assert.Throws<InvalidOperationException>(() => writer.GetSpan(1));
        }

        private sealed class CapturingPipeWriter : PipeWriter
        {
            private readonly List<byte> _writtenBytes = [];

            public bool FailNextWrite { get; set; }

            public byte[] WrittenBytes
            {
                get
                {
                    return [.. _writtenBytes];
                }
            }

            public override void Advance(int bytes)
            {
                throw new NotSupportedException();
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
                return new byte[Math.Max(sizeHint, 1)];
            }

            public override Span<byte> GetSpan(int sizeHint = 0)
            {
                return new byte[Math.Max(sizeHint, 1)];
            }

            public override ValueTask<FlushResult> WriteAsync(
                ReadOnlyMemory<byte> source,
                CancellationToken cancellationToken = default)
            {
                if (FailNextWrite)
                {
                    FailNextWrite = false;
                    throw new IOException("The inner writer failed.");
                }

                _writtenBytes.AddRange(source.Span);
                return ValueTask.FromResult(new FlushResult(false, false));
            }
        }

        private sealed class ForwardingPipeWriter : PipeWriter
        {
            private readonly byte[] _buffer = new byte[32];

            public int FlushCount { get; private set; }

            public int CancelCount { get; private set; }

            public int CompleteAsyncCount { get; private set; }

            public int CompleteCount { get; private set; }

            public int BufferRequestCount { get; private set; }

            public override void Advance(int bytes)
            {
            }

            public override void CancelPendingFlush()
            {
                CancelCount++;
            }

            public override void Complete(Exception? exception = null)
            {
                CompleteCount++;
            }

            public override ValueTask CompleteAsync(Exception? exception = null)
            {
                CompleteAsyncCount++;
                return ValueTask.CompletedTask;
            }

            public override ValueTask<FlushResult> FlushAsync(
                CancellationToken cancellationToken = default)
            {
                FlushCount++;
                return ValueTask.FromResult(new FlushResult(false, false));
            }

            public override Memory<byte> GetMemory(int sizeHint = 0)
            {
                BufferRequestCount++;
                return _buffer;
            }

            public override Span<byte> GetSpan(int sizeHint = 0)
            {
                BufferRequestCount++;
                return _buffer;
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
