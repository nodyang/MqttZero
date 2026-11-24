using System.IO.Pipelines;

namespace MqttZero.Tests.Support
{
    internal sealed class AllocationPipeWriter : PipeWriter
    {
        private readonly byte[] _buffer;
        private int _written;

        public AllocationPipeWriter(int capacity)
        {
            _buffer = new byte[capacity];
        }

        public int WriteAsyncCount { get; private set; }

        public override void Advance(int bytes)
        {
            _written += bytes;
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

        public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
        {
            Reset();
            return ValueTask.FromResult(new FlushResult(false, false));
        }

        public override Memory<byte> GetMemory(int sizeHint = 0)
        {
            return _buffer.AsMemory(_written);
        }

        public override Span<byte> GetSpan(int sizeHint = 0)
        {
            return _buffer.AsSpan(_written);
        }

        public override ValueTask<FlushResult> WriteAsync(
            ReadOnlyMemory<byte> source,
            CancellationToken cancellationToken = default)
        {
            WriteAsyncCount++;
            source.Span.CopyTo(_buffer.AsSpan(_written));
            _written += source.Length;
            return ValueTask.FromResult(new FlushResult(false, false));
        }

        public void Reset()
        {
            _written = 0;
        }
    }

}
