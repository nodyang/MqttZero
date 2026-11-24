using System.IO.Pipelines;

namespace MqttZero.Memory;

internal sealed class MqttZeroLimitedPipeWriter : PipeWriter
{
    private readonly PipeWriter _inner;
    private readonly int _maximumBytes;
    private int _writtenBytes;

    public MqttZeroLimitedPipeWriter(PipeWriter inner, int maximumBytes)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentOutOfRangeException.ThrowIfNegative(maximumBytes);
        _inner = inner;
        _maximumBytes = maximumBytes;
    }

    public override void Advance(int bytes)
    {
        EnsureCapacity(bytes);
        _inner.Advance(bytes);
        _writtenBytes += bytes;
    }

    public override Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureRemainingCapacity();
        return _inner.GetMemory(sizeHint);
    }

    public override Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureRemainingCapacity();
        return _inner.GetSpan(sizeHint);
    }

    public override ValueTask<FlushResult> FlushAsync(
        CancellationToken cancellationToken = default)
    {
        return _inner.FlushAsync(cancellationToken);
    }

    public override void Complete(Exception? exception = null)
    {
        _inner.Complete(exception);
    }

    public override ValueTask CompleteAsync(Exception? exception = null)
    {
        return _inner.CompleteAsync(exception);
    }

    public override void CancelPendingFlush()
    {
        _inner.CancelPendingFlush();
    }

    public override ValueTask<FlushResult> WriteAsync(
        ReadOnlyMemory<byte> source,
        CancellationToken cancellationToken = default)
    {
        EnsureCapacity(source.Length);
        _writtenBytes += source.Length;
        return _inner.WriteAsync(source, cancellationToken);
    }

    private void EnsureRemainingCapacity()
    {
        if (_writtenBytes >= _maximumBytes)
        {
            throw new InvalidOperationException(
                "The publish payload exceeds the broker maximum packet size.");
        }
    }

    private void EnsureCapacity(int additionalBytes)
    {
        if (additionalBytes > _maximumBytes - _writtenBytes)
        {
            throw new InvalidOperationException(
                "The publish payload exceeds the broker maximum packet size.");
        }
    }
}
