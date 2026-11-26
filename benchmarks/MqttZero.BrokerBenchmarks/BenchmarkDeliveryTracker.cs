using System.Buffers;
using System.Diagnostics;

namespace MqttZero.BrokerBenchmarks;

/// <summary>
/// Tracks delivery integrity and delivery latency for one subscriber.
/// </summary>
internal sealed class BenchmarkDeliveryTracker
{
    private MqttZeroLatencyHistogram? _latency;
    private long _maxSequence;
    private long _receivedMessages;
    private long _sequenceRegressions;
    private long _unsequencedMessages;

    public BenchmarkDeliveryTracker(
        MqttZeroLatencyHistogram? latency = null)
    {
        _latency = latency;
    }

    public void AttachLatency(MqttZeroLatencyHistogram? latency)
    {
        Volatile.Write(ref _latency, latency);
    }

    public long ReceivedMessages
    {
        get
        {
            return Interlocked.Read(ref _receivedMessages);
        }
    }

    public long MaxSequence
    {
        get
        {
            return Interlocked.Read(ref _maxSequence);
        }
    }

    public long SequenceRegressions
    {
        get
        {
            return Interlocked.Read(ref _sequenceRegressions);
        }
    }

    public long UnsequencedMessages
    {
        get
        {
            return Interlocked.Read(ref _unsequencedMessages);
        }
    }

    public void OnMessageReceived(ReadOnlySequence<byte> payload)
    {
        Interlocked.Increment(ref _receivedMessages);
        Span<byte> header = stackalloc byte[BenchmarkDeliveryEnvelope.HeaderLength];
        if (payload.Length < BenchmarkDeliveryEnvelope.HeaderLength)
        {
            Interlocked.Increment(ref _unsequencedMessages);
            return;
        }

        payload.Slice(0, BenchmarkDeliveryEnvelope.HeaderLength)
            .CopyTo(header);
        if (!BenchmarkDeliveryEnvelope.TryRead(
                header,
                out long sequence,
                out long publishTimestamp))
        {
            Interlocked.Increment(ref _unsequencedMessages);
            return;
        }

        while (true)
        {
            long current = Volatile.Read(ref _maxSequence);
            if (sequence <= current)
            {
                Interlocked.Increment(ref _sequenceRegressions);
                break;
            }

            if (Interlocked.CompareExchange(
                ref _maxSequence,
                sequence,
                current) == current)
            {
                break;
            }
        }

        MqttZeroLatencyHistogram? latency = Volatile.Read(ref _latency);
        if (latency != null)
        {
            long now = Stopwatch.GetTimestamp();
            if (publishTimestamp > 0 &&
                now > publishTimestamp)
            {
                latency.RecordTicks(now - publishTimestamp);
            }
        }
    }
}
