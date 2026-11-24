using BenchmarkDotNet.Attributes;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Sessions;

namespace MqttZero.Benchmarks;

/// <summary>
/// Measures pending message queue enqueue and dequeue operations.
/// </summary>
[MemoryDiagnoser]
public class QueueBenchmarks
{
    private MqttZeroMessageLease _lease = null!;
    private MqttZeroQueuedMessage[] _messages = [];
    private MqttZeroMessageQueue _queue = null!;

    /// <summary>
    /// Gets or sets the queue depth.
    /// </summary>
    [Params(1, 1000)]
    public int Depth { get; set; }

    /// <summary>
    /// Creates pooled queue messages.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        _lease = MqttZeroMessageLease.CopyFrom(
            new System.Buffers.ReadOnlySequence<byte>("hot/topic"u8.ToArray()),
            System.Buffers.ReadOnlySequence<byte>.Empty,
            new System.Buffers.ReadOnlySequence<byte>(new byte[1024]),
            MqttQualityOfServiceLevel.AtMostOnce,
            false,
            null,
            null);
        _messages = new MqttZeroQueuedMessage[Depth];
        _queue = new MqttZeroMessageQueue(Depth);
        for (int index = 0; index < Depth; index++)
        {
            _messages[index] = MqttZeroQueuedMessage.Rent(
                _lease,
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                null,
                null);
        }
    }

    /// <summary>
    /// Releases pooled messages.
    /// </summary>
    [GlobalCleanup]
    public void Cleanup()
    {
        foreach (MqttZeroQueuedMessage message in _messages)
        {
            message.Release();
        }

        _lease.Release();
    }

    /// <summary>
    /// Measures a full enqueue and dequeue cycle.
    /// </summary>
    /// <returns>The number of dequeued messages.</returns>
    [Benchmark]
    public int EnqueueDequeue()
    {
        for (int index = 0; index < Depth; index++)
        {
            _queue.Add(_messages[index]);
        }

        for (int index = 0; index < Depth; index++)
        {
            _ = _queue.Dequeue();
        }

        return Depth;
    }
}
