using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using MqttZero.Protocol;
using System.Collections.Concurrent;

namespace MqttZero.Benchmarks;

/// <summary>
/// Compares per-connection packet writer allocation with a reusable writer pool.
/// </summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class PacketWriterLifecycleBenchmarks
{
    private const int Iterations = 10_000;
    private readonly ConcurrentQueue<MqttZeroPacketWriter> _pool = new();

    /// <summary>
    /// Gets or sets the maximum number of concurrent writer users.
    /// </summary>
    [Params(1, 128)]
    public int Parallelism { get; set; }

    /// <summary>
    /// Initializes the candidate writer pool.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        _pool.Clear();
        for (int index = 0; index < Parallelism; index++)
        {
            _pool.Enqueue(new MqttZeroPacketWriter());
        }
    }

    /// <summary>
    /// Measures creating one packet writer for every use.
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("WriterLifecycle")]
    public void PerConnection()
    {
        Parallel.For(
            0,
            Iterations,
            new ParallelOptions
            {
                MaxDegreeOfParallelism = Parallelism
            },
            static _ =>
            {
                MqttZeroPacketWriter writer = new();
                UseWriter(writer);
            });
    }

    /// <summary>
    /// Measures reusing packet writers from a concurrent pool.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("WriterLifecycle")]
    public void Pooled()
    {
        Parallel.For(
            0,
            Iterations,
            new ParallelOptions
            {
                MaxDegreeOfParallelism = Parallelism
            },
            _ =>
            {
                if (!_pool.TryDequeue(out MqttZeroPacketWriter? writer))
                {
                    writer = new MqttZeroPacketWriter();
                }

                UseWriter(writer);
                _pool.Enqueue(writer);
            });
    }

    private static void UseWriter(MqttZeroPacketWriter writer)
    {
        writer.Clear();
        Span<byte> span = writer.GetSpan(32);
        span[..32].Fill(0x30);
        writer.Advance(32);
    }
}
