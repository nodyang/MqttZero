using System.Buffers;
using BenchmarkDotNet.Attributes;

namespace MqttZero.Benchmarks;

/// <summary>
/// Measures the lower bound for copying PUBLISH payload bytes.
/// </summary>
[MemoryDiagnoser]
public class PayloadCopyBenchmarks
{
    private readonly PreallocatedWriter _destination = new(70 * 1024);
    private byte[] _source = [];

    /// <summary>
    /// Gets or sets the payload size.
    /// </summary>
    [Params(32, 8 * 1024, 64 * 1024)]
    public int PayloadSize { get; set; }

    /// <summary>
    /// Creates the payload fixture.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        _source = new byte[PayloadSize];
    }

    /// <summary>
    /// Measures one payload copy into the destination writer.
    /// </summary>
    /// <returns>The copied byte count.</returns>
    [Benchmark(Baseline = true)]
    public int Copy()
    {
        _destination.Reset();
        Span<byte> destination = _destination.GetSpan(_source.Length);
        _source.CopyTo(destination);
        _destination.Advance(_source.Length);
        return _destination.WrittenCount;
    }

    private sealed class PreallocatedWriter : IBufferWriter<byte>
    {
        private readonly byte[] _buffer;
        private int _written;

        public PreallocatedWriter(int capacity)
        {
            _buffer = new byte[capacity];
        }

        public int WrittenCount
        {
            get
            {
                return _written;
            }
        }

        public void Advance(int count)
        {
            _written += count;
        }

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            return _buffer.AsMemory(_written);
        }

        public Span<byte> GetSpan(int sizeHint = 0)
        {
            return _buffer.AsSpan(_written);
        }

        public void Reset()
        {
            _written = 0;
        }
    }
}
