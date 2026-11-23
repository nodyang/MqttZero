using System.Buffers;
using BenchmarkDotNet.Attributes;

namespace MqttZero.Benchmarks;

/// <summary>
/// Measures core MQTT packet encoding and decoding allocations.
/// </summary>
[MemoryDiagnoser]
public class CodecBenchmarks
{
    private readonly ArrayBufferWriter<byte> _writer = new();
    private ReadOnlySequence<byte> _publishBytes;
    private MqttPublishPacket _publishPacket = null!;

    /// <summary>
    /// Creates the benchmark packet and encoded sequence.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        byte[] payload = new byte[8192];
        _publishPacket = new MqttPublishPacket
        {
            TopicName = new ReadOnlySequence<byte>("benchmark/topic"u8.ToArray()),
            Payload = new ReadOnlySequence<byte>(payload),
            QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce,
            PacketIdentifier = 1
        };
        MqttPacketCodec.Encode(_publishPacket, _writer);
        _publishBytes = new ReadOnlySequence<byte>(_writer.WrittenMemory);
    }

    /// <summary>
    /// Measures MQTT 5 PUBLISH decoding.
    /// </summary>
    /// <returns>The consumed byte count.</returns>
    [Benchmark]
    public long DecodePublish()
    {
        _ = MqttPacketCodec.TryDecode(_publishBytes, out _, out long consumed);
        return consumed;
    }

    /// <summary>
    /// Measures MQTT 5 PUBLISH encoding.
    /// </summary>
    [Benchmark]
    public void EncodePublish()
    {
        _writer.Clear();
        MqttPacketCodec.Encode(_publishPacket, _writer);
    }
}
