using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Sessions;
using System.Buffers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using MQTTnet.Adapter;
using MQTTnet.Formatter;
using MQTTnet.Formatter.V5;
using MqttNetPublishPacket = MQTTnet.Packets.MqttPublishPacket;

namespace MqttZero.Benchmarks;

/// <summary>
/// Compares MQTTZero and MQTTnet MQTT 5 PUBLISH codec throughput and allocation.
/// </summary>
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class CodecBenchmarks
{
    private readonly MqttBufferWriter _mqttNetWriter = new(70 * 1024, 70 * 1024);
    private readonly PreallocatedBufferWriter _mqttZeroWriter = new(70 * 1024);
    private readonly MqttZeroPacketWriter _mqttZeroHeaderWriter = new();

    private MqttZeroMessageLease _lease = null!;
    private MqttZeroQueuedMessage _queuedMessage = null!;
    private MqttPublishPacket _mqttZeroPacket = null!;
    private ReadOnlySequence<byte> _mqttZeroDecodeSource;
    private MqttV5PacketDecoder _mqttNetDecoder = null!;
    private MqttV5PacketEncoder _mqttNetEncoder = null!;
    private MqttNetPublishPacket _mqttNetPacket = null!;
    private ReceivedMqttPacket _receivedMqttNetPacket;

    /// <summary>
    /// Gets or sets the PUBLISH payload size.
    /// </summary>
    [Params(32, 8 * 1024, 64 * 1024)]
    public int PayloadSize { get; set; }

    /// <summary>
    /// Gets or sets the number of segments presented to the MQTTZero decoder.
    /// </summary>
    [Params(1, 10)]
    public int Segments { get; set; }

    /// <summary>
    /// Creates packet and codec fixtures.
    /// </summary>
    [GlobalSetup]
    public void Setup()
    {
        byte[] payload = new byte[PayloadSize];
        _mqttZeroPacket = new MqttPublishPacket
        {
            TopicName = new ReadOnlySequence<byte>("benchmark/topic"u8.ToArray()),
            Payload = new ReadOnlySequence<byte>(payload),
            QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce,
            PacketIdentifier = 1,
            MessageExpiryInterval = 60,
            SubscriptionIdentifier = 7
        };
        ArrayBufferWriter<byte> setupWriter = new();
        MqttPacketCodec.Encode(_mqttZeroPacket, setupWriter);
        byte[] encoded = setupWriter.WrittenSpan.ToArray();
        _mqttZeroDecodeSource = CreateSegmentedSequence(encoded, Segments);
        _lease = MqttZeroMessageLease.CopyFrom(
            _mqttZeroPacket.TopicName,
            _mqttZeroPacket.Properties,
            _mqttZeroPacket.Payload,
            _mqttZeroPacket.QualityOfServiceLevel,
            false,
            _mqttZeroPacket.MessageExpiryInterval,
            _mqttZeroPacket.SubscriptionIdentifier);
        _queuedMessage = MqttZeroQueuedMessage.Rent(
            _lease,
            MqttQualityOfServiceLevel.AtLeastOnce,
            false,
            _mqttZeroPacket.SubscriptionIdentifier,
            null);

        _mqttNetPacket = new MqttNetPublishPacket
        {
            Topic = "benchmark/topic",
            Payload = new ReadOnlySequence<byte>(payload),
            QualityOfServiceLevel = MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
            PacketIdentifier = 1,
            MessageExpiryInterval = 60,
            SubscriptionIdentifiers = [7]
        };
        _mqttNetEncoder = new MqttV5PacketEncoder(_mqttNetWriter);
        MqttPacketBuffer encodedNet = _mqttNetEncoder.Encode(_mqttNetPacket);
        ReadOnlySequence<byte> mqttNetDecodeSource = new(
            encodedNet.Packet.AsMemory().ToArray().Concat(encodedNet.Payload.ToArray()).ToArray());
        _receivedMqttNetPacket = CreateReceivedPacket(mqttNetDecodeSource.FirstSpan);
        _mqttNetDecoder = new MqttV5PacketDecoder();
    }

    /// <summary>
    /// Releases pooled codec fixtures.
    /// </summary>
    [GlobalCleanup]
    public void Cleanup()
    {
        _queuedMessage.Release();
        _lease.Release();
    }

    /// <summary>
    /// Measures generic MQTTZero PUBLISH decoding.
    /// </summary>
    /// <returns>The consumed byte count.</returns>
    [Benchmark]
    [BenchmarkCategory("Decode")]
    public long DecodeGeneric()
    {
        _ = MqttPacketCodec.TryDecode(
            _mqttZeroDecodeSource,
            out _,
            out long consumed);
        return consumed;
    }

    /// <summary>
    /// Measures fast MQTTZero PUBLISH decoding.
    /// </summary>
    /// <returns>The payload and consumed byte count.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Decode")]
    public long DecodeFast()
    {
        _ = MqttPublishPacketCodec.TryDecode(
            _mqttZeroDecodeSource,
            out MqttZeroPublishView view,
            out long consumed);
        return view.Payload.Length + consumed;
    }

    /// <summary>
    /// Measures MQTTnet MQTT 5 PUBLISH decoding.
    /// </summary>
    /// <returns>The decoded packet payload length.</returns>
    [Benchmark]
    [BenchmarkCategory("Decode")]
    public long DecodeMqttNet()
    {
        MqttNetPublishPacket packet =
            (MqttNetPublishPacket)_mqttNetDecoder.Decode(_receivedMqttNetPacket);
        return packet.Payload.Length;
    }

    private static ReceivedMqttPacket CreateReceivedPacket(ReadOnlySpan<byte> bytes)
    {
        byte fixedHeader = bytes[0];
        int remainingLengthOffset = 1;
        int multiplier = 1;
        int remainingLength = 0;
        byte encoded;
        do
        {
            encoded = bytes[remainingLengthOffset++];
            remainingLength += (encoded & 0x7F) * multiplier;
            multiplier *= 128;
        }
        while ((encoded & 0x80) != 0);

        ArraySegment<byte> body = new(bytes[remainingLengthOffset..].ToArray());
        return new ReceivedMqttPacket(
            fixedHeader,
            body,
            remainingLengthOffset + remainingLength);
    }

    /// <summary>
    /// Measures fast MQTTZero PUBLISH encoding.
    /// </summary>
    /// <returns>The encoded packet length.</returns>
    [Benchmark]
    [BenchmarkCategory("Encode")]
    public int EncodeToWriterFast()
    {
        _mqttZeroWriter.Reset();
        MqttPublishPacketCodec.EncodeToWriter(
            _queuedMessage.CreateView(false),
            _mqttZeroHeaderWriter,
            _mqttZeroWriter);
        return _mqttZeroWriter.WrittenCount;
    }

    /// <summary>
    /// Measures MQTTnet MQTT 5 PUBLISH encoding.
    /// </summary>
    /// <returns>The encoded packet length.</returns>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Encode")]
    public int EncodeHeaderMqttNet()
    {
        _mqttNetWriter.Reset(0);
        MqttPacketBuffer packet = _mqttNetEncoder.Encode(_mqttNetPacket);
        return packet.Length;
    }

    /// <summary>
    /// Measures MQTTZero PUBLISH header encoding without materializing the payload.
    /// </summary>
    /// <returns>The encoded header length.</returns>
    [Benchmark]
    [BenchmarkCategory("Encode")]
    public int EncodeHeaderFast()
    {
        MqttZeroPublishPacketBuffer buffer = MqttPublishPacketCodec.EncodeHeader(
            _queuedMessage.CreateView(false),
            _mqttZeroHeaderWriter);
        return buffer.Header.Length;
    }

    /// <summary>
    /// Measures MQTTnet encoding with the payload materialized into the same writer.
    /// </summary>
    /// <returns>The complete encoded length.</returns>
    [Benchmark]
    [BenchmarkCategory("Encode")]
    public int EncodeToWriterMqttNet()
    {
        _mqttZeroWriter.Reset();
        _mqttNetWriter.Reset(0);
        MqttPacketBuffer packet = _mqttNetEncoder.Encode(_mqttNetPacket);
        packet.Packet.AsSpan().CopyTo(_mqttZeroWriter.GetSpan(packet.Packet.Count));
        _mqttZeroWriter.Advance(packet.Packet.Count);
        foreach (ReadOnlyMemory<byte> segment in packet.Payload)
        {
            segment.Span.CopyTo(_mqttZeroWriter.GetSpan(segment.Length));
            _mqttZeroWriter.Advance(segment.Length);
        }

        return _mqttZeroWriter.WrittenCount;
    }

    private static ReadOnlySequence<byte> CreateSegmentedSequence(byte[] bytes, int segmentCount)
    {
        if (segmentCount == 1)
        {
            return new ReadOnlySequence<byte>(bytes);
        }

        int segmentLength = (bytes.Length + segmentCount - 1) / segmentCount;
        SequenceSegment? first = null;
        SequenceSegment? last = null;
        for (int offset = 0; offset < bytes.Length; offset += segmentLength)
        {
            int length = Math.Min(segmentLength, bytes.Length - offset);
            SequenceSegment segment = new(bytes.AsMemory(offset, length));
            if (first == null)
            {
                first = segment;
            }
            else
            {
                last!.SetNext(segment);
            }

            last = segment;
        }

        return new ReadOnlySequence<byte>(first!, 0, last!, last!.Memory.Length);
    }

    private sealed class SequenceSegment : ReadOnlySequenceSegment<byte>
    {
        public SequenceSegment(ReadOnlyMemory<byte> memory)
        {
            Memory = memory;
        }

        public void SetNext(SequenceSegment next)
        {
            next.RunningIndex = RunningIndex + Memory.Length;
            Next = next;
        }
    }

    private sealed class PreallocatedBufferWriter : IBufferWriter<byte>
    {
        private readonly byte[] _buffer;
        private int _written;

        public PreallocatedBufferWriter(int capacity)
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
