using System.Buffers.Binary;

namespace MqttZero.BrokerBenchmarks;

/// <summary>
/// Encodes a sequence number and publish timestamp into the benchmark payload prefix.
/// </summary>
internal static class BenchmarkDeliveryEnvelope
{
    public const int HeaderLength = 16;

    public static void Write(
        Span<byte> payload,
        long sequence,
        long publishTimestamp)
    {
        if (payload.Length < HeaderLength)
        {
            throw new ArgumentException(
                "The benchmark payload is smaller than the delivery envelope.",
                nameof(payload));
        }

        BinaryPrimitives.WriteInt64LittleEndian(
            payload,
            sequence);
        BinaryPrimitives.WriteInt64LittleEndian(
            payload[sizeof(long)..],
            publishTimestamp);
    }

    public static bool TryRead(
        ReadOnlySpan<byte> payload,
        out long sequence,
        out long publishTimestamp)
    {
        if (payload.Length < HeaderLength)
        {
            sequence = 0;
            publishTimestamp = 0;
            return false;
        }

        sequence = BinaryPrimitives.ReadInt64LittleEndian(payload);
        publishTimestamp = BinaryPrimitives.ReadInt64LittleEndian(
            payload[sizeof(long)..]);
        return sequence > 0;
    }
}
