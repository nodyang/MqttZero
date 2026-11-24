using System.Buffers;
using System.Buffers.Binary;

namespace MqttZero.Protocol;

/// <summary>
/// Provides shared MQTT packet codec read and write helpers.
/// </summary>
public static partial class MqttPacketCodec
{
    private static bool TryReadProperties(
        ref SequenceReader<byte> reader,
        out ReadOnlySequence<byte> properties)
    {
        properties = ReadOnlySequence<byte>.Empty;
        if (!TryReadVariableByteInteger(ref reader, out int propertyLength) ||
            propertyLength < 0 ||
            reader.Remaining < propertyLength)
        {
            return false;
        }

        if (propertyLength > 0)
        {
            properties = reader.Sequence.Slice(reader.Position, propertyLength);
        }

        reader.Advance(propertyLength);
        return true;
    }

    private static bool TryReadUtf8(ref SequenceReader<byte> reader, out ReadOnlySequence<byte> value)
    {
        if (!TryReadUInt16(ref reader, out ushort length) || reader.Remaining < length)
        {
            value = ReadOnlySequence<byte>.Empty;
            return false;
        }

        value = length == 0
            ? ReadOnlySequence<byte>.Empty
            : reader.Sequence.Slice(reader.Position, length);
        reader.Advance(length);
        return true;
    }

    private static bool TryReadBinary(ref SequenceReader<byte> reader, out ReadOnlySequence<byte> value)
    {
        return TryReadUtf8(ref reader, out value);
    }

    private static bool TryReadUInt16(ref SequenceReader<byte> reader, out ushort value)
    {
        value = 0;
        if (!reader.TryRead(out byte high) || !reader.TryRead(out byte low))
        {
            return false;
        }

        value = BinaryPrimitives.ReadUInt16BigEndian([high, low]);
        return true;
    }

    private static bool TryReadVariableByteInteger(ref SequenceReader<byte> reader, out int value)
    {
        value = 0;
        int multiplier = 1;
        for (int index = 0; index < 4; index++)
        {
            if (!reader.TryRead(out byte encodedByte))
            {
                return false;
            }

            value += (encodedByte & 0x7F) * multiplier;
            if ((encodedByte & 0x80) == 0)
            {
                return true;
            }

            multiplier *= 128;
        }

        return false;
    }

    private static void WriteProperties(IBufferWriter<byte> writer, ReadOnlySequence<byte> properties)
    {
        int length = (int)properties.Length;
        Span<byte> lengthSpan = writer.GetSpan(4);
        int lengthBytes = WriteVariableByteInteger(lengthSpan, length);
        writer.Advance(lengthBytes);
        WriteSequence(writer, properties);
    }

    private static void WriteUtf8(IBufferWriter<byte> writer, ReadOnlySpan<byte> value)
    {
        if (value.Length > ushort.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(value), "MQTT UTF-8 values may not exceed 65535 bytes.");
        }

        WriteUInt16(writer, (ushort)value.Length);
        Span<byte> destination = writer.GetSpan(value.Length);
        value.CopyTo(destination);
        writer.Advance(value.Length);
    }

    private static void WriteBinary(IBufferWriter<byte> writer, ReadOnlySequence<byte> value)
    {
        if (value.Length > ushort.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(value), "MQTT binary values may not exceed 65535 bytes.");
        }

        WriteUInt16(writer, (ushort)value.Length);
        WriteSequence(writer, value);
    }

    private static void WriteSequence(IBufferWriter<byte> writer, ReadOnlySequence<byte> value)
    {
        foreach (ReadOnlyMemory<byte> segment in value)
        {
            Span<byte> destination = writer.GetSpan(segment.Length);
            segment.Span.CopyTo(destination);
            writer.Advance(segment.Length);
        }
    }

    private static void WriteByte(IBufferWriter<byte> writer, byte value)
    {
        Span<byte> destination = writer.GetSpan(1);
        destination[0] = value;
        writer.Advance(1);
    }

    private static void WriteUInt16(IBufferWriter<byte> writer, ushort value)
    {
        Span<byte> destination = writer.GetSpan(2);
        BinaryPrimitives.WriteUInt16BigEndian(destination, value);
        writer.Advance(2);
    }

    private static int WriteVariableByteInteger(Span<byte> destination, int value)
    {
        int index = 0;
        do
        {
            byte encodedByte = (byte)(value % 128);
            value /= 128;
            if (value > 0)
            {
                encodedByte |= 0x80;
            }

            destination[index++] = encodedByte;
        }
        while (value > 0);

        return index;
    }

    private static int GetVariableByteIntegerSize(long value)
    {
        if (value < 128)
        {
            return 1;
        }

        if (value < 16_384)
        {
            return 2;
        }

        if (value < 2_097_152)
        {
            return 3;
        }

        return 4;
    }
}
