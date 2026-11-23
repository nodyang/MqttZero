using System.Buffers.Binary;

namespace MqttZero.Protocol;

internal ref struct MqttZeroSpanWriter
{
    private readonly Span<byte> _destination;
    private int _position;

    public MqttZeroSpanWriter(Span<byte> destination)
    {
        _destination = destination;
        _position = 0;
    }

    public int WrittenCount
    {
        get
        {
            return _position;
        }
    }

    public void WriteByte(byte value)
    {
        _destination[_position++] = value;
    }

    public void WriteUInt16(ushort value)
    {
        BinaryPrimitives.WriteUInt16BigEndian(_destination[_position..], value);
        _position += sizeof(ushort);
    }

    public void WriteUInt32(uint value)
    {
        BinaryPrimitives.WriteUInt32BigEndian(_destination[_position..], value);
        _position += sizeof(uint);
    }

    public void WriteVariableByteInteger(uint value)
    {
        do
        {
            byte encoded = (byte)(value % 128);
            value /= 128;
            if (value > 0)
            {
                encoded |= 0x80;
            }

            _destination[_position++] = encoded;
        }
        while (value > 0);
    }

    public void WriteBytes(ReadOnlySpan<byte> value)
    {
        value.CopyTo(_destination[_position..]);
        _position += value.Length;
    }
}
