using System.Buffers;

namespace MqttZero.Memory;

internal sealed class MqttZeroNoCacheArrayPool : ArrayPool<byte>
{
    public static MqttZeroNoCacheArrayPool Instance { get; } = new();

    private MqttZeroNoCacheArrayPool()
    {
    }

    public override byte[] Rent(int minimumLength)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(minimumLength);
        return new byte[minimumLength];
    }

    public override void Return(byte[] array, bool clearArray = false)
    {
        if (clearArray)
        {
            Array.Clear(array);
        }
    }
}
