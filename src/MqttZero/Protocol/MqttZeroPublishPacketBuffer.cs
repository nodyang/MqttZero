using System.Buffers;

namespace MqttZero.Protocol;

internal readonly struct MqttZeroPublishPacketBuffer
{
    public MqttZeroPublishPacketBuffer(
        ReadOnlyMemory<byte> header,
        ReadOnlySequence<byte> payload)
    {
        Header = header;
        Payload = payload;
    }

    public ReadOnlyMemory<byte> Header { get; }

    public ReadOnlySequence<byte> Payload { get; }

    public int Length
    {
        get
        {
            return Header.Length + (int)Payload.Length;
        }
    }
}
