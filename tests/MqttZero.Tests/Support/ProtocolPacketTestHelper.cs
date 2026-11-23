using System.Buffers;
using MqttZero.Protocol;

namespace MqttZero.Tests.Support
{
    internal static class ProtocolPacketTestHelper
    {
        public static byte[] Encode(MqttPacket packet)
        {
            ArrayBufferWriter<byte> writer = new();
            MqttPacketCodec.Encode(packet, writer);
            return writer.WrittenSpan.ToArray();
        }
    }
}
