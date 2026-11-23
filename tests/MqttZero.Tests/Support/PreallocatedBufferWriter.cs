using System.Buffers;

namespace MqttZero.Tests.Support
{
    internal sealed class PreallocatedBufferWriter : IBufferWriter<byte>
    {
        private readonly byte[] _buffer;
        private int _written;

        public PreallocatedBufferWriter(int capacity)
        {
            _buffer = new byte[capacity];
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
