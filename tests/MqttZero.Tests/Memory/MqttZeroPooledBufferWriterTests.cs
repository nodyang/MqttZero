using MqttZero.Memory;

namespace MqttZero.Tests.Memory
{
    public sealed class MqttZeroPooledBufferWriterTests
    {
        [Fact]
        public void AdvanceTracksWrittenMemoryAndDisposeReturnsBuffer()
        {
            using MqttZeroBufferPool pool = new(1024 * 1024);
            MqttZeroPooledBufferWriter writer = new(pool, 4);
            Span<byte> span = writer.GetSpan(4);
            "test"u8.CopyTo(span);
            writer.Advance(4);

            Assert.Equal("test"u8.ToArray(), writer.WrittenMemory.ToArray());
            Assert.True(pool.RentedBytes > 0);

            writer.Dispose();

            Assert.Equal(0, pool.RentedBytes);
        }

        [Fact]
        public void AdvanceOverCapacityThrowsWithoutChangingWrittenMemory()
        {
            using MqttZeroBufferPool pool = new(1024 * 1024);
            using MqttZeroPooledBufferWriter writer = new(pool, 1);

            Assert.Throws<ArgumentOutOfRangeException>(() => writer.Advance(257));
            Assert.Empty(writer.WrittenMemory.ToArray());
        }

        [Fact]
        public void DisposeIsIdempotent()
        {
            using MqttZeroBufferPool pool = new(1024 * 1024);
            MqttZeroPooledBufferWriter writer = new(pool, 1);

            writer.Dispose();
            writer.Dispose();

            Assert.Equal(0, pool.RentedBytes);
        }
    }
}
