using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
namespace MqttZero.Tests.Memory
{
    public sealed class MqttZeroBufferPoolTests
    {
        [Fact]
        public void RentAndReturnReusesBucketedBufferWithoutAllocation()
        {
            using MqttZeroBufferPool pool = new(1024 * 1024);
            byte[] warmup = pool.Rent(8192);
            pool.Return(warmup);

            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int index = 0; index < 1000; index++)
            {
                byte[] buffer = pool.Rent(8192);
                pool.Return(buffer);
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.Equal(0, allocated);
        }

        [Fact]
        public void PoolEnforcesRentedByteLimitAndRecoversAfterReturn()
        {
            using MqttZeroBufferPool pool = new(1024);
            byte[] first = pool.Rent(1024);
            Assert.True(pool.RentedBytes >= 1024);
            Assert.False(pool.TryRent(1, out _));

            pool.Return(first);

            Assert.Equal(0, pool.RentedBytes);
            Assert.True(pool.TryRent(1, out byte[] second));
            pool.Return(second);
        }

        [Fact]
        public void FailedMessageLeaseDoesNotLeakEarlierBuffers()
        {
            using MqttZeroBufferPool pool = new(512);

            Assert.ThrowsAny<InvalidOperationException>(() =>
                MqttZeroMessageLease.CopyFrom(
                    new System.Buffers.ReadOnlySequence<byte>("topic"u8.ToArray()),
                    System.Buffers.ReadOnlySequence<byte>.Empty,
                    new System.Buffers.ReadOnlySequence<byte>(new byte[1024]),
                    MqttQualityOfServiceLevel.AtMostOnce,
                    false,
                    null,
                    null,
                    pool));

            Assert.Equal(0, pool.RentedBytes);
        }
    }
}
