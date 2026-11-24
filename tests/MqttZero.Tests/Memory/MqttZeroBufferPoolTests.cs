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

        [Fact]
        public async Task WaitForCapacityCompletesAfterBufferIsReturned()
        {
            using MqttZeroBufferPool pool = new(1024);
            byte[] buffer = pool.Rent(1024);
            Task wait = pool.WaitForCapacityAsync(
                TestContext.Current.CancellationToken).AsTask();

            Assert.False(wait.IsCompleted);
            pool.Return(buffer);

            await wait.WaitAsync(
                TimeSpan.FromSeconds(2),
                TestContext.Current.CancellationToken);
        }

        [Fact]
        public async Task WaitForCapacityHonorsCancellation()
        {
            using MqttZeroBufferPool pool = new(1024);
            byte[] buffer = pool.Rent(1024);
            using CancellationTokenSource cancellation = new();
            Task wait = pool.WaitForCapacityAsync(cancellation.Token).AsTask();
            cancellation.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                async () => await wait);
            pool.Return(buffer);
        }

        [Fact]
        public void CachedBufferReturnsToBucketWhenCapacityCannotBeReserved()
        {
            using MqttZeroBufferPool pool = new(1024);
            byte[] cached = pool.Rent(256);
            pool.Return(cached);
            byte[] held = pool.Rent(1024);

            Assert.False(pool.TryRent(256, out byte[] unavailable));
            Assert.Empty(unavailable);

            pool.Return(held);
        }

        [Fact]
        public void ReturnEmptyBufferAndDisposeAreSafe()
        {
            MqttZeroBufferPool pool = new(1024);

            pool.Return([]);
            pool.Dispose();
            pool.Dispose();

            Assert.Throws<ObjectDisposedException>(() => pool.Rent(1));
        }
    }
}
