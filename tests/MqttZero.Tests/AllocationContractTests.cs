using System.Buffers;

namespace MqttZero.Tests
{
    public sealed class AllocationContractTests
    {
        [Fact]
        public void PooledMessageLeaseDoesNotAllocateAfterWarmup()
        {
            ReadOnlySequence<byte> topic = new("hot/topic"u8.ToArray());
            ReadOnlySequence<byte> payload = new(new byte[8192]);
            ReadOnlySequence<byte> properties = ReadOnlySequence<byte>.Empty;

            using (MqttZeroMessageLease warmup = MqttZeroMessageLease.CopyFrom(
                topic,
                properties,
                payload,
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                null,
                null))
            {
                Assert.Equal(8192, warmup.Payload.Length);
            }

            int total = 0;
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int index = 0; index < 1_000; index++)
            {
                using MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                    topic,
                    properties,
                    payload,
                    MqttQualityOfServiceLevel.AtLeastOnce,
                    false,
                    null,
                    null);
                total += lease.Payload.Length;
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.Equal(8_192_000, total);
            Assert.Equal(0, allocated);
        }

        [Fact]
        public void MessageLeaseReturnsBufferOnlyAfterLastReference()
        {
            MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>("payload"u8.ToArray()),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                null,
                null);
            lease.Retain();

            lease.Release();
            Assert.Equal("payload", System.Text.Encoding.UTF8.GetString(lease.Payload.Span));
            lease.Release();
        }
    }
}
