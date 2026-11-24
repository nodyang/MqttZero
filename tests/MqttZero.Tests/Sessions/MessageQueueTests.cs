using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Sessions;
using System.Buffers;

namespace MqttZero.Tests.Sessions
{
    public sealed class MessageQueueTests
    {
        [Fact]
        public void RemoveAtDecrementsExpiringCountWhenRemovedMessageExpires()
        {
            MqttZeroMessageQueue queue = new(4);
            MqttZeroQueuedMessage expiring = CreateQueuedMessage(
                DateTimeOffset.UtcNow.AddMinutes(1),
                out MqttZeroMessageLease lease);
            queue.Add(expiring);

            queue.RemoveAt(0);

            Assert.False(queue.HasExpiringMessages);
            expiring.Release();
            lease.Release();
        }

        [Fact]
        public void RemoveAtKeepsExpiringCountWhenShiftedMessageExpires()
        {
            MqttZeroMessageQueue queue = new(4);
            MqttZeroQueuedMessage persistent = CreateQueuedMessage(
                null,
                out MqttZeroMessageLease persistentLease);
            MqttZeroQueuedMessage expiring = CreateQueuedMessage(
                DateTimeOffset.UtcNow.AddMinutes(1),
                out MqttZeroMessageLease expiringLease);
            queue.Add(persistent);
            queue.Add(expiring);

            queue.RemoveAt(0);

            Assert.True(queue.HasExpiringMessages);
            Assert.Same(expiring, queue.Peek());
            persistent.Release();
            expiring.Release();
            persistentLease.Release();
            expiringLease.Release();
        }

        private static MqttZeroQueuedMessage CreateQueuedMessage(
            DateTimeOffset? expiresAt,
            out MqttZeroMessageLease lease)
        {
            lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("queue/topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>("payload"u8.ToArray()),
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                null,
                null);
            return MqttZeroQueuedMessage.Rent(
                lease,
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                null,
                expiresAt);
        }
    }
}
