using MqttZero.Server.Scheduling;

namespace MqttZero.Tests.Scheduling
{
    public sealed class MqttZeroDeadlineQueueTests
    {
        private static readonly MqttZeroDeadlineKey WillKey = new(
            MqttZeroDeadlineKind.Will,
            "client");

        [Fact]
        public void TakesDueEntriesInDeadlineOrder()
        {
            MqttZeroDeadlineQueue queue = new();
            queue.Schedule(
                new MqttZeroDeadlineKey(
                    MqttZeroDeadlineKind.RetainedExpiry,
                    "b"),
                200);
            queue.Schedule(
                new MqttZeroDeadlineKey(
                    MqttZeroDeadlineKind.Will,
                    "a"),
                100);

            Assert.True(queue.TryTakeDue(300, out MqttZeroDeadline first));
            Assert.Equal("a", first.Key.Id);
            Assert.True(queue.TryTakeDue(300, out MqttZeroDeadline second));
            Assert.Equal("b", second.Key.Id);
            Assert.False(queue.TryTakeDue(300, out _));
        }

        [Fact]
        public void DeadlineEqualToNowIsDue()
        {
            MqttZeroDeadlineQueue queue = new();
            queue.Schedule(WillKey, 500);

            Assert.True(queue.TryTakeDue(500, out MqttZeroDeadline deadline));
            Assert.Equal(500, deadline.DueTicks);
        }

        [Fact]
        public void ReschedulingInvalidatesTheStaleEntry()
        {
            MqttZeroDeadlineQueue queue = new();
            queue.Schedule(WillKey, 100);
            queue.Schedule(WillKey, 300);

            Assert.False(queue.TryTakeDue(200, out _));
            Assert.True(queue.TryTakeDue(300, out MqttZeroDeadline deadline));
            Assert.Equal(300, deadline.DueTicks);
            Assert.False(queue.TryTakeDue(400, out _));
        }

        [Fact]
        public void RemovedDeadlinesAreNotDelivered()
        {
            MqttZeroDeadlineQueue queue = new();
            queue.Schedule(WillKey, 100);
            queue.Remove(WillKey);

            Assert.Equal(0, queue.Count);
            Assert.False(queue.TryTakeDue(200, out _));
        }

        [Fact]
        public void TakingADeadlineClearsItFromTheSchedule()
        {
            MqttZeroDeadlineQueue queue = new();
            queue.Schedule(WillKey, 100);

            Assert.Equal(1, queue.Count);
            Assert.True(queue.TryTakeDue(100, out _));
            Assert.Equal(0, queue.Count);
        }

        [Fact]
        public void ClearDropsEveryScheduledDeadline()
        {
            MqttZeroDeadlineQueue queue = new();
            queue.Schedule(WillKey, 100);
            queue.Schedule(
                new MqttZeroDeadlineKey(
                    MqttZeroDeadlineKind.RetainedExpiry,
                    "topic"),
                200);

            queue.Clear();

            Assert.Equal(0, queue.Count);
            Assert.False(queue.TryTakeDue(300, out _));
        }
    }
}
