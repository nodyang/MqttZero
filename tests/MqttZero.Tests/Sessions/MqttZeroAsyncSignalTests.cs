using MqttZero.Server.Sessions;

namespace MqttZero.Tests.Sessions
{
    public sealed class MqttZeroAsyncSignalTests
    {
        [Fact]
        public async Task SignalBeforeWaitCompletesImmediately()
        {
            MqttZeroAsyncSignal signal = new();

            signal.Signal();
            await signal.WaitAsync();
        }

        [Fact]
        public async Task MultipleSignalsBeforeWaitCoalesce()
        {
            MqttZeroAsyncSignal signal = new();

            signal.Signal();
            signal.Signal();
            await signal.WaitAsync();
            ValueTask next = signal.WaitAsync();

            Assert.False(next.IsCompleted);
        }

        [Fact]
        public async Task SignalCompletesPendingWait()
        {
            MqttZeroAsyncSignal signal = new();
            ValueTask wait = signal.WaitAsync();

            Assert.False(wait.IsCompleted);
            signal.Signal();

            await wait;
        }

        [Fact]
        public async Task SignalCanBeWaitedAcrossSequentialCycles()
        {
            MqttZeroAsyncSignal signal = new();

            for (int index = 0; index < 1_000; index++)
            {
                signal.Signal();
                await signal.WaitAsync();
            }
        }

        [Fact]
        public async Task SignalAndConsumeDoNotAllocateAfterWarmup()
        {
            MqttZeroAsyncSignal signal = new();

            for (int index = 0; index < 100; index++)
            {
                signal.Signal();
                await signal.WaitAsync();
            }

            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int index = 0; index < 10_000; index++)
            {
                signal.Signal();
                await signal.WaitAsync();
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.Equal(0, allocated);
        }
    }
}
