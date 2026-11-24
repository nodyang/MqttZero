using MqttZero.Server.Sessions;

namespace MqttZero.Tests.Sessions
{
    public sealed class OutboundPumpTests
    {
        [Fact]
        public async Task SignalBeforeStartCausesDrain()
        {
            TaskCompletionSource drained = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            await using MqttZeroSessionOutboundPump pump = new(
                _ =>
                {
                    drained.TrySetResult();
                    return ValueTask.CompletedTask;
                },
                _ => { });

            pump.Signal();
            pump.Start();

            await drained.Task.WaitAsync(
                TimeSpan.FromSeconds(1),
                TestContext.Current.CancellationToken);
            await pump.StopAsync();
        }

        [Fact]
        public async Task SignalsDuringDrainCauseAnotherDrain()
        {
            int drainCount = 0;
            TaskCompletionSource firstDrainStarted = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource releaseFirstDrain = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource secondDrainCompleted = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            await using MqttZeroSessionOutboundPump pump = new(
                async cancellationToken =>
                {
                    int current = Interlocked.Increment(ref drainCount);
                    if (current == 1)
                    {
                        firstDrainStarted.TrySetResult();
                        await releaseFirstDrain.Task.WaitAsync(cancellationToken);
                    }
                    else if (current == 2)
                    {
                        secondDrainCompleted.TrySetResult();
                    }
                },
                _ => { });

            pump.Start();
            pump.Signal();
            await firstDrainStarted.Task.WaitAsync(
                TimeSpan.FromSeconds(1),
                TestContext.Current.CancellationToken);
            pump.Signal();
            pump.Signal();
            releaseFirstDrain.TrySetResult();

            await secondDrainCompleted.Task.WaitAsync(
                TimeSpan.FromSeconds(1),
                TestContext.Current.CancellationToken);
            Assert.Equal(2, Volatile.Read(ref drainCount));
            await pump.StopAsync();
        }

        [Fact]
        public async Task StopCancelsActiveDrain()
        {
            TaskCompletionSource started = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource cancelled = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            await using MqttZeroSessionOutboundPump pump = new(
                async cancellationToken =>
                {
                    started.TrySetResult();
                    try
                    {
                        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        cancelled.TrySetResult();
                        throw;
                    }
                },
                _ => { });

            pump.Start();
            pump.Signal();
            await started.Task.WaitAsync(
                TimeSpan.FromSeconds(1),
                TestContext.Current.CancellationToken);
            await pump.StopAsync();

            await cancelled.Task.WaitAsync(
                TimeSpan.FromSeconds(1),
                TestContext.Current.CancellationToken);
        }

        [Fact]
        public async Task DrainFailureInvokesFaultCallback()
        {
            InvalidOperationException expected = new("drain failed");
            TaskCompletionSource<Exception> faulted = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            await using MqttZeroSessionOutboundPump pump = new(
                _ => throw expected,
                exception => faulted.TrySetResult(exception));

            pump.Start();
            pump.Signal();

            Exception actual = await faulted.Task.WaitAsync(
                TimeSpan.FromSeconds(1),
                TestContext.Current.CancellationToken);
            Assert.Same(expected, actual);
            await pump.StopAsync();
        }
    }
}
