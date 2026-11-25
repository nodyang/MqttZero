using MqttZero.Server.Scheduling;

namespace MqttZero.Tests.Scheduling;

public sealed class MqttZeroEventLoopTests
{
    [Fact(Timeout = 5_000)]
    public async Task ProcessesWorkInOrderWithSingleActiveDrain()
    {
        await using MqttZeroEventLoop shard = new(3, 128);
        List<int> order = [];
        int active = 0;
        int maximumActive = 0;
        TaskCompletionSource completed = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        for (int index = 0; index < 100; index++)
        {
            int value = index;
            await shard.EnqueueAsync(
                new DelegateShardWork(
                    async cancellationToken =>
                    {
                        int current = Interlocked.Increment(ref active);
                        int observed;
                        do
                        {
                            observed = Volatile.Read(ref maximumActive);
                        }
                        while (current > observed &&
                            Interlocked.CompareExchange(
                                ref maximumActive,
                                current,
                                observed) != observed);

                        order.Add(value);
                        await Task.Yield();
                        Interlocked.Decrement(ref active);
                        if (order.Count == 100)
                        {
                            completed.TrySetResult();
                        }
                    }),
                TestContext.Current.CancellationToken);
        }

        await completed.Task.WaitAsync(
            TestContext.Current.CancellationToken);

        Assert.Equal(Enumerable.Range(0, 100), order);
        Assert.Equal(1, maximumActive);
        await shard.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task AsyncWorkBlocksFollowingWorkUntilCompletion()
    {
        await using MqttZeroEventLoop shard = new(0, 16);
        TaskCompletionSource firstStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseFirst = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource secondCompleted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        bool secondStarted = false;

        await shard.EnqueueAsync(
            new DelegateShardWork(
                async cancellationToken =>
                {
                    firstStarted.TrySetResult();
                    await releaseFirst.Task.WaitAsync(cancellationToken);
                }),
            TestContext.Current.CancellationToken);
        await shard.EnqueueAsync(
            new DelegateShardWork(
                _ =>
                {
                    secondStarted = true;
                    secondCompleted.TrySetResult();
                    return ValueTask.CompletedTask;
                }),
            TestContext.Current.CancellationToken);

        await firstStarted.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        await Task.Delay(
            TimeSpan.FromMilliseconds(50),
            TestContext.Current.CancellationToken);
        Assert.False(secondStarted);

        releaseFirst.TrySetResult();
        await secondCompleted.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        Assert.True(secondStarted);
        await shard.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task EnqueueAppliesBackpressureWhenQueueIsFull()
    {
        await using MqttZeroEventLoop shard = new(0, 1);
        TaskCompletionSource releaseFirst = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource bothQueued = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        await shard.EnqueueAsync(
            new DelegateShardWork(
                async cancellationToken =>
                {
                    await releaseFirst.Task.WaitAsync(cancellationToken);
                }),
            TestContext.Current.CancellationToken);
        Task secondEnqueue = shard.EnqueueAsync(
            new DelegateShardWork(
                _ =>
                {
                    bothQueued.TrySetResult();
                    return ValueTask.CompletedTask;
                }),
            TestContext.Current.CancellationToken).AsTask();

        await Task.Delay(
            TimeSpan.FromMilliseconds(50),
            TestContext.Current.CancellationToken);
        Assert.False(secondEnqueue.IsCompleted);

        releaseFirst.TrySetResult();
        await secondEnqueue.WaitAsync(
            TestContext.Current.CancellationToken);
        await bothQueued.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        await shard.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task WorkFailureDoesNotStopFollowingWork()
    {
        await using MqttZeroEventLoop shard = new(0, 8);
        TaskCompletionSource completed = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        await shard.EnqueueAsync(
            new DelegateShardWork(
                _ => throw new InvalidOperationException("expected")),
            TestContext.Current.CancellationToken);
        await shard.EnqueueAsync(
            new DelegateShardWork(
                _ =>
                {
                    completed.TrySetResult();
                    return ValueTask.CompletedTask;
                }),
            TestContext.Current.CancellationToken);

        await completed.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        MqttZeroShardDiagnostics snapshot = shard.GetSnapshot();
        Assert.Equal(1, snapshot.FailedCount);
        Assert.Equal(1, snapshot.CompletedCount);
        await shard.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task StopDrainsQueuedWorkAndRejectsNewWork()
    {
        await using MqttZeroEventLoop shard = new(0, 8);
        TaskCompletionSource release = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource secondCompleted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        await shard.EnqueueAsync(
            new DelegateShardWork(
                async cancellationToken =>
                {
                    await release.Task.WaitAsync(cancellationToken);
                }),
            TestContext.Current.CancellationToken);
        await shard.EnqueueAsync(
            new DelegateShardWork(
                _ =>
                {
                    secondCompleted.TrySetResult();
                    return ValueTask.CompletedTask;
                }),
            TestContext.Current.CancellationToken);

        Task stop = shard.StopAsync().AsTask();
        Assert.False(stop.IsCompleted);
        release.TrySetResult();
        await stop.WaitAsync(TestContext.Current.CancellationToken);
        await secondCompleted.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await shard.EnqueueAsync(
                new DelegateShardWork(_ => ValueTask.CompletedTask),
                TestContext.Current.CancellationToken));
    }

    [Fact(Timeout = 5_000)]
    public async Task IsCurrentIsTrueOnlyInsideOwningShard()
    {
        await using MqttZeroEventLoop shard = new(1, 8);
        TaskCompletionSource<bool> current = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        await shard.EnqueueAsync(
            new DelegateShardWork(
                _ =>
                {
                    current.TrySetResult(shard.IsCurrent);
                    return ValueTask.CompletedTask;
                }),
            TestContext.Current.CancellationToken);

        Assert.True(await current.Task.WaitAsync(
            TestContext.Current.CancellationToken));
        Assert.False(shard.IsCurrent);
        await shard.StopAsync();
    }

    [Fact(Timeout = 10_000)]
    public async Task ProcessesBudgetedBacklogAcrossMultipleDrains()
    {
        await using MqttZeroEventLoop shard = new(0, 2048);
        const int workCount = 2048;
        int completedCount = 0;
        TaskCompletionSource completed = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        for (int index = 0; index < workCount; index++)
        {
            await shard.EnqueueAsync(
                new DelegateShardWork(
                    _ =>
                    {
                        Thread.SpinWait(32);
                        if (Interlocked.Increment(ref completedCount) ==
                            workCount)
                        {
                            completed.TrySetResult();
                        }

                        return ValueTask.CompletedTask;
                    }),
                TestContext.Current.CancellationToken);
        }

        await completed.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        Assert.Equal(workCount, completedCount);
        await shard.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task LowPriorityWorkRunsAfterQueuedHighPriorityWork()
    {
        await using MqttZeroEventLoop shard = new(0, 16);
        TaskCompletionSource blockerStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseBlocker = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource completed = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        List<string> order = [];

        await shard.EnqueueAsync(
            new DelegateShardWork(
                async cancellationToken =>
                {
                    blockerStarted.TrySetResult();
                    await releaseBlocker.Task.WaitAsync(cancellationToken);
                }),
            TestContext.Current.CancellationToken);
        await blockerStarted.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        await shard.EnqueueAsync(
            new PriorityShardWork(
                isLowPriority: true,
                _ =>
                {
                    order.Add("low");
                    if (order.Count == 2)
                    {
                        completed.TrySetResult();
                    }

                    return ValueTask.CompletedTask;
                }),
            TestContext.Current.CancellationToken);
        await shard.EnqueueAsync(
            new PriorityShardWork(
                isLowPriority: false,
                _ =>
                {
                    order.Add("high");
                    if (order.Count == 2)
                    {
                        completed.TrySetResult();
                    }

                    return ValueTask.CompletedTask;
                }),
            TestContext.Current.CancellationToken);
        releaseBlocker.TrySetResult();

        await completed.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        Assert.Equal(["high", "low"], order);
        await shard.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task ReportsPriorityQueueDepthAndLowPriorityWait()
    {
        await using MqttZeroEventLoop shard = new(0, 32);
        TaskCompletionSource blockerStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseBlocker = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource completed = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        int remaining = 13;

        await shard.EnqueueAsync(
            new DelegateShardWork(
                async cancellationToken =>
                {
                    blockerStarted.TrySetResult();
                    await releaseBlocker.Task.WaitAsync(cancellationToken);
                }),
            TestContext.Current.CancellationToken);
        await blockerStarted.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        await shard.EnqueueAsync(
            new PriorityShardWork(
                isLowPriority: true,
                _ =>
                {
                    if (Interlocked.Decrement(ref remaining) == 0)
                    {
                        completed.TrySetResult();
                    }

                    return ValueTask.CompletedTask;
                }),
            TestContext.Current.CancellationToken);

        for (int index = 0; index < 12; index++)
        {
            await shard.EnqueueAsync(
                new PriorityShardWork(
                    isLowPriority: false,
                    _ =>
                    {
                        if (Interlocked.Decrement(ref remaining) == 0)
                        {
                            completed.TrySetResult();
                        }

                        return ValueTask.CompletedTask;
                    }),
                TestContext.Current.CancellationToken);
        }

        MqttZeroShardDiagnostics queued = shard.GetSnapshot();
        Assert.Equal(12, queued.HighPriorityQueuedCount);
        Assert.Equal(1, queued.LowPriorityQueuedCount);
        Assert.True(queued.HighPriorityOldestWorkAgeMilliseconds >= 0);
        Assert.True(queued.LowPriorityOldestWorkAgeMilliseconds >= 0);

        releaseBlocker.TrySetResult();
        await completed.Task.WaitAsync(
            TestContext.Current.CancellationToken);

        MqttZeroShardDiagnostics completedSnapshot =
            shard.GetSnapshot();
        Assert.True(completedSnapshot.LowPriorityWaitCount > 0);
        Assert.Equal(0, completedSnapshot.HighPriorityQueuedCount);
        Assert.Equal(0, completedSnapshot.LowPriorityQueuedCount);
        await shard.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task DisabledPriorityDiagnosticsSkipOptionalCounters()
    {
        await using MqttZeroEventLoop shard = new(
            0,
            32,
            trackPriorityDiagnostics: false);
        TaskCompletionSource blockerStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseBlocker = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource completed = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        int remaining = 2;

        await shard.EnqueueAsync(
            new DelegateShardWork(
                async cancellationToken =>
                {
                    blockerStarted.TrySetResult();
                    await releaseBlocker.Task.WaitAsync(cancellationToken);
                }),
            TestContext.Current.CancellationToken);
        await blockerStarted.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        await shard.EnqueueAsync(
            new PriorityShardWork(
                isLowPriority: true,
                _ =>
                {
                    if (Interlocked.Decrement(ref remaining) == 0)
                    {
                        completed.TrySetResult();
                    }

                    return ValueTask.CompletedTask;
                }),
            TestContext.Current.CancellationToken);
        await shard.EnqueueAsync(
            new PriorityShardWork(
                isLowPriority: false,
                _ =>
                {
                    if (Interlocked.Decrement(ref remaining) == 0)
                    {
                        completed.TrySetResult();
                    }

                    return ValueTask.CompletedTask;
                }),
            TestContext.Current.CancellationToken);

        MqttZeroShardDiagnostics queued = shard.GetSnapshot();
        Assert.Equal(2, queued.QueuedCount);
        Assert.Equal(0, queued.HighPriorityQueuedCount);
        Assert.Equal(0, queued.LowPriorityQueuedCount);
        Assert.Equal(
            0,
            queued.HighPriorityOldestWorkAgeMilliseconds);
        Assert.Equal(
            0,
            queued.LowPriorityOldestWorkAgeMilliseconds);

        releaseBlocker.TrySetResult();
        await completed.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        Assert.Equal(0, shard.GetSnapshot().LowPriorityWaitCount);
        await shard.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task TryEnqueueRunsWorkWhenCapacityIsAvailable()
    {
        await using MqttZeroEventLoop shard = new(0, 4);
        TaskCompletionSource completed = new(
            TaskCreationOptions.RunContinuationsAsynchronously);

        bool enqueued = shard.TryEnqueue(
            new DelegateShardWork(
                _ =>
                {
                    completed.TrySetResult();
                    return ValueTask.CompletedTask;
                }));

        Assert.True(enqueued);
        await completed.Task.WaitAsync(
            TestContext.Current.CancellationToken);
        await shard.StopAsync();
    }

    [Fact(Timeout = 5_000)]
    public async Task TryEnqueueReturnsFalseWhenQueueIsFull()
    {
        await using MqttZeroEventLoop shard = new(0, 1);
        TaskCompletionSource blockerStarted = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseBlocker = new(
            TaskCreationOptions.RunContinuationsAsynchronously);
        await shard.EnqueueAsync(
            new DelegateShardWork(
                async cancellationToken =>
                {
                    blockerStarted.TrySetResult();
                    await releaseBlocker.Task.WaitAsync(cancellationToken);
                }),
            TestContext.Current.CancellationToken);
        await blockerStarted.Task.WaitAsync(
            TestContext.Current.CancellationToken);

        bool enqueued = shard.TryEnqueue(
            new DelegateShardWork(_ => ValueTask.CompletedTask));

        Assert.False(enqueued);
        releaseBlocker.TrySetResult();
        await shard.StopAsync();
    }

    private sealed class DelegateShardWork : MqttZeroShardWork
    {
        private readonly Func<CancellationToken, ValueTask> _execute;

        public DelegateShardWork(
            Func<CancellationToken, ValueTask> execute)
        {
            _execute = execute;
        }

        public override ValueTask ExecuteAsync(
            MqttZeroEventLoop shard,
            CancellationToken cancellationToken)
        {
            return _execute(cancellationToken);
        }
    }

    private sealed class PriorityShardWork : MqttZeroShardWork
    {
        private readonly Func<CancellationToken, ValueTask> _execute;

        public PriorityShardWork(
            bool isLowPriority,
            Func<CancellationToken, ValueTask> execute)
        {
            IsLowPriority = isLowPriority;
            _execute = execute;
        }

        public override bool IsLowPriority { get; }

        public override ValueTask ExecuteAsync(
            MqttZeroEventLoop shard,
            CancellationToken cancellationToken)
        {
            return _execute(cancellationToken);
        }
    }
}
