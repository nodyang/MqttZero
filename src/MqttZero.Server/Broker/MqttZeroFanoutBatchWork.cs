using MqttZero.Memory;
using MqttZero.Server.Scheduling;

namespace MqttZero.Server.Broker;

internal sealed class MqttZeroFanoutBatchWork : MqttZeroShardWork
{
    private static readonly MqttZeroBoundedObjectPool<MqttZeroFanoutBatchWork>
        Pool = new(
            MqttZeroBoundedObjectPool<MqttZeroFanoutBatchWork>.
                DefaultCapacity);

    private MqttZeroFanoutOperation? _operation;
    private int _shardId;

    private MqttZeroFanoutBatchWork()
    {
    }

    internal static int PoolCountForTest
    {
        get
        {
            return Pool.Count;
        }
    }

    internal static MqttZeroFanoutBatchWork Rent(
        MqttZeroFanoutOperation operation,
        int shardId)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentOutOfRangeException.ThrowIfNegative(shardId);
        if (!Pool.TryRent(out MqttZeroFanoutBatchWork? work) ||
            work is null)
        {
            work = new MqttZeroFanoutBatchWork();
        }

        work._operation = operation;
        work._shardId = shardId;
        return work;
    }

    public override async ValueTask ExecuteAsync(
        MqttZeroEventLoop shard,
        CancellationToken cancellationToken)
    {
        MqttZeroFanoutOperation operation = _operation ??
            throw new InvalidOperationException(
                "The fanout batch work was not initialized.");
        try
        {
            await operation.ExecuteBatchAsync(
                _shardId,
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            operation.Fail(exception);
            throw;
        }
        finally
        {
            operation.CompleteBatch();
        }
    }

    public override void Release()
    {
        _operation = null;
        _shardId = 0;
        _ = Pool.Return(this);
    }
}
