using MqttZero.Memory;

namespace MqttZero.Server.Broker;

internal sealed class MqttZeroFanoutScheduledBatch
{
    private static readonly MqttZeroBoundedObjectPool<
        MqttZeroFanoutScheduledBatch> Pool = new(
            MqttZeroBoundedObjectPool<MqttZeroFanoutScheduledBatch>.
                DefaultCapacity);

    private MqttZeroFanoutOperation? _operation;
    private int _shardId;

    private MqttZeroFanoutScheduledBatch()
    {
    }

    internal static int PoolCountForTest
    {
        get
        {
            return Pool.Count;
        }
    }

    internal MqttZeroFanoutOperation Operation
    {
        get
        {
            return _operation ??
                throw new InvalidOperationException(
                    "The fanout scheduled batch was not initialized.");
        }
    }

    internal int ShardId
    {
        get
        {
            return _shardId;
        }
    }

    internal static MqttZeroFanoutScheduledBatch Rent(
        MqttZeroFanoutOperation operation,
        int shardId)
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentOutOfRangeException.ThrowIfNegative(shardId);
        if (!Pool.TryRent(out MqttZeroFanoutScheduledBatch? batch) ||
            batch is null)
        {
            batch = new MqttZeroFanoutScheduledBatch();
        }

        batch._operation = operation;
        batch._shardId = shardId;
        return batch;
    }

    internal void Release()
    {
        _operation = null;
        _shardId = 0;
        _ = Pool.Return(this);
    }
}
