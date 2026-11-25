using MqttZero.Memory;
using MqttZero.Server.Scheduling;

namespace MqttZero.Server.Broker;

internal sealed class MqttZeroInboundShardWork : MqttZeroShardWork
{
    private static readonly MqttZeroBoundedObjectPool<MqttZeroInboundShardWork>
        Pool = new(
            MqttZeroBoundedObjectPool<MqttZeroInboundShardWork>.
                DefaultCapacity);
    private Action<Exception>? _faulted;
    private Func<MqttZeroInboundWork, CancellationToken, ValueTask>? _route;
    private MqttZeroInboundWork? _work;

    private MqttZeroInboundShardWork()
    {
    }

    internal static int PoolCountForTest
    {
        get
        {
            return Pool.Count;
        }
    }

    public static MqttZeroInboundShardWork Rent(
        MqttZeroInboundWork work,
        Func<MqttZeroInboundWork, CancellationToken, ValueTask> route,
        Action<Exception> faulted)
    {
        ArgumentNullException.ThrowIfNull(work);
        ArgumentNullException.ThrowIfNull(route);
        ArgumentNullException.ThrowIfNull(faulted);
        if (!Pool.TryRent(out MqttZeroInboundShardWork? shardWork) ||
            shardWork is null)
        {
            shardWork = new MqttZeroInboundShardWork();
        }

        shardWork._work = work;
        shardWork._route = route;
        shardWork._faulted = faulted;
        return shardWork;
    }

    public override async ValueTask ExecuteAsync(
        MqttZeroEventLoop shard,
        CancellationToken cancellationToken)
    {
        MqttZeroInboundWork work = _work ??
            throw new InvalidOperationException(
                "The inbound shard work was not initialized.");
        Func<MqttZeroInboundWork, CancellationToken, ValueTask> route =
            _route ??
                throw new InvalidOperationException(
                    "The inbound shard route was not initialized.");
        try
        {
            await route(work, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            _faulted?.Invoke(exception);
            throw;
        }
    }

    public override void Release()
    {
        _work?.Release();
        _work = null;
        _route = null;
        _faulted = null;
        _ = Pool.Return(this);
    }
}
