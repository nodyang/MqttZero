namespace MqttZero.Server.Broker;

internal sealed record MqttZeroBrokerMemorySnapshot(
    long BufferRentedBytes,
    long BufferCachedBytes,
    long BufferCommittedBytes,
    int RoutePlanAvailableCount,
    int RoutePlanInUseCount,
    long RoutePlanRetainedBytes,
    int SharedCursorCount,
    MqttZeroObjectPoolSnapshot ObjectPools);
