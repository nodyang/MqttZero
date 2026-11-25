namespace MqttZero.Server.Routing;

internal sealed record MqttZeroRoutePlanBufferPoolSnapshot(
    int AvailableCount,
    int InUseCount,
    long RetainedBytes);
