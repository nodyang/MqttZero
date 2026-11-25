namespace MqttZero.Memory;

internal sealed record MqttZeroBufferBucketSnapshot(
    int BufferSize,
    int CachedCount,
    long CachedBytes);
