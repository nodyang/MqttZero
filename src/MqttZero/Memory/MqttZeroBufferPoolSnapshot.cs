namespace MqttZero.Memory;

internal sealed record MqttZeroBufferPoolSnapshot(
    long RentedBytes,
    long CachedBytes,
    long CommittedBytes,
    MqttZeroBufferBucketSnapshot[] Buckets);
