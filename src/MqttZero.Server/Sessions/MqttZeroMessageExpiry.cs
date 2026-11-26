namespace MqttZero.Server.Sessions;

/// <summary>
/// Identifies one queued message at a specific queue link version.
/// </summary>
internal readonly struct MqttZeroMessageExpiry
{
    public MqttZeroMessageExpiry(
        MqttZeroQueuedMessage message,
        long version)
    {
        Message = message ??
            throw new ArgumentNullException(nameof(message));
        Version = version;
    }

    public MqttZeroQueuedMessage Message { get; }

    public long Version { get; }
}
