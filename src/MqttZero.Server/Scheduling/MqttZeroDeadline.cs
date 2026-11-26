namespace MqttZero.Server.Scheduling;

/// <summary>
/// One scheduled broker deadline.
/// </summary>
internal readonly record struct MqttZeroDeadline(
    MqttZeroDeadlineKey Key,
    long DueTicks);
