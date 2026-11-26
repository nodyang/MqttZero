namespace MqttZero.Server.Scheduling;

/// <summary>
/// Identifies one scheduled deadline by kind and owner identifier.
/// </summary>
internal readonly struct MqttZeroDeadlineKey :
    IEquatable<MqttZeroDeadlineKey>
{
    public MqttZeroDeadlineKey(
        MqttZeroDeadlineKind kind,
        string id)
    {
        Kind = kind;
        Id = id ?? throw new ArgumentNullException(nameof(id));
    }

    public MqttZeroDeadlineKind Kind { get; }

    public string Id { get; }

    public bool Equals(MqttZeroDeadlineKey other)
    {
        return Kind == other.Kind &&
            string.Equals(Id, other.Id, StringComparison.Ordinal);
    }

    public override bool Equals(object? obj)
    {
        return obj is MqttZeroDeadlineKey other && Equals(other);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine((int)Kind, Id);
    }
}
