namespace MqttZero.Routing;

internal sealed class MqttZeroSharedRouteGroup
{
    public MqttZeroSharedRouteGroup(
        string group,
        MqttZeroSharedRouteEntry[] entries)
    {
        Group = group;
        Entries = entries;
    }

    public string Group { get; }

    public MqttZeroSharedRouteEntry[] Entries { get; }
}
