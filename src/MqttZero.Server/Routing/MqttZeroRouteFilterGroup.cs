namespace MqttZero.Routing;

internal sealed class MqttZeroRouteFilterGroup
{
    public MqttZeroRouteFilterGroup(byte[] filter, MqttZeroRouteEntry[] entries)
    {
        Filter = filter;
        Entries = entries;
    }

    public byte[] Filter { get; }

    public MqttZeroRouteEntry[] Entries { get; }
}
