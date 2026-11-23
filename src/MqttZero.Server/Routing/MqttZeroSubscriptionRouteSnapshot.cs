namespace MqttZero.Routing;

internal sealed class MqttZeroSubscriptionRouteSnapshot
{
    public static MqttZeroSubscriptionRouteSnapshot Empty { get; } =
        new([], []);

    public MqttZeroSubscriptionRouteSnapshot(
        MqttZeroRouteFilterGroup[] regularGroups,
        MqttZeroRouteEntry[] sharedEntries)
    {
        RegularGroups = regularGroups;
        SharedEntries = sharedEntries;
    }

    public MqttZeroRouteFilterGroup[] RegularGroups { get; }

    public MqttZeroRouteEntry[] SharedEntries { get; }
}
