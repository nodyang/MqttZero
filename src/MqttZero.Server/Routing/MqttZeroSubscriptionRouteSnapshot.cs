namespace MqttZero.Routing;

internal sealed class MqttZeroSubscriptionRouteSnapshot
{
    public static MqttZeroSubscriptionRouteSnapshot Empty { get; } =
        new([], []);

    public MqttZeroSubscriptionRouteSnapshot(
        MqttZeroRouteFilterGroup[] regularGroups,
        MqttZeroSharedRouteGroup[] sharedGroups)
    {
        RegularGroups = regularGroups;
        SharedGroups = sharedGroups;
    }

    public MqttZeroRouteFilterGroup[] RegularGroups { get; }

    public MqttZeroSharedRouteGroup[] SharedGroups { get; }
}
