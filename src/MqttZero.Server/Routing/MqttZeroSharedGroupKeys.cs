using MqttZero.Server.Sessions;
using System.Text;

namespace MqttZero.Server.Routing;

/// <summary>
/// Builds shared subscription keys used by routing and cursor accounting.
/// </summary>
internal static class MqttZeroSharedGroupKeys
{
    public static MqttZeroSharedGroupKey Create(
        MqttZeroSubscription subscription)
    {
        ArgumentNullException.ThrowIfNull(subscription);
        return new MqttZeroSharedGroupKey(
            Encoding.UTF8.GetBytes(subscription.SharedGroup!),
            subscription.MatchFilter);
    }
}
