namespace MqttZero.AspNetCore;

/// <summary>
/// Configures a MQTT-over-WebSocket endpoint.
/// </summary>
public sealed class MqttZeroWebSocketOptions
{
    /// <summary>
    /// Gets or sets a per-connection packet fragmentation selector. A null value disables fragmentation.
    /// </summary>
    public Func<MqttZeroTransportInfo, bool>? AllowPacketFragmentationSelector { get; set; }

    internal bool ResolveAllowPacketFragmentation(MqttZeroTransportInfo transportInfo)
    {
        ArgumentNullException.ThrowIfNull(transportInfo);
        return AllowPacketFragmentationSelector?.Invoke(transportInfo) ?? false;
    }
}
