namespace MqttZero.AspNetCore;

/// <summary>
/// Configures a TCP or TLS MQTTZero Kestrel listener.
/// </summary>
public sealed class MqttZeroEndpointOptions
{
    /// <summary>
    /// Gets the accepted socket options.
    /// </summary>
    public MqttZeroAcceptedSocketOptions AcceptedSockets { get; } = new();

    /// <summary>
    /// Gets or sets the static TLS configuration. A null value disables MQTTZero TLS setup.
    /// </summary>
    public MqttZeroTlsOptions? Tls { get; set; }

    /// <summary>
    /// Gets or sets a per-connection packet fragmentation selector. A null value allows fragmentation.
    /// </summary>
    public Func<MqttZeroTransportInfo, bool>? AllowPacketFragmentationSelector { get; set; }

    internal bool ResolveAllowPacketFragmentation(MqttZeroTransportInfo transportInfo)
    {
        ArgumentNullException.ThrowIfNull(transportInfo);
        return AllowPacketFragmentationSelector?.Invoke(transportInfo) ?? true;
    }
}
