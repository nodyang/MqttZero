using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Server.Kestrel.Core;

namespace MqttZero.AspNetCore;

/// <summary>
/// Configures Kestrel listeners for MQTTZero.
/// </summary>
public static class MqttZeroListenOptionsExtensions
{
    /// <summary>
    /// Uses the MQTTZero connection handler for a Kestrel listener.
    /// </summary>
    /// <param name="listenOptions">The Kestrel listener options.</param>
    /// <returns>The same listener options instance.</returns>
    public static ListenOptions UseMqttZero(this ListenOptions listenOptions)
    {
        ArgumentNullException.ThrowIfNull(listenOptions);
        listenOptions.UseConnectionHandler<MqttZeroConnectionHandler>();
        return listenOptions;
    }
}
