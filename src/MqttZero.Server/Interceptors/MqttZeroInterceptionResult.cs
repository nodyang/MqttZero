using MqttZero.Protocol;
namespace MqttZero.Server.Interceptors;

/// <summary>
/// Controls how a broker pipeline continues after an interceptor call.
/// </summary>
public enum MqttZeroInterceptionResult
{
    /// <summary>Continue processing.</summary>
    Continue,

    /// <summary>Consume the current operation without further broker processing.</summary>
    Consume,

    /// <summary>Close the current client connection.</summary>
    CloseConnection
}
