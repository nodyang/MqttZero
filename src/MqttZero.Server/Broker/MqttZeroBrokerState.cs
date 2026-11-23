using MqttZero.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Routing;
using MqttZero.Sessions;
namespace MqttZero.Broker;

/// <summary>
/// Describes the MQTTZero broker lifecycle state.
/// </summary>
public enum MqttZeroBrokerState
{
    /// <summary>The broker has not been started.</summary>
    Created,

    /// <summary>The broker is starting.</summary>
    Starting,

    /// <summary>The broker is accepting connections.</summary>
    Running,

    /// <summary>The broker is stopping.</summary>
    Stopping,

    /// <summary>The broker has stopped.</summary>
    Stopped
}
