namespace MqttZero.Interceptors;

/// <summary>Controls CONNECT processing.</summary>
public enum MqttZeroConnectionInterceptionResult
{
    /// <summary>Continue processing.</summary>
    Continue,
    /// <summary>Reject with a CONNACK reason.</summary>
    Reject,
    /// <summary>Close without a CONNACK.</summary>
    Drop
}
