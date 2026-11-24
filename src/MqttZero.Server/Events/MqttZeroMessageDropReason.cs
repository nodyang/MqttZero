namespace MqttZero.Events;

/// <summary>Describes why a delivery was dropped.</summary>
public enum MqttZeroMessageDropReason
{
    /// <summary>No subscription matched.</summary>
    NoMatchingSubscribers,
    /// <summary>The message expired.</summary>
    Expired,
    /// <summary>Offline QoS 0 storage is disabled.</summary>
    OfflineClient,
    /// <summary>An interceptor rejected the delivery.</summary>
    InterceptorRejected
}
