namespace MqttZero.Protocol;

/// <summary>
/// MQTT reason codes used by supported acknowledgement packets.
/// </summary>
public enum MqttReasonCode : byte
{
    /// <summary>The operation succeeded.</summary>
    Success = 0x00,

    /// <summary>Subscription granted at QoS 1.</summary>
    GrantedQos1 = 0x01,

    /// <summary>The requested QoS is not supported.</summary>
    QosNotSupported = 0x9B,

    /// <summary>Quota exceeded.</summary>
    QuotaExceeded = 0x97,

    /// <summary>The operation is not authorized.</summary>
    NotAuthorized = 0x87,

    /// <summary>No matching subscription existed.</summary>
    NoSubscriptionExisted = 0x11,

    /// <summary>The client is not connected.</summary>
    NotConnected = 0x90,

    /// <summary>The server is shutting down.</summary>
    ServerShuttingDown = 0x8B,

    /// <summary>The client session was taken over by another connection.</summary>
    SessionTakenOver = 0x8E,

    /// <summary>The keep-alive timeout expired.</summary>
    KeepAliveTimeout = 0x8D,

    /// <summary>The payload format is invalid.</summary>
    PayloadFormatInvalid = 0x99
}
