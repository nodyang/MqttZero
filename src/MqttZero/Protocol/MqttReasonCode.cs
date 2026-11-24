namespace MqttZero.Protocol;

/// <summary>
/// MQTT reason codes used by supported acknowledgement packets.
/// </summary>
public enum MqttReasonCode : byte
{
    /// <summary>The operation succeeded.</summary>
    Success = 0x00,

    /// <summary>The server failed unexpectedly.</summary>
    UnspecifiedError = 0x80,

    /// <summary>The server encountered an implementation error.</summary>
    ImplementationSpecificError = 0x83,

    /// <summary>The server is unavailable.</summary>
    ServerUnavailable = 0x88,

    /// <summary>The client is banned.</summary>
    Banned = 0x8A,

    /// <summary>The user name or password is invalid.</summary>
    BadUserNameOrPassword = 0x86,

    /// <summary>Subscription granted at QoS 1.</summary>
    GrantedQos1 = 0x01,

    /// <summary>Subscription granted at QoS 2.</summary>
    GrantedQos2 = 0x02,

    /// <summary>The requested QoS is not supported.</summary>
    QosNotSupported = 0x9B,

    /// <summary>Quota exceeded.</summary>
    QuotaExceeded = 0x97,

    /// <summary>The operation is not authorized.</summary>
    NotAuthorized = 0x87,

    /// <summary>No matching subscription existed.</summary>
    NoSubscriptionExisted = 0x11,

    /// <summary>The client is not connected.</summary>
    NotConnected = 0x8C,

    /// <summary>The topic filter is invalid.</summary>
    TopicFilterInvalid = 0x8F,

    /// <summary>The topic name is invalid.</summary>
    TopicNameInvalid = 0x90,

    /// <summary>The packet identifier is already in use.</summary>
    PacketIdentifierInUse = 0x91,

    /// <summary>The packet identifier was not found.</summary>
    PacketIdentifierNotFound = 0x92,

    /// <summary>The receive maximum was exceeded.</summary>
    ReceiveMaximumExceeded = 0x93,

    /// <summary>The server is shutting down.</summary>
    ServerShuttingDown = 0x8B,

    /// <summary>The client session was taken over by another connection.</summary>
    SessionTakenOver = 0x8E,

    /// <summary>The keep-alive timeout expired.</summary>
    KeepAliveTimeout = 0x8D,

    /// <summary>The payload format is invalid.</summary>
    PayloadFormatInvalid = 0x99
}
