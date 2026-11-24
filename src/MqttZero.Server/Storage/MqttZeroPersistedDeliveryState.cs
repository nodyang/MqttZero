namespace MqttZero.Storage;

/// <summary>Describes persisted delivery progress.</summary>
public enum MqttZeroPersistedDeliveryState
{
    /// <summary>Queued for delivery.</summary>
    Queued,
    /// <summary>Awaiting PUBACK.</summary>
    AwaitPubAck,
    /// <summary>Awaiting PUBREC.</summary>
    AwaitPubRec,
    /// <summary>Awaiting PUBCOMP.</summary>
    AwaitPubComp
}
