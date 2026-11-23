namespace MqttZero.Protocol;

/// <summary>
/// Defines when retained messages are sent for a subscription.
/// </summary>
public enum MqttRetainHandling : byte
{
    /// <summary>Send retained messages when the subscription is created.</summary>
    SendAtSubscribe = 0,

    /// <summary>Send retained messages only when the subscription is new.</summary>
    SendAtSubscribeIfNewSubscriptionOnly = 1,

    /// <summary>Do not send retained messages when subscribing.</summary>
    DoNotSendOnSubscribe = 2
}
