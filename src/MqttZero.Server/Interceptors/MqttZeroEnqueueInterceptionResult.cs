namespace MqttZero.Server.Interceptors;

/// <summary>Controls one receiver delivery.</summary>
public enum MqttZeroEnqueueInterceptionResult
{
    /// <summary>Enqueue or send the delivery.</summary>
    Enqueue,
    /// <summary>Drop this receiver delivery.</summary>
    Drop,
    /// <summary>Close the sender connection.</summary>
    CloseSenderConnection
}
