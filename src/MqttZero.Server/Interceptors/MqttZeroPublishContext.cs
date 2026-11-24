using MqttZero.Protocol;
using MqttZero.Clients;
using System.Buffers;
using System.Collections.Concurrent;

namespace MqttZero.Interceptors;

/// <summary>
/// Contains mutable publish data for an interceptor.
/// </summary>
public sealed class MqttZeroPublishContext
{
    private static readonly ConcurrentQueue<MqttZeroPublishContext> ContextPool = new();

    /// <summary>
    /// Gets or sets the UTF-8 topic name.
    /// </summary>
    public ReadOnlySequence<byte> Topic { get; set; }

    /// <summary>
    /// Gets or sets the payload sequence.
    /// </summary>
    public ReadOnlySequence<byte> Payload { get; set; }

    /// <summary>
    /// Gets or sets the MQTT 5 property section.
    /// </summary>
    public ReadOnlySequence<byte> Properties { get; set; }

    /// <summary>
    /// Gets or sets the quality of service.
    /// </summary>
    public MqttQualityOfServiceLevel QualityOfServiceLevel { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether the message is retained.
    /// </summary>
    public bool Retain { get; set; }

    /// <summary>Gets the originating connected client, or null for server-originated messages.</summary>
    public MqttZeroConnectedClient? Sender { get; internal set; }

    /// <summary>Gets or sets the result reason code.</summary>
    public MqttReasonCode ResultReasonCode { get; set; } = MqttReasonCode.Success;

    /// <summary>Gets or sets an optional result reason string.</summary>
    public string? ReasonString { get; set; }

    internal static MqttZeroPublishContext Rent()
    {
        if (ContextPool.TryDequeue(out MqttZeroPublishContext? context))
        {
            return context;
        }

        return new MqttZeroPublishContext();
    }

    internal void Release()
    {
        Topic = ReadOnlySequence<byte>.Empty;
        Payload = ReadOnlySequence<byte>.Empty;
        Properties = ReadOnlySequence<byte>.Empty;
        QualityOfServiceLevel = MqttQualityOfServiceLevel.AtMostOnce;
        Retain = false;
        Sender = null;
        ResultReasonCode = MqttReasonCode.Success;
        ReasonString = null;
        ContextPool.Enqueue(this);
    }
}
