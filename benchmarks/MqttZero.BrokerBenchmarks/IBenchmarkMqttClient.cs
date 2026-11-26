using MqttZero.Protocol;

namespace MqttZero.BrokerBenchmarks;

internal interface IBenchmarkMqttClient : IAsyncDisposable
{
    bool IsConnected { get; }

    BenchmarkDeliveryTracker Delivery { get; }

    long ReceivedMessageCount { get; }

    ValueTask ConnectAsync(
        string clientId,
        CancellationToken cancellationToken);

    ValueTask SubscribeAsync(
        string topicFilter,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        CancellationToken cancellationToken);

    ValueTask PublishAsync(
        string topic,
        ReadOnlyMemory<byte> payload,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        CancellationToken cancellationToken,
        uint? messageExpiryInterval = null);

    ValueTask DisconnectAsync(CancellationToken cancellationToken);
}
