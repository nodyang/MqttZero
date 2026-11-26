using MQTTnet;
using MqttZero.Protocol;
using System.Buffers;
using MqttQos = MQTTnet.Protocol.MqttQualityOfServiceLevel;

namespace MqttZero.BrokerBenchmarks;

internal sealed class MqttNetBenchmarkMqttClient : IBenchmarkMqttClient
{
    private readonly IMqttClient _client;
    private readonly BenchmarkDeliveryTracker _delivery;
    private readonly int _port;
    private readonly TimeSpan _receiveDelay;

    public MqttNetBenchmarkMqttClient(
        IMqttClient client,
        int port,
        BenchmarkDeliveryTracker? delivery = null,
        TimeSpan receiveDelay = default)
    {
        _client = client;
        _port = port;
        _delivery = delivery ?? new BenchmarkDeliveryTracker();
        _receiveDelay = receiveDelay;
        _client.ApplicationMessageReceivedAsync += message =>
        {
            _delivery.OnMessageReceived(message.ApplicationMessage.Payload);
            return _receiveDelay > TimeSpan.Zero
                ? Task.Delay(_receiveDelay)
                : Task.CompletedTask;
        };
    }

    public bool IsConnected
    {
        get
        {
            return _client.IsConnected;
        }
    }

    public long ReceivedMessageCount
    {
        get
        {
            return _delivery.ReceivedMessages;
        }
    }

    public BenchmarkDeliveryTracker Delivery
    {
        get
        {
            return _delivery;
        }
    }

    public async ValueTask ConnectAsync(
        string clientId,
        CancellationToken cancellationToken)
    {
        MqttClientOptions options = new MqttClientOptionsBuilder()
            .WithTcpServer("127.0.0.1", _port)
            .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
            .WithClientId(clientId)
            .WithCleanStart()
            .Build();
        await _client.ConnectAsync(options, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask SubscribeAsync(
        string topicFilter,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        CancellationToken cancellationToken)
    {
        MqttClientSubscribeOptions options =
            new MqttClientSubscribeOptionsBuilder()
                .WithTopicFilter(
                    topicFilter,
                    ToMqttNetQos(qualityOfServiceLevel))
                .Build();
        await _client.SubscribeAsync(options, cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask PublishAsync(
        string topic,
        ReadOnlyMemory<byte> payload,
        MqttQualityOfServiceLevel qualityOfServiceLevel,
        CancellationToken cancellationToken,
        uint? messageExpiryInterval = null)
    {
        MqttApplicationMessageBuilder builder =
            new MqttApplicationMessageBuilder()
            .WithTopic(topic)
            .WithPayload(new ReadOnlySequence<byte>(payload))
            .WithQualityOfServiceLevel(ToMqttNetQos(qualityOfServiceLevel));
        if (messageExpiryInterval.HasValue)
        {
            builder = builder.WithMessageExpiryInterval(
                messageExpiryInterval.Value);
        }

        await _client.PublishAsync(
            builder.Build(),
            cancellationToken).ConfigureAwait(false);
    }

    public async ValueTask DisconnectAsync(CancellationToken cancellationToken)
    {
        if (_client.IsConnected)
        {
            await _client.DisconnectAsync(
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }
    }

    public ValueTask DisposeAsync()
    {
        _client.Dispose();
        return ValueTask.CompletedTask;
    }

    private static MqttQos ToMqttNetQos(
        MqttQualityOfServiceLevel qualityOfServiceLevel)
    {
        return qualityOfServiceLevel switch
        {
            MqttQualityOfServiceLevel.AtMostOnce => MqttQos.AtMostOnce,
            MqttQualityOfServiceLevel.AtLeastOnce => MqttQos.AtLeastOnce,
            MqttQualityOfServiceLevel.ExactlyOnce => MqttQos.ExactlyOnce,
            _ => throw new ArgumentOutOfRangeException(
                nameof(qualityOfServiceLevel))
        };
    }
}
