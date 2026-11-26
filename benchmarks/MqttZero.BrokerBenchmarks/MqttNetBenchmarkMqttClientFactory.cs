using MQTTnet;

namespace MqttZero.BrokerBenchmarks;

internal sealed class MqttNetBenchmarkMqttClientFactory :
    IBenchmarkMqttClientFactory
{
    private readonly MqttClientFactory _factory = new();
    private readonly int _port;
    private readonly TimeSpan _receiveDelay;

    public MqttNetBenchmarkMqttClientFactory(
        int port,
        TimeSpan receiveDelay = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(port);
        _port = port;
        _receiveDelay = receiveDelay;
    }

    public IBenchmarkMqttClient Create()
    {
        return Create(null);
    }

    public IBenchmarkMqttClient Create(
        BenchmarkDeliveryTracker? delivery)
    {
        return new MqttNetBenchmarkMqttClient(
            _factory.CreateMqttClient(),
            _port,
            delivery,
            _receiveDelay);
    }
}
