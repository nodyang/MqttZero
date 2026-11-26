using MQTTnet;

namespace MqttZero.BrokerBenchmarks;

internal sealed class MqttNetBenchmarkMqttClientFactory :
    IBenchmarkMqttClientFactory
{
    private readonly MqttClientFactory _factory = new();
    private readonly int _port;

    public MqttNetBenchmarkMqttClientFactory(int port)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(port);
        _port = port;
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
            delivery);
    }
}
