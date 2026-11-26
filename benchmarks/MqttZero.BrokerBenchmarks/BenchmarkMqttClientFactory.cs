namespace MqttZero.BrokerBenchmarks;

internal sealed class BenchmarkMqttClientFactory :
    IBenchmarkMqttClientFactory
{
    private readonly MqttNetBenchmarkMqttClientFactory _mqttNetFactory;
    private readonly int _port;
    private readonly BenchmarkClientKind _clientKind;
    private readonly Func<BenchmarkDeliveryTracker> _trackerFactory;
    private readonly TimeSpan _receiveDelay;

    public BenchmarkMqttClientFactory(
        BenchmarkClientKind clientKind,
        int port,
        Func<BenchmarkDeliveryTracker>? trackerFactory = null,
        TimeSpan receiveDelay = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(port);
        _clientKind = clientKind;
        _port = port;
        _receiveDelay = receiveDelay;
        _trackerFactory = trackerFactory ??
            (() => new BenchmarkDeliveryTracker());
        _mqttNetFactory = new MqttNetBenchmarkMqttClientFactory(
            port,
            receiveDelay);
    }

    public IBenchmarkMqttClient Create()
    {
        BenchmarkDeliveryTracker tracker = _trackerFactory();
        return _clientKind switch
        {
            BenchmarkClientKind.MqttNet => _mqttNetFactory.Create(tracker),
            BenchmarkClientKind.Raw => new RawMqttClient(
                new RawMqttClientOptions
                {
                    Host = "127.0.0.1",
                    Port = _port
                },
                tracker,
                _receiveDelay),
            _ => throw new ArgumentOutOfRangeException(
                nameof(_clientKind))
        };
    }
}
