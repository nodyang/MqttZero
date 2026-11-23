using MqttZero.Broker;
using MqttZero.Interceptors;
using Microsoft.Extensions.Hosting;

namespace MqttZero.AspNetCore;

internal sealed class MqttZeroHostedService : IHostedService
{
    private readonly MqttZeroBroker _broker;

    public MqttZeroHostedService(MqttZeroBroker broker)
    {
        _broker = broker;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        return _broker.StartAsync(cancellationToken).AsTask();
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return _broker.StopAsync(cancellationToken).AsTask();
    }
}
