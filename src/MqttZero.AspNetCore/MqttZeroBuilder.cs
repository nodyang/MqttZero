using Microsoft.Extensions.DependencyInjection;

namespace MqttZero.AspNetCore;

internal sealed class MqttZeroBuilder : IMqttZeroBuilder
{
    public MqttZeroBuilder(IServiceCollection services)
    {
        Services = services;
    }

    public IServiceCollection Services { get; }
}
