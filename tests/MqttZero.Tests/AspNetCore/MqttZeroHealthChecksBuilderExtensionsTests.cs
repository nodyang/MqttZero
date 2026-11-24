using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using MqttZero.AspNetCore;

namespace MqttZero.Tests.AspNetCore
{
    public sealed class MqttZeroHealthChecksBuilderExtensionsTests
    {
        [Fact]
        public async Task AddMqttZeroHealthCheckRegistersNamedCheckWithMetadata()
        {
            ServiceCollection services = new();
            services.AddMqttZero();
            services.AddHealthChecks()
                .AddMqttZeroHealthCheck(
                    "ready",
                    HealthStatus.Degraded,
                    ["mqtt", "broker"],
                    TimeSpan.FromSeconds(3));
            await using ServiceProvider provider = services.BuildServiceProvider();
            HealthCheckServiceOptions options =
                provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value;

            HealthCheckRegistration registration =
                Assert.Single(options.Registrations);
            Assert.Equal("ready", registration.Name);
            Assert.Equal(HealthStatus.Degraded, registration.FailureStatus);
            Assert.Equal(TimeSpan.FromSeconds(3), registration.Timeout);
            Assert.Equal(["mqtt", "broker"], registration.Tags);
            Assert.Equal(
                typeof(MqttZeroHealthCheck),
                registration.Factory(provider).GetType());
        }
    }
}
