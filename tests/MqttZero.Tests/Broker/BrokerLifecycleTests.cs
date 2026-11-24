using MqttQualityOfServiceLevel = MqttZero.Protocol.MqttQualityOfServiceLevel;
using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using System.Buffers;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using MqttZero.AspNetCore;

namespace MqttZero.Tests.Broker
{
    public sealed class BrokerLifecycleTests
    {
        [Fact]
        public async Task LifecycleTransitionsAndSnapshotsAreAvailable()
        {
            MqttZeroBroker broker = MqttZeroBroker.Create(
                new MqttZeroOptions(),
                TimeProvider.System);

            await broker.StartAsync(TestContext.Current.CancellationToken);

            Assert.Equal(MqttZeroBrokerState.Running, broker.State);
            Assert.Empty(broker.GetClients());

            await broker.PublishAsync(
                "devices/a",
                new ReadOnlySequence<byte>("payload"u8.ToArray()),
                MqttQualityOfServiceLevel.AtMostOnce,
                true,
                TestContext.Current.CancellationToken);
            MqttZeroRetainedMessageSnapshot retained = Assert.Single(broker.GetRetainedMessages());
            Assert.Equal("devices/a", retained.Topic);
            Assert.Equal(7, retained.PayloadLength);

            await broker.PublishAsync(
                "devices/a",
                ReadOnlySequence<byte>.Empty,
                MqttQualityOfServiceLevel.AtMostOnce,
                true,
                TestContext.Current.CancellationToken);
            Assert.Empty(broker.GetRetainedMessages());

            await broker.StopAsync(TestContext.Current.CancellationToken);
            Assert.Equal(MqttZeroBrokerState.Stopped, broker.State);
        }

        [Fact]
        public async Task HealthCheckReportsBrokerState()
        {
            MqttZeroBroker broker = MqttZeroBroker.Create(
                new MqttZeroOptions(),
                TimeProvider.System);
            MqttZeroHealthCheck healthCheck = new(broker);

            HealthCheckResult stopped = await healthCheck.CheckHealthAsync(
                new HealthCheckContext(),
                TestContext.Current.CancellationToken);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            HealthCheckResult running = await healthCheck.CheckHealthAsync(
                new HealthCheckContext(),
                TestContext.Current.CancellationToken);

            Assert.Equal(HealthStatus.Unhealthy, stopped.Status);
            Assert.Equal(HealthStatus.Healthy, running.Status);
        }

        [Fact]
        public async Task BrokerCanRestartAfterStopButNotAfterDispose()
        {
            MqttZeroBroker broker = MqttZeroBroker.Create(
                new MqttZeroOptions(),
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            await broker.StopAsync(TestContext.Current.CancellationToken);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            Assert.Equal(MqttZeroBrokerState.Running, broker.State);
            await broker.DisposeAsync();

            await Assert.ThrowsAsync<ObjectDisposedException>(
                async () => await broker.StartAsync(
                    TestContext.Current.CancellationToken));
        }
    }
}
