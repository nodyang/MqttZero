using MqttQualityOfServiceLevel = MqttZero.Protocol.MqttQualityOfServiceLevel;
using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using System.Buffers;
using System.IO.Pipelines;
using System.Net;
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

        [Fact(Timeout = 10_000)]
        public async Task StopDrainsConnectionBeforeConnectCompletes()
        {
            MqttZeroBroker broker = MqttZeroBroker.Create(
                new MqttZeroOptions(),
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            Pipe input = new();
            Pipe output = new();
            Task accept = broker.AcceptConnectionAsync(
                input.Reader,
                output.Writer,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                null,
                true,
                CancellationToken.None).AsTask();

            Assert.Equal(1, broker.ConnectedClientCount);
            await broker.StopAsync(TestContext.Current.CancellationToken);
            await accept;

            Assert.Equal(0, broker.ConnectedClientCount);
            Assert.Equal(MqttZeroBrokerState.Stopped, broker.State);
            await broker.DisposeAsync();
        }

        [Fact(Timeout = 10_000)]
        public async Task ConcurrentStopAndDisposeDrainConnectedClient()
        {
            MqttZeroBroker broker = MqttZeroBroker.Create(
                new MqttZeroOptions(),
                TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            Pipe input = new();
            Pipe output = new();
            Task accept = broker.AcceptConnectionAsync(
                input.Reader,
                output.Writer,
                new IPEndPoint(IPAddress.Loopback, 0),
                false,
                false,
                null,
                true,
                CancellationToken.None).AsTask();
            ArrayBufferWriter<byte> writer = new();
            MqttPacketCodec.Encode(
                new MqttConnectPacket
                {
                    ClientId = new ReadOnlySequence<byte>("lifecycle-client"u8.ToArray()),
                    CleanStart = true
                },
                writer);
            await input.Writer.WriteAsync(
                writer.WrittenMemory,
                TestContext.Current.CancellationToken);
            _ = await ReadConnAckAsync(
                output.Reader,
                TestContext.Current.CancellationToken);

            Task stop = broker.StopAsync(
                TestContext.Current.CancellationToken).AsTask();
            Task dispose = broker.DisposeAsync().AsTask();
            await Task.WhenAll(stop, dispose, accept);

            Assert.Equal(0, broker.ConnectedClientCount);
            Assert.Equal(MqttZeroBrokerState.Stopped, broker.State);
        }

        private static async Task<MqttConnAckPacket> ReadConnAckAsync(
            PipeReader reader,
            CancellationToken cancellationToken)
        {
            while (true)
            {
                ReadResult result = await reader.ReadAsync(cancellationToken);
                if (MqttPacketCodec.TryDecode(
                    result.Buffer,
                    out MqttPacket? packet,
                    out long consumed))
                {
                    reader.AdvanceTo(result.Buffer.GetPosition(consumed));
                    return Assert.IsType<MqttConnAckPacket>(packet);
                }

                reader.AdvanceTo(result.Buffer.Start, result.Buffer.End);
                if (result.IsCompleted)
                {
                    throw new EndOfStreamException();
                }
            }
        }
    }
}
