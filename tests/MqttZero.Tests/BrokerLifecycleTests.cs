namespace MqttZero.Tests
{
    public sealed class BrokerLifecycleTests
    {
        [Fact]
        public async Task LifecycleTransitionsAndSnapshotsAreAvailable()
        {
            MqttZeroBroker broker = new(new MqttZeroOptions(), TimeProvider.System);

            await broker.StartAsync(TestContext.Current.CancellationToken);

            Assert.Equal(MqttZeroBrokerState.Running, broker.State);
            Assert.Empty(broker.GetClients());

            await broker.PublishAsync(
                "devices/a",
                "payload"u8.ToArray(),
                MqttQualityOfServiceLevel.AtMostOnce,
                true,
                TestContext.Current.CancellationToken);
            MqttZeroRetainedMessageSnapshot retained = Assert.Single(broker.GetRetainedMessages());
            Assert.Equal("devices/a", retained.Topic);
            Assert.Equal(7, retained.PayloadLength);

            await broker.PublishAsync(
                "devices/a",
                ReadOnlyMemory<byte>.Empty,
                MqttQualityOfServiceLevel.AtMostOnce,
                true,
                TestContext.Current.CancellationToken);
            Assert.Empty(broker.GetRetainedMessages());

            await broker.StopAsync(TestContext.Current.CancellationToken);
            Assert.Equal(MqttZeroBrokerState.Stopped, broker.State);
        }
    }
}
