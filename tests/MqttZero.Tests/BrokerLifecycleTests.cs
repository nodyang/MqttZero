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
                false,
                TestContext.Current.CancellationToken);

            await broker.StopAsync(TestContext.Current.CancellationToken);
            Assert.Equal(MqttZeroBrokerState.Stopped, broker.State);
        }
    }
}
