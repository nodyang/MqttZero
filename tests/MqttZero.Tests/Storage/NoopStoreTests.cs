using MqttZero.Protocol;
using MqttZero.Server.Storage;

namespace MqttZero.Tests.Storage
{
    public sealed class NoopStoreTests
    {
        [Fact]
        public async Task SessionStoreIsDisabledAndIgnoresMutations()
        {
            MqttZeroNoopSessionStore store = new();
            MqttZeroPersistedSession session = new()
            {
                ClientId = "client"
            };

            Assert.False(store.IsEnabled);
            Assert.Empty(await store.LoadAllAsync(
                TestContext.Current.CancellationToken));
            await store.SaveAsync(
                session,
                TestContext.Current.CancellationToken);
            await store.DeleteAsync(
                "client",
                TestContext.Current.CancellationToken);
        }

        [Fact]
        public async Task MessageStoreIsDisabledAndIgnoresMutations()
        {
            MqttZeroNoopMessageStore store = new();
            MqttZeroPersistedMessage message = CreateMessage();

            Assert.False(store.IsEnabled);
            Assert.Empty(await store.LoadAllAsync(
                TestContext.Current.CancellationToken));
            await store.SaveQueuedAsync(
                message,
                TestContext.Current.CancellationToken);
            await store.SaveOutboundAsync(
                message,
                TestContext.Current.CancellationToken);
            await store.SaveInboundQos2Async(
                message,
                TestContext.Current.CancellationToken);
            await store.TransitionQueuedToOutboundAsync(
                "client",
                1,
                message,
                TestContext.Current.CancellationToken);
            await store.DeleteMessageAsync(
                "client",
                1,
                TestContext.Current.CancellationToken);
            await store.DeleteInboundQos2Async(
                "client",
                1,
                TestContext.Current.CancellationToken);
            await store.DeleteByClientAsync(
                "client",
                TestContext.Current.CancellationToken);
            await store.ReplaceQueuedAsync(
                "client",
                1,
                message,
                TestContext.Current.CancellationToken);
        }

        [Fact]
        public async Task RetainedStoreIsDisabledAndIgnoresMutations()
        {
            MqttZeroNoopRetainedMessageStore store = new();
            MqttZeroPersistedRetainedMessage message = new()
            {
                Topic = "topic"u8.ToArray()
            };

            Assert.False(store.IsEnabled);
            Assert.Empty(await store.LoadAllAsync(
                TestContext.Current.CancellationToken));
            await store.SaveAsync(
                message,
                TestContext.Current.CancellationToken);
            await store.DeleteAsync(
                "topic",
                TestContext.Current.CancellationToken);
            await store.ClearAsync(TestContext.Current.CancellationToken);
        }

        private static MqttZeroPersistedMessage CreateMessage()
        {
            return new MqttZeroPersistedMessage
            {
                ClientId = "client",
                MessageId = 1,
                PacketIdentifier = 1,
                Topic = "topic"u8.ToArray(),
                QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce
            };
        }
    }
}
