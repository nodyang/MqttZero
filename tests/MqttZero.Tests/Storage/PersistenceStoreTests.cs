using Microsoft.Extensions.DependencyInjection;
using MqttZero.Server;
using MqttZero.AspNetCore;
using MqttZero.Server.Broker;
using MqttZero.Protocol;
using MqttZero.Server.Storage;

namespace MqttZero.Tests.Storage
{
    public sealed class PersistenceStoreTests
    {
        [Fact]
        public async Task BrokerRestoresSessionsFromCustomStore()
        {
            TestSessionStore sessionStore = new();
            sessionStore.Sessions["restored"] = new MqttZeroPersistedSession
            {
                ClientId = "restored",
                ExpiryInterval = 60,
                Subscriptions =
                [
                    new MqttZeroPersistedSubscription
                    {
                        TopicFilter = "restored/#"u8.ToArray(),
                        QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce
                    }
                ]
            };
            ServiceCollection services = new();
            services.AddSingleton<IMqttZeroSessionStore>(sessionStore);
            services.AddSingleton<IMqttZeroMessageStore, TestMessageStore>();
            services.AddSingleton<IMqttZeroRetainedMessageStore, TestRetainedStore>();
            services.AddMqttZero();
            await using ServiceProvider provider = services.BuildServiceProvider();
            MqttZeroBroker broker = provider.GetRequiredService<MqttZeroBroker>();

            await broker.StartAsync(TestContext.Current.CancellationToken);

            MqttZeroSessionSnapshot session = Assert.Single(broker.GetSessions());
            Assert.Equal("restored", session.ClientId);
            Assert.Equal(1, session.SubscriptionCount);
        }

        private sealed class TestSessionStore : IMqttZeroSessionStore
        {
            public bool IsEnabled
            {
                get
                {
                    return true;
                }
            }

            public Dictionary<string, MqttZeroPersistedSession> Sessions { get; } = [];

            public ValueTask<IReadOnlyList<MqttZeroPersistedSession>> LoadAllAsync(
                CancellationToken cancellationToken)
            {
                return ValueTask.FromResult<IReadOnlyList<MqttZeroPersistedSession>>(
                    Sessions.Values.ToArray());
            }

            public ValueTask SaveAsync(
                MqttZeroPersistedSession session,
                CancellationToken cancellationToken)
            {
                Sessions[session.ClientId] = session;
                return ValueTask.CompletedTask;
            }

            public ValueTask DeleteAsync(string clientId, CancellationToken cancellationToken)
            {
                Sessions.Remove(clientId);
                return ValueTask.CompletedTask;
            }
        }

        private sealed class TestMessageStore : IMqttZeroMessageStore
        {
            public bool IsEnabled
            {
                get
                {
                    return true;
                }
            }

            public ValueTask<IReadOnlyList<MqttZeroPersistedMessage>> LoadAllAsync(
                CancellationToken cancellationToken)
            {
                return ValueTask.FromResult<IReadOnlyList<MqttZeroPersistedMessage>>([]);
            }

            public ValueTask SaveQueuedAsync(MqttZeroPersistedMessage message, CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }

            public ValueTask SaveOutboundAsync(MqttZeroPersistedMessage message, CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }

            public ValueTask SaveInboundQos2Async(MqttZeroPersistedMessage message, CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }

            public ValueTask DeleteOutboundAsync(string clientId, long messageId, CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }

            public ValueTask DeleteInboundQos2Async(string clientId, ushort packetIdentifier, CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }

            public ValueTask DeleteByClientAsync(
                string clientId,
                CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }

            public ValueTask ReplaceQueuedAsync(
                string clientId,
                long replacedMessageId,
                MqttZeroPersistedMessage message,
                CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }
        }

        private sealed class TestRetainedStore : IMqttZeroRetainedMessageStore
        {
            public bool IsEnabled
            {
                get
                {
                    return true;
                }
            }

            public ValueTask<IReadOnlyList<MqttZeroPersistedRetainedMessage>> LoadAllAsync(
                CancellationToken cancellationToken)
            {
                return ValueTask.FromResult<IReadOnlyList<MqttZeroPersistedRetainedMessage>>([]);
            }

            public ValueTask SaveAsync(MqttZeroPersistedRetainedMessage message, CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }

            public ValueTask DeleteAsync(string topic, CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }

            public ValueTask ClearAsync(CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }
        }
    }
}
