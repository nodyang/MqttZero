using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
namespace MqttZero.Tests
{
    public sealed class OptionsTests
    {
        [Fact]
        public void DefaultsMatchDocumentedCapacity()
        {
            MqttZeroOptions options = new();

            Assert.Equal(20_000, options.MaxConnections);
            Assert.Equal(20_000, options.MaxSessions);
            Assert.Equal(1_000, options.MaxSubscriptionsPerSession);
            Assert.Equal(32, options.MaxInflight);
            Assert.Equal(1_000, options.MaxMessageQueueLength);
            Assert.True(options.StoreQos0InQueue);
            Assert.Equal(1024 * 1024, options.MaxPacketSize);
            Assert.Equal(512L * 1024 * 1024, options.MaxBufferedBytes);
            Assert.Equal(100_000, options.MaxRetainedMessages);
            Assert.Equal(256L * 1024 * 1024, options.MaxRetainedBytes);
            Assert.Equal(TimeSpan.FromSeconds(10), options.ConnectTimeout);
            Assert.Equal(TimeSpan.FromSeconds(30), options.IncompletePacketTimeout);
            Assert.Equal(TimeSpan.FromHours(24), options.MaxSessionExpiryInterval);
            Assert.Equal(TimeSpan.FromMilliseconds(500), options.KeepAliveCheckInterval);
            Assert.Equal((ushort)32, options.TopicAliasMaximum);
        }
    }
}
