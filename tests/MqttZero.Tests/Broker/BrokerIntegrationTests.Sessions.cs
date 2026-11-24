using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using MqttQualityOfServiceLevel = MqttZero.Protocol.MqttQualityOfServiceLevel;
using System.Buffers;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MQTTnet.Protocol;
using MqttZero.AspNetCore;

namespace MqttZero.Tests.Broker
{
    public sealed partial class BrokerIntegrationTests
    {
        [Fact(Timeout = 15_000)]
        public async Task RestoresPersistentSessionAndSubscription()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            MqttClientFactory factory = new();

            using (IMqttClient initial = factory.CreateMqttClient())
            {
                await initial.ConnectAsync(CreatePersistentClientOptions(port, "persistent-client"), cancellationToken);
                await initial.SubscribeAsync(
                    new MqttClientSubscribeOptionsBuilder()
                        .WithTopicFilter(
                            "persistent/#",
                            MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                        .Build(),
                    cancellationToken);
                await initial.DisconnectAsync(cancellationToken: cancellationToken);
            }

            using IMqttClient restored = factory.CreateMqttClient();
            using IMqttClient publisher = factory.CreateMqttClient();
            TaskCompletionSource<string> received = new(TaskCreationOptions.RunContinuationsAsynchronously);
            restored.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.TrySetResult(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };

            MQTTnet.MqttClientConnectResult result =
                await restored.ConnectAsync(CreatePersistentClientOptions(port, "persistent-client"), cancellationToken);
            await publisher.ConnectAsync(CreateClientOptions(port, "persistent-publisher"), cancellationToken);

            Assert.True(result.IsSessionPresent);

            await publisher.PublishStringAsync(
                "persistent/value",
                "restored",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
                cancellationToken: cancellationToken);

            Assert.Equal(
                "restored",
                await received.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken));

            await publisher.DisconnectAsync(cancellationToken: cancellationToken);
            await restored.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }
        [Fact(Timeout = 15_000)]
        public async Task ExpiresDisconnectedSession()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            MqttClientFactory factory = new();
            using IMqttClient client = factory.CreateMqttClient();

            await client.ConnectAsync(
                new MqttClientOptionsBuilder()
                    .WithTcpServer("127.0.0.1", port)
                    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
                    .WithClientId("expiring-client")
                    .WithCleanStart(false)
                    .WithSessionExpiryInterval(1)
                    .Build(),
                cancellationToken);
            await client.DisconnectAsync(cancellationToken: cancellationToken);

            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
            Assert.DoesNotContain(
                broker.GetSessions(),
                session => session.ClientId == "expiring-client");
            await app.StopAsync(cancellationToken);
        }
        [Fact(Timeout = 15_000)]
        public async Task TakesOverExistingClientId()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient first = factory.CreateMqttClient();
            using IMqttClient second = factory.CreateMqttClient();
            TaskCompletionSource<MQTTnet.MqttClientDisconnectReason> firstDisconnected =
                new(TaskCreationOptions.RunContinuationsAsynchronously);
            first.DisconnectedAsync += eventArgs =>
            {
                firstDisconnected.TrySetResult(eventArgs.Reason);
                return Task.CompletedTask;
            };

            await first.ConnectAsync(CreatePersistentClientOptions(port, "takeover-client"), cancellationToken);
            MQTTnet.MqttClientConnectResult result =
                await second.ConnectAsync(CreatePersistentClientOptions(port, "takeover-client"), cancellationToken);

            Assert.True(result.IsSessionPresent);
            Assert.Equal(
                MQTTnet.MqttClientDisconnectReason.SessionTakenOver,
                await firstDisconnected.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken));

            await second.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }
        [Fact(Timeout = 10_000)]
        public async Task ClampsSessionExpiryInConnAck()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(
                port,
                options => options.MaxSessionExpiryInterval = TimeSpan.FromSeconds(1));
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient client = factory.CreateMqttClient();

            MQTTnet.MqttClientConnectResult result = await client.ConnectAsync(
                new MqttClientOptionsBuilder()
                    .WithTcpServer("127.0.0.1", port)
                    .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
                    .WithClientId("clamped-client")
                    .WithCleanStart(false)
                    .WithSessionExpiryInterval(60)
                    .Build(),
                cancellationToken);

            Assert.Equal(1U, result.SessionExpiryInterval);
            await client.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }
        [Fact(Timeout = 10_000)]
        public async Task DisconnectCanOverrideSessionExpiryToZero()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            using TcpClient client = new();
            await client.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
            NetworkStream stream = client.GetStream();
            await SendPacketAsync(
                stream,
                new MqttConnectPacket
                {
                    ClientId = new ReadOnlySequence<byte>("disconnect-expiry"u8.ToArray()),
                    CleanStart = false,
                    KeepAliveSeconds = 30,
                    SessionExpiryInterval = 60
                },
                cancellationToken);
            _ = Assert.IsType<MqttConnAckPacket>(await ReadPacketAsync(stream, cancellationToken));
            await SendPacketAsync(
                stream,
                new MqttDisconnectPacket
                {
                    SessionExpiryInterval = 0
                },
                cancellationToken);
            using CancellationTokenSource closeTimeout =
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            closeTimeout.CancelAfter(TimeSpan.FromSeconds(2));
            byte[] endOfStream = new byte[1];
            _ = await stream.ReadAsync(endOfStream, closeTimeout.Token);
            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);

            Assert.DoesNotContain(
                broker.GetSessions(),
                session => session.ClientId == "disconnect-expiry");
            await app.StopAsync(cancellationToken);
        }
    }
}
