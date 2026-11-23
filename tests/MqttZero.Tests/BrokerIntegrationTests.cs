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

namespace MqttZero.Tests
{
    public sealed class BrokerIntegrationTests
    {
        [Fact(Timeout = 15_000)]
        public async Task RoutesPublishedMessageToSubscriber()
        {
            int port = GetFreePort();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
            {
                kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero());
            });
            builder.Services.AddMqttZero();

            await using WebApplication app = builder.Build();
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            await app.StartAsync(cancellationToken);
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            Assert.Equal(MqttZeroBrokerState.Running, broker.State);

            MqttClientFactory factory = new();
            using IMqttClient subscriber = factory.CreateMqttClient();
            using IMqttClient publisher = factory.CreateMqttClient();
            TaskCompletionSource<string> received = new(TaskCreationOptions.RunContinuationsAsynchronously);

            subscriber.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.TrySetResult(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };

            MQTTnet.MqttClientOptions subscriberOptions = CreateClientOptions(port, "subscriber");
            MQTTnet.MqttClientOptions publisherOptions = CreateClientOptions(port, "publisher");

            await subscriber.ConnectAsync(subscriberOptions, cancellationToken);
            await publisher.ConnectAsync(publisherOptions, cancellationToken);
            Assert.True(broker.ConnectedClientCount >= 2);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(
                        "devices/+/temperature",
                        MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build(),
                cancellationToken);

            await publisher.PublishStringAsync(
                "devices/a/temperature",
                "21.5",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
                cancellationToken: cancellationToken);

            string payload = await received.Task.WaitAsync(
                TimeSpan.FromSeconds(5),
                cancellationToken);
            Assert.Equal("21.5", payload);

            await publisher.DisconnectAsync(cancellationToken: cancellationToken);
            await subscriber.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 15_000)]
        public async Task RoutesWebSocketMessagesUsingMqttSubProtocol()
        {
            int port = GetFreePort();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.UseUrls($"http://127.0.0.1:{port}");
            builder.Services.AddMqttZero();

            await using WebApplication app = builder.Build();
            app.UseWebSockets();
            app.MapMqttZero("/mqtt");

            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            await app.StartAsync(cancellationToken);

            MqttClientFactory factory = new();
            using IMqttClient subscriber = factory.CreateMqttClient();
            using IMqttClient publisher = factory.CreateMqttClient();
            TaskCompletionSource<string> received = new(TaskCreationOptions.RunContinuationsAsynchronously);

            subscriber.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.TrySetResult(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };

            MQTTnet.MqttClientOptions subscriberOptions = CreateWebSocketClientOptions(port, "ws-subscriber");
            MQTTnet.MqttClientOptions publisherOptions = CreateWebSocketClientOptions(port, "ws-publisher");

            await subscriber.ConnectAsync(subscriberOptions, cancellationToken);
            await publisher.ConnectAsync(publisherOptions, cancellationToken);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(
                        "sensors/#",
                        MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build(),
                cancellationToken);

            await publisher.PublishStringAsync(
                "sensors/a",
                "ok",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
                cancellationToken: cancellationToken);

            Assert.Equal("ok", await received.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken));

            await publisher.DisconnectAsync(cancellationToken: cancellationToken);
            await subscriber.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 15_000)]
        public async Task RoutesMessagesOverKestrelTls()
        {
            int port = GetFreePort();
            using X509Certificate2 certificate = CreateServerCertificate();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
            {
                kestrel.Listen(
                    IPAddress.Loopback,
                    port,
                    listen => listen.UseHttps(certificate).UseMqttZero());
            });
            builder.Services.AddMqttZero();

            await using WebApplication app = builder.Build();
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            await app.StartAsync(cancellationToken);

            MqttClientFactory factory = new();
            using IMqttClient subscriber = factory.CreateMqttClient();
            using IMqttClient publisher = factory.CreateMqttClient();
            TaskCompletionSource<string> received = new(TaskCreationOptions.RunContinuationsAsynchronously);
            subscriber.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.TrySetResult(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };

            await subscriber.ConnectAsync(CreateTlsClientOptions(port, "tls-subscriber"), cancellationToken);
            await publisher.ConnectAsync(CreateTlsClientOptions(port, "tls-publisher"), cancellationToken);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(
                        "secure/#",
                        MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build(),
                cancellationToken);

            await publisher.PublishStringAsync(
                "secure/data",
                "encrypted",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
                cancellationToken: cancellationToken);

            Assert.Equal(
                "encrypted",
                await received.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken));

            await publisher.DisconnectAsync(cancellationToken: cancellationToken);
            await subscriber.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 15_000)]
        public async Task ReplaysRetainedMessageToNewSubscriber()
        {
            int port = GetFreePort();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
            {
                kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero());
            });
            builder.Services.AddMqttZero();

            await using WebApplication app = builder.Build();
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            await app.StartAsync(cancellationToken);

            MqttClientFactory factory = new();
            using IMqttClient publisher = factory.CreateMqttClient();
            using IMqttClient subscriber = factory.CreateMqttClient();
            TaskCompletionSource<string> received = new(TaskCreationOptions.RunContinuationsAsynchronously);
            subscriber.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.TrySetResult(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };

            await publisher.ConnectAsync(CreateClientOptions(port, "retained-publisher"), cancellationToken);
            await publisher.PublishStringAsync(
                "retained/value",
                "saved",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.AtMostOnce,
                retain: true,
                cancellationToken: cancellationToken);

            await subscriber.ConnectAsync(CreateClientOptions(port, "retained-subscriber"), cancellationToken);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(
                        "retained/#",
                        MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build(),
                cancellationToken);

            Assert.Equal("saved", await received.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken));

            await subscriber.DisconnectAsync(cancellationToken: cancellationToken);
            await publisher.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 15_000)]
        public async Task RestoresPersistentSessionAndSubscription()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
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
        public async Task QueuesQos1MessageWhileSessionOffline()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();

            using (IMqttClient initial = factory.CreateMqttClient())
            {
                await initial.ConnectAsync(CreatePersistentClientOptions(port, "offline-client"), cancellationToken);
                await initial.SubscribeAsync(
                    new MqttClientSubscribeOptionsBuilder()
                        .WithTopicFilter(
                            "offline/#",
                            MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                        .Build(),
                    cancellationToken);
                await initial.DisconnectAsync(cancellationToken: cancellationToken);
            }

            using IMqttClient publisher = factory.CreateMqttClient();
            await publisher.ConnectAsync(CreateClientOptions(port, "offline-publisher"), cancellationToken);
            await publisher.PublishStringAsync(
                "offline/value",
                "queued",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
                cancellationToken: cancellationToken);

            using IMqttClient restored = factory.CreateMqttClient();
            TaskCompletionSource<string> received = new(TaskCreationOptions.RunContinuationsAsynchronously);
            restored.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.TrySetResult(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };
            MQTTnet.MqttClientConnectResult result =
                await restored.ConnectAsync(CreatePersistentClientOptions(port, "offline-client"), cancellationToken);

            Assert.True(result.IsSessionPresent);
            Assert.Equal(
                "queued",
                await received.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken));

            await publisher.DisconnectAsync(cancellationToken: cancellationToken);
            await restored.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 15_000)]
        public async Task UnsubscribeRemovesPersistentSubscription()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            MqttClientFactory factory = new();
            using IMqttClient client = factory.CreateMqttClient();

            await client.ConnectAsync(CreateClientOptions(port, "unsubscribe-client"), cancellationToken);
            await client.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(
                        "remove/#",
                        MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build(),
                cancellationToken);
            await client.UnsubscribeAsync(
                new MqttClientUnsubscribeOptionsBuilder()
                    .WithTopicFilter("remove/#")
                    .Build(),
                cancellationToken);

            MqttZeroSessionSnapshot snapshot = Assert.Single(
                broker.GetSessions(),
                session => session.ClientId == "unsubscribe-client");
            Assert.Equal(0, snapshot.SubscriptionCount);

            await client.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 15_000)]
        public async Task SharedSubscriptionSelectsOneSubscriberPerPublish()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient worker1 = factory.CreateMqttClient();
            using IMqttClient worker2 = factory.CreateMqttClient();
            using IMqttClient publisher = factory.CreateMqttClient();
            TaskCompletionSource<string> received1 = new(TaskCreationOptions.RunContinuationsAsynchronously);
            TaskCompletionSource<string> received2 = new(TaskCreationOptions.RunContinuationsAsynchronously);
            worker1.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received1.TrySetResult(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };
            worker2.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received2.TrySetResult(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };

            await worker1.ConnectAsync(CreateClientOptions(port, "worker-1"), cancellationToken);
            await worker2.ConnectAsync(CreateClientOptions(port, "worker-2"), cancellationToken);
            await publisher.ConnectAsync(CreateClientOptions(port, "shared-publisher"), cancellationToken);
            foreach (IMqttClient worker in new[] { worker1, worker2 })
            {
                await worker.SubscribeAsync(
                    new MqttClientSubscribeOptionsBuilder()
                        .WithTopicFilter(
                            "$share/workers/jobs/#",
                            MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                        .Build(),
                    cancellationToken);
            }

            await publisher.PublishStringAsync(
                "jobs/one",
                "first",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
                cancellationToken: cancellationToken);
            await publisher.PublishStringAsync(
                "jobs/two",
                "second",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
                cancellationToken: cancellationToken);

            string[] payloads =
            [
                await received1.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken),
                await received2.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken)
            ];
            Assert.Contains("first", payloads);
            Assert.Contains("second", payloads);

            await publisher.DisconnectAsync(cancellationToken: cancellationToken);
            await worker1.DisconnectAsync(cancellationToken: cancellationToken);
            await worker2.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 15_000)]
        public async Task PublishesWillAfterUnexpectedDisconnect()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient watcher = factory.CreateMqttClient();
            TaskCompletionSource<string> willReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);
            watcher.ApplicationMessageReceivedAsync += eventArgs =>
            {
                willReceived.TrySetResult(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };
            await watcher.ConnectAsync(CreateClientOptions(port, "will-watcher"), cancellationToken);
            await watcher.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(
                        "will/#",
                        MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build(),
                cancellationToken);
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            Assert.Equal(
                1,
                Assert.Single(
                    broker.GetSessions(),
                    session => session.ClientId == "will-watcher").SubscriptionCount);

            using TcpClient abortedClient = new();
            await abortedClient.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
            NetworkStream stream = abortedClient.GetStream();
            ArrayBufferWriter<byte> writer = new();
            MqttPacketCodec.Encode(
                new MqttConnectPacket
                {
                    ClientId = new ReadOnlySequence<byte>("aborted-client"u8.ToArray()),
                    CleanStart = true,
                    KeepAliveSeconds = 30,
                    HasWill = true,
                    WillTopic = new ReadOnlySequence<byte>("will/aborted"u8.ToArray()),
                    WillPayload = new ReadOnlySequence<byte>("gone"u8.ToArray()),
                    WillQualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce
                },
                writer);
            Assert.True(
                MqttPacketCodec.TryDecode(
                    new ReadOnlySequence<byte>(writer.WrittenMemory),
                    out MqttPacket? encodedConnect,
                    out _));
            MqttConnectPacket encodedConnectPacket = Assert.IsType<MqttConnectPacket>(encodedConnect);
            Assert.Equal(
                "will/aborted",
                System.Text.Encoding.UTF8.GetString(encodedConnectPacket.WillTopic.FirstSpan));
            await stream.WriteAsync(writer.WrittenMemory, cancellationToken);
            byte[] connAck = new byte[5];
            _ = await stream.ReadAsync(connAck, cancellationToken);
            MqttZeroSessionSnapshot connectedAbortedSession = Assert.Single(
                broker.GetSessions(),
                session => session.ClientId == "aborted-client");
            Assert.True(connectedAbortedSession.HasWill);
            Assert.Equal("will/aborted", connectedAbortedSession.WillTopic);
            abortedClient.Client.Shutdown(SocketShutdown.Both);
            abortedClient.Client.LingerState = new LingerOption(true, 0);
            abortedClient.Close();

            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            Assert.Equal(1, broker.PublishedWillCount);

            Assert.Equal(
                "gone",
                await willReceived.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken));

            await watcher.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 15_000)]
        public async Task DoesNotPublishWillAfterCleanDisconnect()
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
                    .WithClientId("clean-will-client")
                    .WithCleanStart()
                    .WithWillTopic("will/clean")
                    .WithWillPayload("unexpected")
                    .WithWillQualityOfServiceLevel(
                        MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build(),
                cancellationToken);
            await client.DisconnectAsync(cancellationToken: cancellationToken);
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);

            Assert.Equal(0, broker.PublishedWillCount);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 15_000)]
        public async Task PublishesWillAfterKeepAliveTimeout()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient watcher = factory.CreateMqttClient();
            TaskCompletionSource<string> willReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);
            watcher.ApplicationMessageReceivedAsync += eventArgs =>
            {
                willReceived.TrySetResult(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };
            await watcher.ConnectAsync(CreateClientOptions(port, "keepalive-watcher"), cancellationToken);
            await watcher.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(
                        "will/keepalive",
                        MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build(),
                cancellationToken);

            using TcpClient idleClient = new();
            await idleClient.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
            NetworkStream stream = idleClient.GetStream();
            ArrayBufferWriter<byte> writer = new();
            MqttPacketCodec.Encode(
                new MqttConnectPacket
                {
                    ClientId = new ReadOnlySequence<byte>("idle-client"u8.ToArray()),
                    CleanStart = true,
                    KeepAliveSeconds = 1,
                    HasWill = true,
                    WillTopic = new ReadOnlySequence<byte>("will/keepalive"u8.ToArray()),
                    WillPayload = new ReadOnlySequence<byte>("timeout"u8.ToArray()),
                    WillQualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce
                },
                writer);
            await stream.WriteAsync(writer.WrittenMemory, cancellationToken);
            byte[] connAck = new byte[5];
            _ = await stream.ReadAsync(connAck, cancellationToken);

            Assert.Equal(
                "timeout",
                await willReceived.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken));

            await watcher.DisconnectAsync(cancellationToken: cancellationToken);
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

        [Fact(Timeout = 15_000)]
        public async Task ReconnectCancelsDelayedWill()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();

            using (TcpClient abortedClient = new())
            {
                await abortedClient.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
                NetworkStream stream = abortedClient.GetStream();
                ArrayBufferWriter<byte> writer = new();
                MqttPacketCodec.Encode(
                    new MqttConnectPacket
                    {
                        ClientId = new ReadOnlySequence<byte>("delayed-will-client"u8.ToArray()),
                        CleanStart = false,
                        KeepAliveSeconds = 30,
                        SessionExpiryInterval = 60,
                        HasWill = true,
                        WillTopic = new ReadOnlySequence<byte>("will/delayed"u8.ToArray()),
                        WillPayload = new ReadOnlySequence<byte>("should-not-publish"u8.ToArray()),
                        WillDelayInterval = 2
                    },
                    writer);
                await stream.WriteAsync(writer.WrittenMemory, cancellationToken);
                byte[] connAck = new byte[5];
                _ = await stream.ReadAsync(connAck, cancellationToken);
                abortedClient.Client.LingerState = new LingerOption(true, 0);
                abortedClient.Client.Shutdown(SocketShutdown.Both);
                abortedClient.Close();
            }

            MqttClientFactory factory = new();
            using IMqttClient restored = factory.CreateMqttClient();
            MQTTnet.MqttClientConnectResult result =
                await restored.ConnectAsync(CreatePersistentClientOptions(port, "delayed-will-client"), cancellationToken);
            Assert.True(result.IsSessionPresent);

            await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken);
            Assert.Equal(0, broker.PublishedWillCount);

            await restored.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 20_000)]
        public async Task ResendsInflightQos1MessageWithDuplicateFlag()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient publisher = factory.CreateMqttClient();
            ushort originalPacketIdentifier;

            using (TcpClient first = new())
            {
                await first.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
                NetworkStream stream = first.GetStream();
                await SendPacketAsync(
                    stream,
                    new MqttConnectPacket
                    {
                        ClientId = new ReadOnlySequence<byte>("dup-client"u8.ToArray()),
                        CleanStart = false,
                        KeepAliveSeconds = 30,
                        SessionExpiryInterval = 60
                    },
                    cancellationToken);
                _ = Assert.IsType<MqttConnAckPacket>(await ReadPacketAsync(stream, cancellationToken));
                await SendPacketAsync(
                    stream,
                    new MqttSubscribePacket
                    {
                        PacketIdentifier = 1,
                        Subscriptions =
                        [
                            new MqttSubscriptionRequest
                            {
                                TopicFilter = new ReadOnlySequence<byte>("dup/#"u8.ToArray()),
                                QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce
                            }
                        ]
                    },
                    cancellationToken);
                _ = Assert.IsType<MqttSubAckPacket>(await ReadPacketAsync(stream, cancellationToken));

                await publisher.ConnectAsync(CreateClientOptions(port, "dup-publisher"), cancellationToken);
                await publisher.PublishStringAsync(
                    "dup/value",
                    "inflight",
                    MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
                    cancellationToken: cancellationToken);

                MqttPublishPacket firstPublish = Assert.IsType<MqttPublishPacket>(
                    await ReadPacketAsync(stream, cancellationToken));
                Assert.False(firstPublish.Duplicate);
                originalPacketIdentifier = firstPublish.PacketIdentifier;
                first.Client.LingerState = new LingerOption(true, 0);
                first.Client.Shutdown(SocketShutdown.Both);
                first.Close();
            }

            using (TcpClient restored = new())
            {
                await restored.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
                NetworkStream stream = restored.GetStream();
                await SendPacketAsync(
                    stream,
                    new MqttConnectPacket
                    {
                        ClientId = new ReadOnlySequence<byte>("dup-client"u8.ToArray()),
                        CleanStart = false,
                        KeepAliveSeconds = 30,
                        SessionExpiryInterval = 60
                    },
                    cancellationToken);
                MqttConnAckPacket connAck = Assert.IsType<MqttConnAckPacket>(
                    await ReadPacketAsync(stream, cancellationToken));
                Assert.True(connAck.SessionPresent);

                MqttPublishPacket resentPublish = Assert.IsType<MqttPublishPacket>(
                    await ReadPacketAsync(stream, cancellationToken));
                Assert.True(resentPublish.Duplicate);
                Assert.Equal(originalPacketIdentifier, resentPublish.PacketIdentifier);
                Assert.Equal(
                    "inflight",
                    System.Text.Encoding.UTF8.GetString(resentPublish.Payload.FirstSpan));
                await SendPacketAsync(
                    stream,
                    new MqttPubAckPacket
                    {
                        PacketIdentifier = originalPacketIdentifier
                    },
                    cancellationToken);
                restored.Client.Shutdown(SocketShutdown.Both);
            }

            await publisher.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 15_000)]
        public async Task ManagementApiDisconnectsConnectedClient()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            MqttClientFactory factory = new();
            using IMqttClient client = factory.CreateMqttClient();
            TaskCompletionSource<MQTTnet.MqttClientDisconnectReason> disconnected =
                new(TaskCreationOptions.RunContinuationsAsynchronously);
            client.DisconnectedAsync += eventArgs =>
            {
                disconnected.TrySetResult(eventArgs.Reason);
                return Task.CompletedTask;
            };

            await client.ConnectAsync(CreateClientOptions(port, "managed-client"), cancellationToken);
            bool found = await broker.DisconnectClientAsync(
                "managed-client",
                MqttReasonCode.ServerShuttingDown,
                cancellationToken);

            Assert.True(found);
            Assert.Equal(
                MQTTnet.MqttClientDisconnectReason.ServerShuttingDown,
                await disconnected.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken));
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 15_000)]
        public async Task ResolvesInboundTopicAlias()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient subscriber = factory.CreateMqttClient();
            System.Collections.Concurrent.ConcurrentQueue<string> payloads = new();
            TaskCompletionSource<bool> receivedBoth = new(TaskCreationOptions.RunContinuationsAsynchronously);
            subscriber.ApplicationMessageReceivedAsync += eventArgs =>
            {
                payloads.Enqueue(eventArgs.ApplicationMessage.ConvertPayloadToString());
                if (payloads.Count >= 2)
                {
                    receivedBoth.TrySetResult(true);
                }

                return Task.CompletedTask;
            };
            await subscriber.ConnectAsync(CreateClientOptions(port, "alias-subscriber"), cancellationToken);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter("alias/topic")
                    .Build(),
                cancellationToken);

            using TcpClient publisher = new();
            await publisher.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
            NetworkStream stream = publisher.GetStream();
            await SendPacketAsync(
                stream,
                new MqttConnectPacket
                {
                    ClientId = new ReadOnlySequence<byte>("alias-publisher"u8.ToArray()),
                    CleanStart = true,
                    KeepAliveSeconds = 30
                },
                cancellationToken);
            _ = Assert.IsType<MqttConnAckPacket>(await ReadPacketAsync(stream, cancellationToken));
            await SendPacketAsync(
                stream,
                new MqttPublishPacket
                {
                    TopicName = new ReadOnlySequence<byte>("alias/topic"u8.ToArray()),
                    TopicAlias = 1,
                    Payload = new ReadOnlySequence<byte>("first"u8.ToArray())
                },
                cancellationToken);
            await SendPacketAsync(
                stream,
                new MqttPublishPacket
                {
                    TopicAlias = 1,
                    Payload = new ReadOnlySequence<byte>("second"u8.ToArray())
                },
                cancellationToken);

            _ = await receivedBoth.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);
            Assert.Equal(["first", "second"], payloads.ToArray());

            await subscriber.DisconnectAsync(cancellationToken: cancellationToken);
            publisher.Client.Shutdown(SocketShutdown.Both);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 20_000)]
        public async Task DropsExpiredQueuedMessageBeforeReconnectDelivery()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();

            using (IMqttClient initial = factory.CreateMqttClient())
            {
                await initial.ConnectAsync(CreatePersistentClientOptions(port, "expiry-client"), cancellationToken);
                await initial.SubscribeAsync(
                    new MqttClientSubscribeOptionsBuilder()
                        .WithTopicFilter(
                            "expiry/#",
                            MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                        .Build(),
                    cancellationToken);
                await initial.DisconnectAsync(cancellationToken: cancellationToken);
            }

            using IMqttClient publisher = factory.CreateMqttClient();
            await publisher.ConnectAsync(CreateClientOptions(port, "expiry-publisher"), cancellationToken);
            await publisher.PublishAsync(
                new MqttApplicationMessageBuilder()
                    .WithTopic("expiry/value")
                    .WithPayload("temporary")
                    .WithQualityOfServiceLevel(
                        MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                    .WithMessageExpiryInterval(1)
                    .Build(),
                cancellationToken);

            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);

            using IMqttClient restored = factory.CreateMqttClient();
            TaskCompletionSource<bool> received = new(TaskCreationOptions.RunContinuationsAsynchronously);
            restored.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.TrySetResult(true);
                return Task.CompletedTask;
            };
            MQTTnet.MqttClientConnectResult result =
                await restored.ConnectAsync(CreatePersistentClientOptions(port, "expiry-client"), cancellationToken);
            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);

            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            MqttZeroSessionSnapshot snapshot = Assert.Single(
                broker.GetSessions(),
                session => session.ClientId == "expiry-client");
            Assert.True(result.IsSessionPresent);
            Assert.False(received.Task.IsCompleted);
            Assert.Equal(0, snapshot.PendingMessageCount);

            await publisher.DisconnectAsync(cancellationToken: cancellationToken);
            await restored.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 15_000)]
        public async Task DoesNotReplayExpiredRetainedMessage()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient publisher = factory.CreateMqttClient();
            using IMqttClient subscriber = factory.CreateMqttClient();
            TaskCompletionSource<bool> received = new(TaskCreationOptions.RunContinuationsAsynchronously);
            subscriber.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.TrySetResult(true);
                return Task.CompletedTask;
            };

            await publisher.ConnectAsync(CreateClientOptions(port, "retained-expiry-publisher"), cancellationToken);
            await publisher.PublishAsync(
                new MqttApplicationMessageBuilder()
                    .WithTopic("retained/expiry")
                    .WithPayload("temporary")
                    .WithRetainFlag()
                    .WithMessageExpiryInterval(1)
                    .Build(),
                cancellationToken);
            await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);

            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            Assert.Empty(broker.GetRetainedMessages());

            await subscriber.ConnectAsync(CreateClientOptions(port, "retained-expiry-subscriber"), cancellationToken);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter("retained/#")
                    .Build(),
                cancellationToken);
            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
            Assert.False(received.Task.IsCompleted);

            await publisher.DisconnectAsync(cancellationToken: cancellationToken);
            await subscriber.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 15_000)]
        public async Task AppliesNoLocalAndSubscriptionIdentifier()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient subscriber = factory.CreateMqttClient();
            using IMqttClient otherPublisher = factory.CreateMqttClient();
            System.Collections.Concurrent.ConcurrentQueue<string> payloads = new();
            System.Collections.Concurrent.ConcurrentQueue<uint[]> identifiers = new();
            TaskCompletionSource<bool> received = new(TaskCreationOptions.RunContinuationsAsynchronously);
            subscriber.ApplicationMessageReceivedAsync += eventArgs =>
            {
                payloads.Enqueue(eventArgs.ApplicationMessage.ConvertPayloadToString());
                identifiers.Enqueue(
                    eventArgs.ApplicationMessage.SubscriptionIdentifiers?.ToArray() ?? []);
                received.TrySetResult(true);
                return Task.CompletedTask;
            };

            await subscriber.ConnectAsync(CreateClientOptions(port, "no-local-subscriber"), cancellationToken);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(
                        new MqttTopicFilterBuilder()
                            .WithTopic("no-local/topic")
                            .WithNoLocal()
                            .Build())
                    .WithSubscriptionIdentifier(77)
                    .Build(),
                cancellationToken);
            await subscriber.PublishStringAsync(
                "no-local/topic",
                "self",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
                cancellationToken: cancellationToken);

            await otherPublisher.ConnectAsync(CreateClientOptions(port, "other-publisher"), cancellationToken);
            await otherPublisher.PublishStringAsync(
                "no-local/topic",
                "other",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
                cancellationToken: cancellationToken);

            _ = await received.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);
            Assert.Equal(["other"], payloads.ToArray());
            Assert.Equal([77U], identifiers.ToArray()[0]);

            await otherPublisher.DisconnectAsync(cancellationToken: cancellationToken);
            await subscriber.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        [Fact(Timeout = 15_000)]
        public async Task HonorsRetainHandlingDoNotSendOnSubscribe()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttClientFactory factory = new();
            using IMqttClient publisher = factory.CreateMqttClient();
            using IMqttClient subscriber = factory.CreateMqttClient();
            TaskCompletionSource<string> received = new(TaskCreationOptions.RunContinuationsAsynchronously);
            subscriber.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.TrySetResult(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };

            await publisher.ConnectAsync(CreateClientOptions(port, "retain-handling-publisher"), cancellationToken);
            await publisher.PublishStringAsync(
                "retain-handling/value",
                "retained",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
                retain: true,
                cancellationToken: cancellationToken);

            await subscriber.ConnectAsync(CreateClientOptions(port, "retain-handling-subscriber"), cancellationToken);
            await subscriber.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(
                        new MqttTopicFilterBuilder()
                            .WithTopic("retain-handling/#")
                            .WithRetainHandling(MQTTnet.Protocol.MqttRetainHandling.DoNotSendOnSubscribe)
                            .Build())
                    .Build(),
                cancellationToken);
            await Task.Delay(TimeSpan.FromMilliseconds(500), cancellationToken);
            Assert.False(received.Task.IsCompleted);

            await publisher.PublishStringAsync(
                "retain-handling/value",
                "live",
                MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
                cancellationToken: cancellationToken);
            Assert.Equal(
                "live",
                await received.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken));

            await publisher.DisconnectAsync(cancellationToken: cancellationToken);
            await subscriber.DisconnectAsync(cancellationToken: cancellationToken);
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
        public async Task ClosesConnectionForOversizedInboundPacket()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(
                port,
                options => options.MaxPacketSize = 64);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            using TcpClient client = new();
            await client.ConnectAsync(IPAddress.Loopback, port, cancellationToken);
            NetworkStream stream = client.GetStream();
            await SendPacketAsync(
                stream,
                new MqttConnectPacket
                {
                    ClientId = new ReadOnlySequence<byte>("oversized-client"u8.ToArray()),
                    CleanStart = true,
                    KeepAliveSeconds = 30
                },
                cancellationToken);
            _ = Assert.IsType<MqttConnAckPacket>(await ReadPacketAsync(stream, cancellationToken));

            await SendPacketAsync(
                stream,
                new MqttPublishPacket
                {
                    TopicName = new ReadOnlySequence<byte>("oversized/topic"u8.ToArray()),
                    Payload = new ReadOnlySequence<byte>(new byte[128])
                },
                cancellationToken);

            using CancellationTokenSource readTimeout =
                CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            readTimeout.CancelAfter(TimeSpan.FromSeconds(2));
            byte[] buffer = new byte[1];
            int read = await stream.ReadAsync(buffer, readTimeout.Token);
            Assert.Equal(0, read);
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

        [Fact(Timeout = 15_000)]
        public async Task EvictsOldestMessagesWhenSessionQueueIsFull()
        {
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(
                port,
                options => options.MaxMessageQueueLength = 2);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            MqttClientFactory factory = new();

            using (IMqttClient initial = factory.CreateMqttClient())
            {
                await initial.ConnectAsync(CreatePersistentClientOptions(port, "queue-client"), cancellationToken);
                await initial.SubscribeAsync(
                    new MqttClientSubscribeOptionsBuilder()
                        .WithTopicFilter(
                            "queue/#",
                            MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce)
                        .Build(),
                    cancellationToken);
                await initial.DisconnectAsync(cancellationToken: cancellationToken);
            }

            for (int index = 1; index <= 3; index++)
            {
                await broker.PublishAsync(
                    "queue/value",
                    System.Text.Encoding.UTF8.GetBytes(index.ToString(CultureInfo.InvariantCulture)),
                    MqttQualityOfServiceLevel.AtLeastOnce,
                    false,
                    cancellationToken);
            }

            MqttZeroSessionSnapshot queuedSession = Assert.Single(
                broker.GetSessions(),
                session => session.ClientId == "queue-client");
            Assert.Equal(2, queuedSession.PendingMessageCount);

            using IMqttClient restored = factory.CreateMqttClient();
            System.Collections.Concurrent.ConcurrentQueue<string> payloads = new();
            TaskCompletionSource<bool> receivedBoth = new(TaskCreationOptions.RunContinuationsAsynchronously);
            restored.ApplicationMessageReceivedAsync += eventArgs =>
            {
                payloads.Enqueue(eventArgs.ApplicationMessage.ConvertPayloadToString());
                if (payloads.Count == 2)
                {
                    receivedBoth.TrySetResult(true);
                }

                return Task.CompletedTask;
            };
            _ = await restored.ConnectAsync(CreatePersistentClientOptions(port, "queue-client"), cancellationToken);
            _ = await receivedBoth.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);
            Assert.Equal(["2", "3"], payloads.ToArray());

            await restored.DisconnectAsync(cancellationToken: cancellationToken);
            await app.StopAsync(cancellationToken);
        }

        private static MQTTnet.MqttClientOptions CreateClientOptions(int port, string clientId)
        {
            return new MqttClientOptionsBuilder()
                .WithTcpServer("127.0.0.1", port)
                .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
                .WithClientId(clientId)
                .WithCleanStart()
                .Build();
        }

        private static MQTTnet.MqttClientOptions CreatePersistentClientOptions(int port, string clientId)
        {
            return new MqttClientOptionsBuilder()
                .WithTcpServer("127.0.0.1", port)
                .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
                .WithClientId(clientId)
                .WithCleanStart(false)
                .WithSessionExpiryInterval(60)
                .Build();
        }

        private static MQTTnet.MqttClientOptions CreateWebSocketClientOptions(int port, string clientId)
        {
            return new MqttClientOptionsBuilder()
                .WithWebSocketServer(options =>
                {
                    options.WithUri($"ws://127.0.0.1:{port}/mqtt");
                })
                .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
                .WithClientId(clientId)
                .WithCleanStart()
                .Build();
        }

        private static MQTTnet.MqttClientOptions CreateTlsClientOptions(int port, string clientId)
        {
            return new MqttClientOptionsBuilder()
                .WithTcpServer("127.0.0.1", port)
                .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
                .WithClientId(clientId)
                .WithCleanStart()
                .WithTlsOptions(options =>
                {
                    options
                        .UseTls()
                        .WithSslProtocols(SslProtocols.Tls12 | SslProtocols.Tls13)
                        .WithAllowUntrustedCertificates()
                        .WithIgnoreCertificateChainErrors()
                        .WithCertificateValidationHandler(static _ => true);
                })
                .Build();
        }

        private static X509Certificate2 CreateServerCertificate()
        {
            using RSA rsa = RSA.Create(2048);
            CertificateRequest request = new(
                "CN=localhost",
                rsa,
                HashAlgorithmName.SHA256,
                RSASignaturePadding.Pkcs1);
            SubjectAlternativeNameBuilder alternativeNames = new();
            alternativeNames.AddDnsName("localhost");
            alternativeNames.AddIpAddress(IPAddress.Loopback);
            request.CertificateExtensions.Add(alternativeNames.Build());
            request.CertificateExtensions.Add(
                new X509BasicConstraintsExtension(false, false, 0, false));
            request.CertificateExtensions.Add(
                new X509KeyUsageExtension(
                    X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment,
                    false));
            request.CertificateExtensions.Add(
                new X509EnhancedKeyUsageExtension(
                    new OidCollection
                    {
                        new("1.3.6.1.5.5.7.3.1")
                    },
                    false));
            using X509Certificate2 temporary = request.CreateSelfSigned(
                DateTimeOffset.UtcNow.AddMinutes(-1),
                DateTimeOffset.UtcNow.AddDays(1));
            return X509CertificateLoader.LoadPkcs12(
                temporary.Export(X509ContentType.Pkcs12),
                null,
                X509KeyStorageFlags.UserKeySet | X509KeyStorageFlags.Exportable);
        }

        private static async Task<WebApplication> StartTcpBrokerAsync(
            int port,
            Action<MqttZeroOptions>? configure = null)
        {
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
            {
                kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero());
            });
            builder.Services.AddMqttZero(configure);

            WebApplication app = builder.Build();
            await app.StartAsync(TestContext.Current.CancellationToken);
            return app;
        }

        private static async Task SendPacketAsync(
            NetworkStream stream,
            MqttPacket packet,
            CancellationToken cancellationToken)
        {
            ArrayBufferWriter<byte> writer = new();
            MqttPacketCodec.Encode(packet, writer);
            await stream.WriteAsync(writer.WrittenMemory, cancellationToken);
        }

        private static async Task<MqttPacket> ReadPacketAsync(
            NetworkStream stream,
            CancellationToken cancellationToken)
        {
            byte[] buffer = new byte[65536];
            int count = 0;
            while (count < buffer.Length)
            {
                int read = await stream.ReadAsync(buffer.AsMemory(count, 1), cancellationToken);
                if (read == 0)
                {
                    throw new EndOfStreamException();
                }

                count += read;
                if (MqttPacketCodec.TryDecode(
                    new ReadOnlySequence<byte>(buffer, 0, count),
                    out MqttPacket? packet,
                    out _))
                {
                    return packet ?? throw new InvalidOperationException("Decoded packet was null.");
                }
            }

            throw new InvalidOperationException("MQTT packet exceeded the test buffer.");
        }

        private static int GetFreePort()
        {
            TcpListener listener = new(IPAddress.Loopback, 0);
            listener.Start();
            int port = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return port;
        }
    }
}
