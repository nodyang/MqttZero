using System.Net;
using System.Net.Sockets;
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
            builder.Logging.AddConsole();
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

        private static MQTTnet.MqttClientOptions CreateClientOptions(int port, string clientId)
        {
            return new MqttClientOptionsBuilder()
                .WithTcpServer("127.0.0.1", port)
                .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
                .WithClientId(clientId)
                .WithCleanStart()
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
