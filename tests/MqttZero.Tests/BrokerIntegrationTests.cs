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
