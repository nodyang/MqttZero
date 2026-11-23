using MqttZero.Broker;
using MqttZero.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Routing;
using MqttZero.Sessions;
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
    }
}
