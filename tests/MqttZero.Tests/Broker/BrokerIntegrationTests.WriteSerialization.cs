using Microsoft.AspNetCore.Builder;
using MQTTnet;
using MQTTnet.Protocol;

namespace MqttZero.Tests.Broker
{
    public sealed partial class BrokerIntegrationTests
    {
        [Fact(Timeout = 60_000)]
        public async Task SelfPublishDeliversEveryMessageOverOneConnection()
        {
            const int TotalMessages = 200;
            int port = GetFreePort();
            await using WebApplication app = await StartTcpBrokerAsync(port);
            MqttClientFactory factory = new();
            using IMqttClient client = factory.CreateMqttClient();
            long received = 0;
            client.ApplicationMessageReceivedAsync += _ =>
            {
                Interlocked.Increment(ref received);
                return Task.CompletedTask;
            };
            CancellationToken cancellationToken =
                TestContext.Current.CancellationToken;
            await client.ConnectAsync(
                CreatePersistentClientOptions(port, "self-publisher"),
                cancellationToken);
            await client.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter(
                        "self/topic",
                        MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build(),
                cancellationToken);
            byte[] payload = new byte[8 * 1024];
            for (int index = 0; index < TotalMessages; index++)
            {
                await client.PublishAsync(
                    new MqttApplicationMessageBuilder()
                        .WithTopic("self/topic")
                        .WithPayload(payload)
                        .WithQualityOfServiceLevel(
                            MqttQualityOfServiceLevel.AtLeastOnce)
                        .Build(),
                    cancellationToken);
            }

            using CancellationTokenSource timeout =
                CancellationTokenSource.CreateLinkedTokenSource(
                    cancellationToken);
            timeout.CancelAfter(TimeSpan.FromSeconds(20));
            while (Interlocked.Read(ref received) < TotalMessages)
            {
                await Task.Delay(
                    TimeSpan.FromMilliseconds(5),
                    timeout.Token);
            }

            Assert.Equal(TotalMessages, Interlocked.Read(ref received));
            Assert.True(client.IsConnected);
        }
    }
}
