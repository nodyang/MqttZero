using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MqttZero.AspNetCore;
using MqttZero.Clients;
using MqttZero.Observers;
using MqttZero.Protocol;
using MqttZero.Publishing;
using System.Buffers;
using System.Collections.Concurrent;
using System.Net;

namespace MqttZero.Tests.Broker
{
    public sealed partial class BrokerIntegrationTests
    {
        [Fact(Timeout = 15_000)]
        public async Task ConnectedClientHandleCanSubscribeAndSendPublish()
        {
            int port = GetFreePort();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
                kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
            builder.Services.AddSingleton<ConnectedClientRecorder>();
            builder.Services.AddMqttZero().AddConnectionObserver<HandleObserver>();
            await using WebApplication app = builder.Build();
            await app.StartAsync(TestContext.Current.CancellationToken);

            MqttClientFactory factory = new();
            using IMqttClient client = factory.CreateMqttClient();
            ConcurrentQueue<string> received = new();
            TaskCompletionSource<bool> messageReceived = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            client.ApplicationMessageReceivedAsync += eventArgs =>
            {
                received.Enqueue(eventArgs.ApplicationMessage.ConvertPayloadToString());
                messageReceived.TrySetResult(true);
                return Task.CompletedTask;
            };
            await client.ConnectAsync(
                CreateClientOptions(port, "managed-client"),
                TestContext.Current.CancellationToken);

            ConnectedClientRecorder recorder =
                app.Services.GetRequiredService<ConnectedClientRecorder>();
            MqttZeroConnectedClient connected = await recorder.Client.Task.WaitAsync(
                TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken);
            await recorder.Subscribed.Task.WaitAsync(
                TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken);

            await connected.SendPublishAsync(
                new MqttZeroPublishMessage
                {
                    Topic = "managed/direct",
                    Payload = new ReadOnlySequence<byte>("direct"u8.ToArray())
                },
                TestContext.Current.CancellationToken);

            _ = await messageReceived.Task.WaitAsync(
                TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken);
            Assert.Equal(["direct"], received.ToArray());
            Assert.Same(connected.Items, recorder.Items);

            await client.DisconnectAsync(
                cancellationToken: TestContext.Current.CancellationToken);
            await app.StopAsync(TestContext.Current.CancellationToken);
        }

        private sealed class ConnectedClientRecorder
        {
            public TaskCompletionSource<MqttZeroConnectedClient> Client { get; } =
                new(TaskCreationOptions.RunContinuationsAsynchronously);
            public TaskCompletionSource<bool> Subscribed { get; } =
                new(TaskCreationOptions.RunContinuationsAsynchronously);
            public IDictionary<object, object?>? Items { get; set; }
        }

        private sealed class HandleObserver : IMqttZeroConnectionObserver
        {
            private readonly ConnectedClientRecorder _recorder;

            public HandleObserver(ConnectedClientRecorder recorder)
            {
                _recorder = recorder;
            }

            public async ValueTask OnConnectedAsync(
                MqttZeroClientConnectedContext context,
                CancellationToken cancellationToken)
            {
                _recorder.Items = context.ConnectedClient.Items;
                context.ConnectedClient.Items["role"] = "device";
                _recorder.Client.TrySetResult(context.ConnectedClient);
                await context.ConnectedClient.SubscribeAsync(
                    [new MqttSubscriptionRequest
                    {
                        TopicFilter = new ReadOnlySequence<byte>("managed/#"u8.ToArray())
                    }],
                    cancellationToken: cancellationToken);
                _recorder.Subscribed.TrySetResult(true);
            }

            public ValueTask OnDisconnectedAsync(
                MqttZeroClientDisconnectedContext context,
                CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }
        }
    }
}
