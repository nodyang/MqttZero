using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MQTTnet;
using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.AspNetCore;
using MqttZero.Server.Events;
using MqttZero.Server.Interceptors;
using MqttZero.Server.Observers;
using MqttZero.Protocol;
using System.Buffers;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace MqttZero.Tests.Broker
{
    public sealed partial class BrokerIntegrationTests
    {
        [Fact(Timeout = 15_000)]
        public async Task RejectsConnectionBeforeCreatingSession()
        {
            int port = GetFreePort();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
                kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
            builder.Services.AddMqttZero()
                .AddConnectionInterceptor<RejectingConnectionInterceptor>();
            await using WebApplication app = builder.Build();
            await app.StartAsync(TestContext.Current.CancellationToken);

            using TcpClient client = new();
            await client.ConnectAsync(
                IPAddress.Loopback,
                port,
                TestContext.Current.CancellationToken);
            NetworkStream stream = client.GetStream();
            await SendPacketAsync(
                stream,
                new MqttConnectPacket
                {
                    ClientId = new ReadOnlySequence<byte>("rejected"u8.ToArray()),
                    HasUserName = true,
                    UserName = new ReadOnlySequence<byte>("user"u8.ToArray()),
                    HasPassword = true,
                    Password = new ReadOnlySequence<byte>("wrong"u8.ToArray())
                },
                TestContext.Current.CancellationToken);

            MqttConnAckPacket connAck = Assert.IsType<MqttConnAckPacket>(
                await ReadPacketAsync(stream, TestContext.Current.CancellationToken));
            Assert.Equal(MqttReasonCode.BadUserNameOrPassword, connAck.ReasonCode);
            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            Assert.DoesNotContain(
                broker.GetSessions(),
                session => session.ClientId == "rejected");
        }

        [Fact(Timeout = 15_000)]
        public async Task SubscriptionInterceptorRejectsOnlyDeniedFilter()
        {
            int port = GetFreePort();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
                kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
            builder.Services.AddMqttZero().AddSubscribeInterceptor<FilterInterceptor>();
            await using WebApplication app = builder.Build();
            await app.StartAsync(TestContext.Current.CancellationToken);

            MqttClientFactory factory = new();
            using IMqttClient client = factory.CreateMqttClient();
            await client.ConnectAsync(
                CreateClientOptions(port, "filter-client"),
                TestContext.Current.CancellationToken);
            MqttClientSubscribeResult result = await client.SubscribeAsync(
                new MqttClientSubscribeOptionsBuilder()
                    .WithTopicFilter("allowed/#")
                    .WithTopicFilter("denied/#")
                    .Build(),
                TestContext.Current.CancellationToken);

            Assert.Contains(result.Items, item => item.TopicFilter.Topic == "allowed/#");
            Assert.Contains(
                result.Items,
                item => item.TopicFilter.Topic == "denied/#" &&
                    item.ResultCode == MqttClientSubscribeResultCode.NotAuthorized);
            MqttZeroSessionSnapshot session = Assert.Single(
                app.Services.GetRequiredService<MqttZeroBroker>().GetSessions(),
                item => item.ClientId == "filter-client");
            Assert.Equal(1, session.SubscriptionCount);
        }

        [Fact(Timeout = 15_000)]
        public async Task EnqueueInterceptorDropsOnlyTargetReceiver()
        {
            int port = GetFreePort();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
                kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
            builder.Services.AddMqttZero().AddEnqueueInterceptor<ReceiverInterceptor>();
            await using WebApplication app = builder.Build();
            await app.StartAsync(TestContext.Current.CancellationToken);

            MqttClientFactory factory = new();
            using IMqttClient dropped = factory.CreateMqttClient();
            using IMqttClient kept = factory.CreateMqttClient();
            using IMqttClient publisher = factory.CreateMqttClient();
            ConcurrentQueue<string> droppedMessages = new();
            TaskCompletionSource<string> keptMessage = new(
                TaskCreationOptions.RunContinuationsAsynchronously);
            dropped.ApplicationMessageReceivedAsync += eventArgs =>
            {
                droppedMessages.Enqueue(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };
            kept.ApplicationMessageReceivedAsync += eventArgs =>
            {
                keptMessage.TrySetResult(eventArgs.ApplicationMessage.ConvertPayloadToString());
                return Task.CompletedTask;
            };
            await dropped.ConnectAsync(
                CreateClientOptions(port, "drop"),
                TestContext.Current.CancellationToken);
            await kept.ConnectAsync(
                CreateClientOptions(port, "keep"),
                TestContext.Current.CancellationToken);
            await publisher.ConnectAsync(
                CreateClientOptions(port, "extension-publisher"),
                TestContext.Current.CancellationToken);
            foreach (IMqttClient subscriber in new[] { dropped, kept })
            {
                await subscriber.SubscribeAsync(
                    new MqttClientSubscribeOptionsBuilder()
                        .WithTopicFilter("route/#")
                        .Build(),
                    TestContext.Current.CancellationToken);
            }

            await publisher.PublishStringAsync(
                "route/value",
                "payload",
                cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(
                "payload",
                await keptMessage.Task.WaitAsync(
                    TimeSpan.FromSeconds(5),
                    TestContext.Current.CancellationToken));
            await Task.Delay(
                TimeSpan.FromMilliseconds(250),
                TestContext.Current.CancellationToken);
            Assert.Empty(droppedMessages);
        }

        [Fact(Timeout = 15_000)]
        public async Task ConnectionObserverReceivesConnectAndDisconnect()
        {
            int port = GetFreePort();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
                kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
            builder.Services.AddSingleton<ConnectionRecorder>();
            builder.Services.AddMqttZero().AddConnectionObserver<ConnectionObserver>();
            await using WebApplication app = builder.Build();
            await app.StartAsync(TestContext.Current.CancellationToken);

            MqttClientFactory factory = new();
            using IMqttClient client = factory.CreateMqttClient();
            await client.ConnectAsync(
                CreateClientOptions(port, "observed-client"),
                TestContext.Current.CancellationToken);
            await client.DisconnectAsync(
                cancellationToken: TestContext.Current.CancellationToken);

            ConnectionRecorder recorder = app.Services.GetRequiredService<ConnectionRecorder>();
            await recorder.Disconnected.Task.WaitAsync(
                TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken);
            Assert.True(recorder.Connected.Task.IsCompletedSuccessfully);
            Assert.Equal(
                MqttZeroDisconnectType.ClientRequest,
                recorder.DisconnectType);
        }

        [Fact(Timeout = 15_000)]
        public async Task EnqueueCloseWithoutSenderTerminatesCurrentDelivery()
        {
            int port = GetFreePort();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.ConfigureKestrel(kestrel =>
                kestrel.Listen(IPAddress.Loopback, port, listen => listen.UseMqttZero()));
            builder.Services.AddMqttZero().AddEnqueueInterceptor<CloseSenderInterceptor>();
            await using WebApplication app = builder.Build();
            await app.StartAsync(TestContext.Current.CancellationToken);
            MqttClientFactory factory = new();

            using (IMqttClient client = factory.CreateMqttClient())
            {
                await client.ConnectAsync(
                    CreatePersistentClientOptions(port, "close-sender-client"),
                    TestContext.Current.CancellationToken);
                await client.SubscribeAsync(
                    new MqttClientSubscribeOptionsBuilder()
                        .WithTopicFilter("close-sender/#")
                        .Build(),
                    TestContext.Current.CancellationToken);
                await client.DisconnectAsync(
                    cancellationToken: TestContext.Current.CancellationToken);
            }

            MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
            await WaitForSessionDisconnectedAsync(
                broker,
                "close-sender-client",
                TestContext.Current.CancellationToken);
            await broker.PublishAsync(
                "close-sender/value",
                new ReadOnlySequence<byte>("dropped"u8.ToArray()),
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                TestContext.Current.CancellationToken);

            MqttZeroSessionSnapshot session = Assert.Single(
                broker.GetSessions(),
                item => item.ClientId == "close-sender-client");
            Assert.Equal(0, session.PendingMessageCount);
            Assert.Equal(0, session.InflightMessageCount);
        }

        private sealed class RejectingConnectionInterceptor : IMqttZeroConnectionInterceptor
        {
            public ValueTask<MqttZeroConnectionInterceptionResult> InterceptAsync(
                MqttZeroConnectionContext context,
                CancellationToken cancellationToken)
            {
                context.RejectReasonCode = MqttReasonCode.BadUserNameOrPassword;
                return ValueTask.FromResult(MqttZeroConnectionInterceptionResult.Reject);
            }
        }

        private sealed class FilterInterceptor : IMqttZeroSubscribeInterceptor
        {
            public ValueTask<MqttZeroInterceptionResult> InterceptAsync(
                MqttZeroSubscribeContext context,
                CancellationToken cancellationToken)
            {
                bool allowed = context.TopicFilter.FirstSpan.StartsWith("allowed/"u8);
                return ValueTask.FromResult(
                    allowed
                        ? MqttZeroInterceptionResult.Continue
                        : MqttZeroInterceptionResult.Consume);
            }
        }

        private sealed class ReceiverInterceptor : IMqttZeroEnqueueInterceptor
        {
            public ValueTask<MqttZeroEnqueueInterceptionResult> InterceptAsync(
                MqttZeroEnqueueContext context,
                CancellationToken cancellationToken)
            {
                Assert.NotNull(context.Receiver);
                return ValueTask.FromResult(
                    context.ReceiverInfo.ClientId == "drop"
                        ? MqttZeroEnqueueInterceptionResult.Drop
                        : MqttZeroEnqueueInterceptionResult.Enqueue);
            }
        }

        private sealed class ConnectionRecorder
        {
            public TaskCompletionSource<bool> Connected { get; } =
                new(TaskCreationOptions.RunContinuationsAsynchronously);
            public TaskCompletionSource<bool> Disconnected { get; } =
                new(TaskCreationOptions.RunContinuationsAsynchronously);
            public MqttZeroDisconnectType DisconnectType { get; private set; }

            public void SetDisconnected(MqttZeroDisconnectType disconnectType)
            {
                DisconnectType = disconnectType;
                Disconnected.TrySetResult(true);
            }
        }

        private sealed class ConnectionObserver : IMqttZeroConnectionObserver
        {
            private readonly ConnectionRecorder _recorder;

            public ConnectionObserver(ConnectionRecorder recorder)
            {
                _recorder = recorder;
            }

            public ValueTask OnConnectedAsync(
                MqttZeroClientConnectedContext context,
                CancellationToken cancellationToken)
            {
                _recorder.Connected.TrySetResult(true);
                return ValueTask.CompletedTask;
            }

            public ValueTask OnDisconnectedAsync(
                MqttZeroClientDisconnectedContext context,
                CancellationToken cancellationToken)
            {
                _recorder.SetDisconnected(context.DisconnectType);
                return ValueTask.CompletedTask;
            }
        }

        private sealed class CloseSenderInterceptor : IMqttZeroEnqueueInterceptor
        {
            public ValueTask<MqttZeroEnqueueInterceptionResult> InterceptAsync(
                MqttZeroEnqueueContext context,
                CancellationToken cancellationToken)
            {
                Assert.Null(context.Sender);
                return ValueTask.FromResult(
                    MqttZeroEnqueueInterceptionResult.CloseSenderConnection);
            }
        }
    }
}
