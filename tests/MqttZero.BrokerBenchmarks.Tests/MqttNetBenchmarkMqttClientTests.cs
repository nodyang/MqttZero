using MQTTnet;
using MQTTnet.Diagnostics.PacketInspection;
using MqttZero.Protocol;
using System.Runtime.InteropServices;

namespace MqttZero.BrokerBenchmarks.Tests;

public sealed class MqttNetBenchmarkMqttClientTests
{
    [Fact]
    public async Task PublishReusesCallerPayloadMemory()
    {
        byte[] payload = [1, 2, 3, 4];
        CapturingMqttClient inner = new();
        MqttNetBenchmarkMqttClient client = new(
            inner,
            port: 1883);

        await client.PublishAsync(
            "bench/topic",
            payload,
            MqttQualityOfServiceLevel.AtMostOnce,
            CancellationToken.None);

        MqttApplicationMessage message = Assert.IsType<MqttApplicationMessage>(
            inner.PublishedMessage);
        Assert.True(
            MemoryMarshal.TryGetArray(
                message.Payload.First,
                out ArraySegment<byte> payloadSegment));
        Assert.Same(payload, payloadSegment.Array);
    }

    private sealed class CapturingMqttClient : IMqttClient
    {
        public MqttApplicationMessage? PublishedMessage { get; private set; }

        public bool IsConnected
        {
            get
            {
                return true;
            }
        }

        public MqttClientOptions Options
        {
            get
            {
                return null!;
            }
        }

        public event Func<
            MqttApplicationMessageReceivedEventArgs,
            Task>? ApplicationMessageReceivedAsync
        {
            add
            {
            }
            remove
            {
            }
        }

        public event Func<MqttClientConnectedEventArgs, Task>? ConnectedAsync
        {
            add
            {
            }
            remove
            {
            }
        }

        public event Func<MqttClientConnectingEventArgs, Task>? ConnectingAsync
        {
            add
            {
            }
            remove
            {
            }
        }

        public event Func<MqttClientDisconnectedEventArgs, Task>? DisconnectedAsync
        {
            add
            {
            }
            remove
            {
            }
        }

        public event Func<InspectMqttPacketEventArgs, Task>? InspectPacketAsync
        {
            add
            {
            }
            remove
            {
            }
        }

        public Task<MqttClientConnectResult> ConnectAsync(
            MqttClientOptions options,
            CancellationToken cancellationToken)
        {
            return Task.FromResult<MqttClientConnectResult>(null!);
        }

        public Task DisconnectAsync(
            MqttClientDisconnectOptions options,
            CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public void Dispose()
        {
        }

        public Task PingAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task<MqttClientPublishResult> PublishAsync(
            MqttApplicationMessage applicationMessage,
            CancellationToken cancellationToken)
        {
            PublishedMessage = applicationMessage;
            return Task.FromResult<MqttClientPublishResult>(null!);
        }

        public Task SendEnhancedAuthenticationExchangeDataAsync(
            MqttEnhancedAuthenticationExchangeData data,
            CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public Task<MqttClientSubscribeResult> SubscribeAsync(
            MqttClientSubscribeOptions options,
            CancellationToken cancellationToken)
        {
            return Task.FromResult<MqttClientSubscribeResult>(null!);
        }

        public Task<MqttClientUnsubscribeResult> UnsubscribeAsync(
            MqttClientUnsubscribeOptions options,
            CancellationToken cancellationToken)
        {
            return Task.FromResult<MqttClientUnsubscribeResult>(null!);
        }
    }
}
