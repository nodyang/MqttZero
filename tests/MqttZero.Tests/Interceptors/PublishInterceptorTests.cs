using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Publishing;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using MqttZero.AspNetCore;

namespace MqttZero.Tests.Interceptors
{
    public sealed class PublishInterceptorTests
    {
        [Fact]
        public async Task RegisteredPublishInterceptorIsInvoked()
        {
            ServiceCollection services = new();
            services.AddMqttZero().AddInterceptor<RecordingPublishInterceptor>();
            await using ServiceProvider provider = services.BuildServiceProvider();

            RecordingPublishInterceptor interceptor =
                provider.GetRequiredService<RecordingPublishInterceptor>();
            Assert.Single(provider.GetServices<IMqttZeroPublishInterceptor>());
            MqttZeroBroker broker = provider.GetRequiredService<MqttZeroBroker>();
            await broker.StartAsync(TestContext.Current.CancellationToken);
            await broker.PublishAsync(
                "devices/a",
                new ReadOnlySequence<byte>("value"u8.ToArray()),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                TestContext.Current.CancellationToken);

            Assert.Equal(1, interceptor.InvocationCount);
            Assert.Equal("devices/a", interceptor.Topic);
        }

        [Fact]
        public async Task RegisteredPublishInterceptorsRunInRegistrationOrder()
        {
            ServiceCollection services = new();
            services.AddSingleton<InterceptorOrderRecorder>();
            services.AddMqttZero()
                .AddInterceptor<FirstPublishInterceptor>()
                .AddInterceptor<SecondPublishInterceptor>();
            await using ServiceProvider provider = services.BuildServiceProvider();

            InterceptorOrderRecorder recorder = provider.GetRequiredService<InterceptorOrderRecorder>();
            MqttZeroBroker broker = provider.GetRequiredService<MqttZeroBroker>();
            await broker.StartAsync(TestContext.Current.CancellationToken);
            await broker.PublishAsync(
                "devices/a",
                new ReadOnlySequence<byte>("value"u8.ToArray()),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                TestContext.Current.CancellationToken);

            Assert.Equal(["first", "second"], recorder.Names);
        }

        [Fact]
        public async Task PublishInterceptorReceivesTypedProperties()
        {
            ServiceCollection services = new();
            services.AddMqttZero().AddInterceptor<RecordingPublishInterceptor>();
            await using ServiceProvider provider = services.BuildServiceProvider();

            RecordingPublishInterceptor interceptor =
                provider.GetRequiredService<RecordingPublishInterceptor>();
            MqttZeroBroker broker = provider.GetRequiredService<MqttZeroBroker>();
            await broker.StartAsync(TestContext.Current.CancellationToken);
            await broker.PublishAsync(
                new MqttZeroPublishMessage
                {
                    Topic = "devices/a",
                    Payload = new ReadOnlySequence<byte>("value"u8.ToArray()),
                    Properties = new MqttPublishProperties
                    {
                        ContentType = "text/plain",
                        UserProperties = [new MqttUserProperty("trace", "one")]
                    }
                },
                TestContext.Current.CancellationToken);

            Assert.Equal("text/plain", interceptor.ContentType);
            Assert.Equal("one", interceptor.UserPropertyValue);
        }

        private sealed class InterceptorOrderRecorder
        {
            public List<string> Names { get; } = [];
        }

        private sealed class FirstPublishInterceptor : IMqttZeroPublishInterceptor
        {
            private readonly InterceptorOrderRecorder _recorder;

            public FirstPublishInterceptor(InterceptorOrderRecorder recorder)
            {
                _recorder = recorder;
            }

            public ValueTask<MqttZeroInterceptionResult> InterceptAsync(
                MqttZeroPublishContext context,
                CancellationToken cancellationToken)
            {
                _recorder.Names.Add("first");
                return ValueTask.FromResult(MqttZeroInterceptionResult.Continue);
            }
        }

        private sealed class SecondPublishInterceptor : IMqttZeroPublishInterceptor
        {
            private readonly InterceptorOrderRecorder _recorder;

            public SecondPublishInterceptor(InterceptorOrderRecorder recorder)
            {
                _recorder = recorder;
            }

            public ValueTask<MqttZeroInterceptionResult> InterceptAsync(
                MqttZeroPublishContext context,
                CancellationToken cancellationToken)
            {
                _recorder.Names.Add("second");
                return ValueTask.FromResult(MqttZeroInterceptionResult.Continue);
            }
        }

        private sealed class RecordingPublishInterceptor : IMqttZeroPublishInterceptor
        {
            public int InvocationCount { get; private set; }

            public string? Topic { get; private set; }

            public string? ContentType { get; private set; }

            public string? UserPropertyValue { get; private set; }

            public ValueTask<MqttZeroInterceptionResult> InterceptAsync(
                MqttZeroPublishContext context,
                CancellationToken cancellationToken)
            {
                InvocationCount++;
                Topic = System.Text.Encoding.UTF8.GetString(context.Topic.FirstSpan);
                ContentType = context.Properties.ContentType;
                UserPropertyValue = context.Properties.UserProperties.Count > 0
                    ? context.Properties.UserProperties[0].Value
                    : null;
                return ValueTask.FromResult(MqttZeroInterceptionResult.Continue);
            }
        }
    }
}
