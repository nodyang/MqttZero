using Microsoft.Extensions.DependencyInjection;
using MqttZero.AspNetCore;

namespace MqttZero.Tests
{
    public sealed class PublishInterceptorTests
    {
        [Fact]
        public async Task RegisteredPublishInterceptorIsInvoked()
        {
            ServiceCollection services = new();
            services.AddMqttZero().AddInterceptor<RecordingPublishInterceptor>();
            await using ServiceProvider provider = services.BuildServiceProvider();

            RecordingPublishInterceptor interceptor = Assert.IsType<RecordingPublishInterceptor>(
                Assert.Single(provider.GetServices<IMqttZeroPublishInterceptor>()));
            MqttZeroBroker broker = provider.GetRequiredService<MqttZeroBroker>();
            await broker.StartAsync(TestContext.Current.CancellationToken);
            await broker.PublishAsync(
                "devices/a",
                "value"u8.ToArray(),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                TestContext.Current.CancellationToken);

            Assert.Equal(1, interceptor.InvocationCount);
            Assert.Equal("devices/a", interceptor.Topic);
        }

        private sealed class RecordingPublishInterceptor : IMqttZeroPublishInterceptor
        {
            public int InvocationCount { get; private set; }

            public string? Topic { get; private set; }

            public ValueTask<MqttZeroInterceptionResult> InterceptAsync(
                MqttZeroPublishContext context,
                CancellationToken cancellationToken)
            {
                InvocationCount++;
                Topic = System.Text.Encoding.UTF8.GetString(context.Topic.FirstSpan);
                return ValueTask.FromResult(MqttZeroInterceptionResult.Continue);
            }
        }
    }
}
