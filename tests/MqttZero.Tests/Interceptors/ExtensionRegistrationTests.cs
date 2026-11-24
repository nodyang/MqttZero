using Microsoft.Extensions.DependencyInjection;
using MqttZero.AspNetCore;
using MqttZero.Server.Interceptors;
using MqttZero.Server.Observers;

namespace MqttZero.Tests.Interceptors
{
    public sealed class ExtensionRegistrationTests
    {
        [Fact]
        public void ExtensionsResolveInRegistrationOrder()
        {
            ServiceCollection services = new();
            services.AddMqttZero()
                .AddConnectionInterceptor<FirstConnectionInterceptor>()
                .AddConnectionInterceptor<SecondConnectionInterceptor>()
                .AddSubscribeInterceptor<FirstSubscribeInterceptor>()
                .AddSubscribeInterceptor<SecondSubscribeInterceptor>()
                .AddUnsubscribeInterceptor<FirstUnsubscribeInterceptor>()
                .AddUnsubscribeInterceptor<SecondUnsubscribeInterceptor>()
                .AddEnqueueInterceptor<FirstEnqueueInterceptor>()
                .AddEnqueueInterceptor<SecondEnqueueInterceptor>()
                .AddConnectionObserver<FirstConnectionObserver>()
                .AddConnectionObserver<SecondConnectionObserver>()
                .AddDeliveryObserver<FirstDeliveryObserver>()
                .AddDeliveryObserver<SecondDeliveryObserver>();
            using ServiceProvider provider = services.BuildServiceProvider();

            Assert.IsType<FirstConnectionInterceptor>(
                provider.GetServices<IMqttZeroConnectionInterceptor>().First());
            Assert.IsType<SecondConnectionInterceptor>(
                provider.GetServices<IMqttZeroConnectionInterceptor>().Last());
            Assert.IsType<FirstSubscribeInterceptor>(
                provider.GetServices<IMqttZeroSubscribeInterceptor>().First());
            Assert.IsType<SecondSubscribeInterceptor>(
                provider.GetServices<IMqttZeroSubscribeInterceptor>().Last());
            Assert.IsType<FirstUnsubscribeInterceptor>(
                provider.GetServices<IMqttZeroUnsubscribeInterceptor>().First());
            Assert.IsType<SecondUnsubscribeInterceptor>(
                provider.GetServices<IMqttZeroUnsubscribeInterceptor>().Last());
            Assert.IsType<FirstEnqueueInterceptor>(
                provider.GetServices<IMqttZeroEnqueueInterceptor>().First());
            Assert.IsType<SecondEnqueueInterceptor>(
                provider.GetServices<IMqttZeroEnqueueInterceptor>().Last());
            Assert.IsType<FirstConnectionObserver>(
                provider.GetServices<IMqttZeroConnectionObserver>().First());
            Assert.IsType<SecondConnectionObserver>(
                provider.GetServices<IMqttZeroConnectionObserver>().Last());
            Assert.IsType<FirstDeliveryObserver>(
                provider.GetServices<IMqttZeroDeliveryObserver>().First());
            Assert.IsType<SecondDeliveryObserver>(
                provider.GetServices<IMqttZeroDeliveryObserver>().Last());
        }

        private sealed class FirstConnectionInterceptor : IMqttZeroConnectionInterceptor
        {
            public ValueTask<MqttZeroConnectionInterceptionResult> InterceptAsync(
                MqttZeroConnectionContext context,
                CancellationToken cancellationToken)
            {
                return ValueTask.FromResult(MqttZeroConnectionInterceptionResult.Continue);
            }
        }

        private sealed class SecondConnectionInterceptor : IMqttZeroConnectionInterceptor
        {
            public ValueTask<MqttZeroConnectionInterceptionResult> InterceptAsync(
                MqttZeroConnectionContext context,
                CancellationToken cancellationToken)
            {
                return ValueTask.FromResult(MqttZeroConnectionInterceptionResult.Continue);
            }
        }

        private sealed class FirstSubscribeInterceptor : IMqttZeroSubscribeInterceptor
        {
            public ValueTask<MqttZeroInterceptionResult> InterceptAsync(
                MqttZeroSubscribeContext context,
                CancellationToken cancellationToken)
            {
                return ValueTask.FromResult(MqttZeroInterceptionResult.Continue);
            }
        }

        private sealed class SecondSubscribeInterceptor : IMqttZeroSubscribeInterceptor
        {
            public ValueTask<MqttZeroInterceptionResult> InterceptAsync(
                MqttZeroSubscribeContext context,
                CancellationToken cancellationToken)
            {
                return ValueTask.FromResult(MqttZeroInterceptionResult.Continue);
            }
        }

        private sealed class FirstUnsubscribeInterceptor : IMqttZeroUnsubscribeInterceptor
        {
            public ValueTask<MqttZeroInterceptionResult> InterceptAsync(
                MqttZeroUnsubscribeContext context,
                CancellationToken cancellationToken)
            {
                return ValueTask.FromResult(MqttZeroInterceptionResult.Continue);
            }
        }

        private sealed class SecondUnsubscribeInterceptor : IMqttZeroUnsubscribeInterceptor
        {
            public ValueTask<MqttZeroInterceptionResult> InterceptAsync(
                MqttZeroUnsubscribeContext context,
                CancellationToken cancellationToken)
            {
                return ValueTask.FromResult(MqttZeroInterceptionResult.Continue);
            }
        }

        private sealed class FirstEnqueueInterceptor : IMqttZeroEnqueueInterceptor
        {
            public ValueTask<MqttZeroEnqueueInterceptionResult> InterceptAsync(
                MqttZeroEnqueueContext context,
                CancellationToken cancellationToken)
            {
                return ValueTask.FromResult(MqttZeroEnqueueInterceptionResult.Enqueue);
            }
        }

        private sealed class SecondEnqueueInterceptor : IMqttZeroEnqueueInterceptor
        {
            public ValueTask<MqttZeroEnqueueInterceptionResult> InterceptAsync(
                MqttZeroEnqueueContext context,
                CancellationToken cancellationToken)
            {
                return ValueTask.FromResult(MqttZeroEnqueueInterceptionResult.Enqueue);
            }
        }

        private sealed class FirstConnectionObserver : IMqttZeroConnectionObserver
        {
            public ValueTask OnConnectedAsync(
                MqttZeroClientConnectedContext context,
                CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }

            public ValueTask OnDisconnectedAsync(
                MqttZeroClientDisconnectedContext context,
                CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }
        }

        private sealed class SecondConnectionObserver : IMqttZeroConnectionObserver
        {
            public ValueTask OnConnectedAsync(
                MqttZeroClientConnectedContext context,
                CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }

            public ValueTask OnDisconnectedAsync(
                MqttZeroClientDisconnectedContext context,
                CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }
        }

        private sealed class FirstDeliveryObserver : IMqttZeroDeliveryObserver
        {
            public ValueTask OnMessageEnqueuedAsync(
                MqttZeroMessageEnqueuedContext context,
                CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }

            public ValueTask OnMessageDroppedAsync(
                MqttZeroMessageDroppedContext context,
                CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }

            public ValueTask OnQueueMessageOverwrittenAsync(
                MqttZeroQueueMessageOverwrittenContext context,
                CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }

            public ValueTask OnMessageNotConsumedAsync(
                MqttZeroMessageNotConsumedContext context,
                CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }
        }

        private sealed class SecondDeliveryObserver : IMqttZeroDeliveryObserver
        {
            public ValueTask OnMessageEnqueuedAsync(
                MqttZeroMessageEnqueuedContext context,
                CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }

            public ValueTask OnMessageDroppedAsync(
                MqttZeroMessageDroppedContext context,
                CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }

            public ValueTask OnQueueMessageOverwrittenAsync(
                MqttZeroQueueMessageOverwrittenContext context,
                CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }

            public ValueTask OnMessageNotConsumedAsync(
                MqttZeroMessageNotConsumedContext context,
                CancellationToken cancellationToken)
            {
                return ValueTask.CompletedTask;
            }
        }
    }
}
