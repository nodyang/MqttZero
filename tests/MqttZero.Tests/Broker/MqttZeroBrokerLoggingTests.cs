using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using MqttZero.Server.Broker;

namespace MqttZero.Tests.Broker
{
    public sealed class MqttZeroBrokerLoggingTests
    {
        [Fact]
        public void NullLoggerDisablesAllBrokerLogMessages()
        {
            InvalidOperationException exception = new("failure");

            MqttZeroBrokerLogging.LogMaintenanceFailure(
                NullLogger<MqttZeroBroker>.Instance,
                exception);
            MqttZeroBrokerLogging.LogConnectionCleanupFailure(
                NullLogger<MqttZeroBroker>.Instance,
                exception);
            MqttZeroBrokerLogging.LogObserverFailure(
                NullLogger<MqttZeroBroker>.Instance,
                exception);
            MqttZeroBrokerLogging.LogPersistenceFailure(
                NullLogger<MqttZeroBroker>.Instance,
                "save",
                "client",
                exception);
        }

        [Fact]
        public void EnabledLoggerReceivesAllBrokerLogMessages()
        {
            RecordingLogger logger = new();
            InvalidOperationException exception = new("failure");

            MqttZeroBrokerLogging.LogMaintenanceFailure(logger, exception);
            MqttZeroBrokerLogging.LogConnectionCleanupFailure(logger, exception);
            MqttZeroBrokerLogging.LogObserverFailure(logger, exception);
            MqttZeroBrokerLogging.LogPersistenceFailure(
                logger,
                "save",
                "client",
                exception);

            Assert.Equal(4, logger.Messages.Count);
            Assert.Contains(
                logger.Messages,
                message => message.Contains("persistence", StringComparison.OrdinalIgnoreCase));
        }

        private sealed class RecordingLogger : ILogger<MqttZeroBroker>
        {
            public List<string> Messages { get; } = [];

            public IDisposable? BeginScope<TState>(TState state)
                where TState : notnull
            {
                return null;
            }

            public bool IsEnabled(LogLevel logLevel)
            {
                return true;
            }

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                Messages.Add(formatter(state, exception));
            }
        }
    }
}
