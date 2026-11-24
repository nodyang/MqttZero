namespace MqttZero.Server.Broker;

internal static class MqttZeroOptionsValidator
{
    public static void Validate(MqttZeroOptions options)
    {
        ValidatePositive(
            options.MaxActiveConnections,
            nameof(options.MaxActiveConnections));
        ValidatePositive(options.MaxSessions, nameof(options.MaxSessions));
        ValidatePositive(
            options.MaxSubscriptionsPerSession,
            nameof(options.MaxSubscriptionsPerSession));
        ValidatePositive(options.MaxInflight, nameof(options.MaxInflight));
        ValidatePositive(
            options.MaxMessageQueueLength,
            nameof(options.MaxMessageQueueLength));
        ValidatePositive(options.MaxPacketSize, nameof(options.MaxPacketSize));
        if (options.MaxBufferedBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "MqttZeroOptions.MaxBufferedBytes must be positive.");
        }

        ValidatePositive(
            options.MaxRetainedMessages,
            nameof(options.MaxRetainedMessages));
        if (options.MaxRetainedBytes <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "MqttZeroOptions.MaxRetainedBytes must be positive.");
        }

        ValidatePositive(options.ConnectTimeout, nameof(options.ConnectTimeout));
        ValidatePositive(
            options.IncompletePacketTimeout,
            nameof(options.IncompletePacketTimeout));
        ValidatePositive(
            options.KeepAliveCheckInterval,
            nameof(options.KeepAliveCheckInterval));
        if (options.MaxSessionExpiryInterval < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "MqttZeroOptions.MaxSessionExpiryInterval cannot be negative.");
        }
    }

    private static void ValidatePositive(int value, string propertyName)
    {
        if (value <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(value),
                $"MqttZeroOptions.{propertyName} must be positive.");
        }
    }

    private static void ValidatePositive(TimeSpan value, string propertyName)
    {
        if (value <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(value),
                $"MqttZeroOptions.{propertyName} must be positive.");
        }
    }
}
