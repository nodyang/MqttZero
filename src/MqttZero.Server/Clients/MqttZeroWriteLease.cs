namespace MqttZero.Server.Clients;

/// <summary>
/// Holds the per-connection write gate and releases it on dispose.
/// </summary>
internal readonly struct MqttZeroWriteLease : IDisposable
{
    private readonly MqttZeroClientConnection? _connection;

    internal MqttZeroWriteLease(MqttZeroClientConnection connection)
    {
        _connection = connection;
    }

    public void Dispose()
    {
        _connection?.ReleaseWrite();
    }
}
