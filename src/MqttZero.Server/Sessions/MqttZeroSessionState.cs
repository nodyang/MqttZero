using MqttZero.Memory;
using MqttZero.Protocol;
using System.Text;

namespace MqttZero.Sessions;

internal sealed class MqttZeroSessionState
{
    private int _nextPacketIdentifier;

    public MqttZeroSessionState(string clientId)
    {
        ClientId = clientId;
    }

    public string ClientId { get; }

    public object SyncRoot { get; } = new();

    public SemaphoreSlim DrainLock { get; } = new(1, 1);

    public List<MqttZeroSubscription> Subscriptions { get; } = [];

    public List<MqttZeroQueuedMessage> PendingMessages { get; } = [];

    public Dictionary<ushort, MqttZeroQueuedMessage> InflightMessages { get; } = [];

    public MqttZeroClientConnection? Connection { get; set; }

    public bool CleanStart { get; set; }

    public uint ExpiryInterval { get; set; }

    public DateTimeOffset? DisconnectedAt { get; set; }

    public MqttZeroWillMessage? WillMessage { get; set; }

    public DateTimeOffset? WillDueAt { get; set; }

    public ushort GetNextPacketIdentifier()
    {
        for (int attempt = 0; attempt < ushort.MaxValue; attempt++)
        {
            ushort candidate = (ushort)Interlocked.Increment(ref _nextPacketIdentifier);
            if (candidate != 0 && !InflightMessages.ContainsKey(candidate))
            {
                return candidate;
            }
        }

        throw new InvalidOperationException("No MQTT packet identifier is available.");
    }

    public MqttZeroSessionSnapshot CreateSnapshot(DateTimeOffset now)
    {
        int subscriptionCount;
        int pendingCount;
        int inflightCount;
        bool isConnected;
        bool cleanStart;
        uint expiryInterval;
        DateTimeOffset? expiresAt;
        MqttZeroWillMessage? willMessage;
        lock (SyncRoot)
        {
            subscriptionCount = Subscriptions.Count;
            pendingCount = PendingMessages.Count;
            inflightCount = InflightMessages.Count;
            isConnected = Connection != null;
            cleanStart = CleanStart;
            expiryInterval = ExpiryInterval;
            expiresAt = DisconnectedAt.HasValue && ExpiryInterval > 0
                ? DisconnectedAt.Value.AddSeconds(ExpiryInterval)
                : null;
            willMessage = WillMessage;
        }

        return new MqttZeroSessionSnapshot
        {
            ClientId = ClientId,
            IsConnected = isConnected,
            CleanStart = cleanStart,
            SessionPresent = true,
            ExpiryIntervalSeconds = expiryInterval,
            ExpiresAt = expiresAt,
            SubscriptionCount = subscriptionCount,
            PendingMessageCount = pendingCount,
            InflightMessageCount = inflightCount,
            HasWill = willMessage != null,
            WillTopic = willMessage == null ? null : Encoding.UTF8.GetString(willMessage.Topic),
            SampledAt = now
        };
    }
}
