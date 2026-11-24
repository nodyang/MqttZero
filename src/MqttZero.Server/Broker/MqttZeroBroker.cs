using MqttZero.Interceptors;
using MqttZero.Memory;
using MqttZero.Observers;
using MqttZero.Protocol;
using MqttZero.Routing;
using MqttZero.Sessions;
using Microsoft.Extensions.Options;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Text;

namespace MqttZero.Broker;

/// <summary>
/// Provides the in-memory MQTT 5 broker pipeline.
/// </summary>
public sealed partial class MqttZeroBroker : IAsyncDisposable
{
    private readonly object _sessionGate = new();
    private readonly MqttZeroBufferPool _bufferPool;
    private readonly IMqttZeroPublishInterceptor[] _publishInterceptors;
    private readonly IMqttZeroConnectionInterceptor[] _connectionInterceptors;
    private readonly IMqttZeroSubscribeInterceptor[] _subscribeInterceptors;
    private readonly IMqttZeroUnsubscribeInterceptor[] _unsubscribeInterceptors;
    private readonly IMqttZeroEnqueueInterceptor[] _enqueueInterceptors;
    private readonly IMqttZeroConnectionObserver[] _connectionObservers;
    private readonly IMqttZeroDeliveryObserver[] _deliveryObservers;
    private readonly ConcurrentDictionary<string, MqttZeroRetainedMessage> _retainedMessages =
        new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, MqttZeroSessionState> _sessions =
        new(StringComparer.Ordinal);
    private MqttZeroSubscriptionRouteSnapshot _subscriptionRouteSnapshot =
        MqttZeroSubscriptionRouteSnapshot.Empty;
    private readonly MqttZeroRouteScopePool _routeScopes = new();
    private readonly List<MqttZeroClientConnection> _keepAliveExpired = [];
    private readonly List<MqttZeroWillMessage> _dueWills = [];
    private readonly List<MqttZeroSessionState> _expiredSessions = [];
    private readonly List<string> _expiredRetainedTopics = [];
    private readonly ConcurrentDictionary<string, int> _sharedSubscriptionCursors =
        new(StringComparer.Ordinal);
    private readonly MqttZeroOptions _options;
    private readonly TimeProvider _timeProvider;
    private CancellationTokenSource? _maintenanceCancellation;
    private Task? _maintenanceTask;
    private int _connectionCount;
    private int _disposed;
    private long _publishedWillCount;
    private long _retainedBytes;
    private int _stopping;
    private int _state;

    /// <summary>Initializes a broker and its extension pipeline.</summary>
    /// <param name="options">The broker options accessor.</param>
    /// <param name="timeProvider">The time provider.</param>
    /// <param name="publishInterceptors">The ordered publish interceptors.</param>
    /// <param name="connectionInterceptors">The ordered CONNECT interceptors.</param>
    /// <param name="subscribeInterceptors">The ordered subscribe interceptors.</param>
    /// <param name="unsubscribeInterceptors">The ordered unsubscribe interceptors.</param>
    /// <param name="enqueueInterceptors">The ordered enqueue interceptors.</param>
    /// <param name="connectionObservers">The ordered connection observers.</param>
    /// <param name="deliveryObservers">The ordered delivery observers.</param>
    public MqttZeroBroker(
        IOptions<MqttZeroOptions> options,
        TimeProvider timeProvider,
        IEnumerable<IMqttZeroPublishInterceptor> publishInterceptors,
        IEnumerable<IMqttZeroConnectionInterceptor> connectionInterceptors,
        IEnumerable<IMqttZeroSubscribeInterceptor> subscribeInterceptors,
        IEnumerable<IMqttZeroUnsubscribeInterceptor> unsubscribeInterceptors,
        IEnumerable<IMqttZeroEnqueueInterceptor> enqueueInterceptors,
        IEnumerable<IMqttZeroConnectionObserver> connectionObservers,
        IEnumerable<IMqttZeroDeliveryObserver> deliveryObservers)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options.Value ?? throw new ArgumentNullException(nameof(options));
        _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
        ArgumentNullException.ThrowIfNull(publishInterceptors);
        _bufferPool = new MqttZeroBufferPool(_options.MaxBufferedBytes);
        _publishInterceptors = publishInterceptors.ToArray();
        _connectionInterceptors = connectionInterceptors.ToArray();
        _subscribeInterceptors = subscribeInterceptors.ToArray();
        _unsubscribeInterceptors = unsubscribeInterceptors.ToArray();
        _enqueueInterceptors = enqueueInterceptors.ToArray();
        _connectionObservers = connectionObservers.ToArray();
        _deliveryObservers = deliveryObservers.ToArray();
    }

    internal static MqttZeroBroker Create(
        MqttZeroOptions options,
        TimeProvider timeProvider,
        IEnumerable<IMqttZeroPublishInterceptor>? publishInterceptors = null)
    {
        return new MqttZeroBroker(
            Options.Create(options),
            timeProvider,
            publishInterceptors ?? [],
            [],
            [],
            [],
            [],
            [],
            []);
    }

    /// <summary>
    /// Gets the current broker lifecycle state.
    /// </summary>
    public MqttZeroBrokerState State
    {
        get
        {
            return (MqttZeroBrokerState)Volatile.Read(ref _state);
        }
    }

    /// <summary>
    /// Gets the current connected client count.
    /// </summary>
    public int ConnectedClientCount
    {
        get
        {
            return Volatile.Read(ref _connectionCount);
        }
    }

    /// <summary>
    /// Gets the number of will messages published since broker creation.
    /// </summary>
    public long PublishedWillCount
    {
        get
        {
            return Volatile.Read(ref _publishedWillCount);
        }
    }

    /// <summary>
    /// Gets the time provider used by the broker.
    /// </summary>
    public TimeProvider TimeProvider
    {
        get
        {
            return _timeProvider;
        }
    }

    /// <summary>
    /// Stops the broker and releases pooled resources.
    /// </summary>
    /// <returns>A task representing disposal.</returns>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await StopAsync().ConfigureAwait(false);
        _routeScopes.Dispose();
        _bufferPool.Dispose();
    }
}
