using MqttZero.Tests.Support;
using MqttZero.Broker;
using MqttZero.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Routing;
using MqttZero.Sessions;
using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Reflection;


namespace MqttZero.Tests.Broker
{
    public sealed class BrokerHotPathAllocationTests
    {
        [Fact]
        public async Task OfflineQueuePromotesBorrowedLeaseAndReleasesOwnedCopy()
        {
            MqttZeroBufferPool pool = new(1024 * 1024);
            MqttZeroBroker broker = new(new MqttZeroOptions(), TimeProvider.System);
            await broker.StartAsync(TestContext.Current.CancellationToken);
            FieldInfo bufferPoolField = typeof(MqttZeroBroker).GetField(
                "_bufferPool",
                BindingFlags.Instance | BindingFlags.NonPublic)!;
            bufferPoolField.SetValue(broker, pool);

            MqttZeroSessionState receiver = new("offline");
            receiver.Subscriptions.Add(new MqttZeroSubscription(
                "hot/#"u8.ToArray(),
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                false,
                null));
            FieldInfo sessionsField = typeof(MqttZeroBroker).GetField(
                "_sessions",
                BindingFlags.Instance | BindingFlags.NonPublic)!;
            ConcurrentDictionary<string, MqttZeroSessionState> sessions =
                (ConcurrentDictionary<string, MqttZeroSessionState>)sessionsField.GetValue(broker)!;
            Assert.True(sessions.TryAdd(receiver.ClientId, receiver));
            MethodInfo rebuildMethod = typeof(MqttZeroBroker).GetMethod(
                "RebuildSubscriptionRouteSnapshot",
                BindingFlags.Instance | BindingFlags.NonPublic)!;
            rebuildMethod.Invoke(broker, null);

            AllocationPipeWriter senderWriter = new(16 * 1024);
            MqttZeroClientConnection senderConnection = new(
                senderWriter,
                "sender",
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System,
                pool);
            MqttZeroSessionState sender = new("sender");
            MqttZeroMessageLease borrowed = MqttZeroMessageLease.BorrowFrom(
                new ReadOnlySequence<byte>("hot/topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>(new byte[1024]),
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                null,
                null,
                pool);
            try
            {
                await broker.RoutePublishLeaseAsync(
                    sender,
                    senderConnection,
                    borrowed,
                    TestContext.Current.CancellationToken);
            }
            finally
            {
                borrowed.Release();
            }

            Assert.Equal(1, receiver.PendingMessages.Count);
            Assert.True(pool.RentedBytes > 0);
            await broker.DisposeAsync();
            Assert.Equal(0, pool.RentedBytes);
        }
        [Fact]
        public async Task BrokerPublishWithoutInterceptorDoesNotAllocateAfterWarmup()
        {
            await AssertBrokerPublishDoesNotAllocateAfterWarmup([]);
        }
        [Fact]
        public async Task BrokerPublishWithSynchronousInterceptorDoesNotAllocateAfterWarmup()
        {
            await AssertBrokerPublishDoesNotAllocateAfterWarmup([new NoOpPublishInterceptor()]);
        }
        [Fact]
        public async Task ClientConnectionPublishSendDoesNotAllocateAfterWarmup()
        {
            AllocationPipeWriter receiverWriter = new(16 * 1024);
            MqttZeroClientConnection connection = new(
                receiverWriter,
                "memory",
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("hot/topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>(new byte[1024]),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                null,
                null);
            MqttZeroQueuedMessage message = MqttZeroQueuedMessage.Rent(
                lease,
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                null,
                null);

            for (int index = 0; index < 10; index++)
            {
                await connection.SendPublishAsync(
                    message,
                    false,
                    TestContext.Current.CancellationToken);
            }

            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int index = 0; index < 1000; index++)
            {
                await connection.SendPublishAsync(
                    message,
                    false,
                    TestContext.Current.CancellationToken);
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            message.Release();
            lease.Release();
            await connection.DisposeAsync();
            Assert.Equal(0, allocated);
        }

        [Fact]
        public async Task ClientConnectionControlPacketsDoNotAllocateAfterWarmup()
        {
            AllocationPipeWriter writer = new(1024);
            MqttZeroClientConnection connection = new(
                writer,
                "memory",
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            MqttConnAckPacket connAck = new()
            {
                SessionPresent = true,
                ReasonCode = MqttReasonCode.Success,
                TopicAliasMaximum = 16,
                ReceiveMaximum = 32,
                MaximumPacketSize = 1024 * 1024,
                SessionExpiryInterval = 60
            };
            MqttDisconnectPacket disconnect = new()
            {
                ReasonCode = MqttReasonCode.ServerShuttingDown,
                SessionExpiryInterval = 60
            };

            for (int index = 0; index < 10; index++)
            {
                await connection.SendAsync(
                    connAck,
                    TestContext.Current.CancellationToken);
                await connection.SendAsync(
                    disconnect,
                    TestContext.Current.CancellationToken);
            }

            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int index = 0; index < 1000; index++)
            {
                await connection.SendAsync(
                    connAck,
                    TestContext.Current.CancellationToken);
                await connection.SendAsync(
                    disconnect,
                    TestContext.Current.CancellationToken);
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            await connection.DisposeAsync();
            Assert.Equal(0, allocated);
        }
        [Fact]
        public async Task BrokerQosOnePublishAndPubAckDoNotAllocateAfterWarmup()
        {
            MqttZeroBroker broker = new(new MqttZeroOptions(), TimeProvider.System);
            MqttZeroSessionState receiver = new("qos-receiver");
            receiver.Subscriptions.Add(new MqttZeroSubscription(
                "hot/#"u8.ToArray(),
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                false,
                null));
            AllocationPipeWriter receiverWriter = new(16 * 1024);
            MqttZeroClientConnection receiverConnection = new(
                receiverWriter,
                "receiver",
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            receiver.Connection = receiverConnection;
            FieldInfo sessionsField = typeof(MqttZeroBroker).GetField(
                "_sessions",
                BindingFlags.Instance | BindingFlags.NonPublic)!;
            ConcurrentDictionary<string, MqttZeroSessionState> sessions =
                (ConcurrentDictionary<string, MqttZeroSessionState>)sessionsField.GetValue(broker)!;
            Assert.True(sessions.TryAdd(receiver.ClientId, receiver));
            MethodInfo rebuildMethod = typeof(MqttZeroBroker).GetMethod(
                "RebuildSubscriptionRouteSnapshot",
                BindingFlags.Instance | BindingFlags.NonPublic)!;
            rebuildMethod.Invoke(broker, null);

            AllocationPipeWriter senderWriter = new(16 * 1024);
            MqttZeroClientConnection senderConnection = new(
                senderWriter,
                "sender",
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            MqttZeroSessionState sender = new("qos-sender");
            MqttPublishPacket packet = new()
            {
                TopicName = new ReadOnlySequence<byte>("hot/topic"u8.ToArray()),
                Payload = new ReadOnlySequence<byte>(new byte[1024]),
                QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce,
                PacketIdentifier = 7
            };
            ArrayBufferWriter<byte> encodedWriter = new();
            MqttPacketCodec.Encode(packet, encodedWriter);
            ReadOnlySequence<byte> encodedPublish = new(encodedWriter.WrittenMemory);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;

            for (int index = 0; index < 10; index++)
            {
                await RouteQosOnePublishAndAcknowledgeAsync(
                    broker,
                    sender,
                    senderConnection,
                    receiver,
                    receiverConnection,
                    encodedPublish,
                    senderWriter,
                    receiverWriter,
                    cancellationToken);
            }

            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int index = 0; index < 1000; index++)
            {
                await RouteQosOnePublishAndAcknowledgeAsync(
                    broker,
                    sender,
                    senderConnection,
                    receiver,
                    receiverConnection,
                    encodedPublish,
                    senderWriter,
                    receiverWriter,
                    cancellationToken);
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.Empty(receiver.InflightMessages);
            await broker.DisposeAsync();
            Assert.Equal(0, allocated);
        }
        private static async ValueTask RouteQosOnePublishAndAcknowledgeAsync(
            MqttZeroBroker broker,
            MqttZeroSessionState sender,
            MqttZeroClientConnection senderConnection,
            MqttZeroSessionState receiver,
            MqttZeroClientConnection receiverConnection,
            ReadOnlySequence<byte> encodedPublish,
            AllocationPipeWriter senderWriter,
            AllocationPipeWriter receiverWriter,
            CancellationToken cancellationToken)
        {
            if (!MqttPublishPacketCodec.TryDecode(
                encodedPublish,
                out MqttZeroPublishView view,
                out _))
            {
                throw new InvalidOperationException("The QoS 1 benchmark PUBLISH could not be decoded.");
            }

            senderWriter.Reset();
            receiverWriter.Reset();
            MqttZeroMessageLease lease = broker.CreateMessageLease(senderConnection, view);
            try
            {
                await broker.RoutePublishLeaseAsync(
                    sender,
                    senderConnection,
                    lease,
                    cancellationToken);
            }
            finally
            {
                lease.Release();
            }

            lock (receiver.SyncRoot)
            {
                ushort packetIdentifier = 0;
                MqttZeroQueuedMessage? acknowledged = null;
                foreach (KeyValuePair<ushort, MqttZeroQueuedMessage> pair in
                    receiver.InflightMessages)
                {
                    packetIdentifier = pair.Key;
                    acknowledged = pair.Value;
                    break;
                }

                if (acknowledged == null)
                {
                    throw new InvalidOperationException("The QoS 1 PUBLISH was not in flight.");
                }

                receiver.InflightMessages.Remove(packetIdentifier);
                acknowledged.Release();
            }
        }

        private static async Task AssertBrokerPublishDoesNotAllocateAfterWarmup(
            IMqttZeroPublishInterceptor[] interceptors)
        {
            MqttZeroBroker broker = new(new MqttZeroOptions(), TimeProvider.System, interceptors);
            FieldInfo sessionsField = typeof(MqttZeroBroker).GetField(
                "_sessions",
                BindingFlags.Instance | BindingFlags.NonPublic)!;
            ConcurrentDictionary<string, MqttZeroSessionState> sessions =
                (ConcurrentDictionary<string, MqttZeroSessionState>)sessionsField.GetValue(broker)!;
            MqttZeroSessionState receiver = new("receiver");
            receiver.Subscriptions.Add(new MqttZeroSubscription(
                "hot/#"u8.ToArray(),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                false,
                null));
            AllocationPipeWriter receiverWriter = new(16 * 1024);
            MqttZeroClientConnection receiverConnection = new(
                receiverWriter,
                "memory",
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);
            receiver.Connection = receiverConnection;
            Assert.True(sessions.TryAdd(receiver.ClientId, receiver));
            MethodInfo rebuildMethod = typeof(MqttZeroBroker).GetMethod(
                "RebuildSubscriptionRouteSnapshot",
                BindingFlags.Instance | BindingFlags.NonPublic)!;
            rebuildMethod.Invoke(broker, null);

            MqttZeroSessionState sender = new("sender");
            MqttPublishPacket packet = new()
            {
                TopicName = new ReadOnlySequence<byte>("hot/topic"u8.ToArray()),
                Payload = new ReadOnlySequence<byte>(new byte[1024])
            };
            ArrayBufferWriter<byte> encodedWriter = new();
            MqttPacketCodec.Encode(packet, encodedWriter);
            ReadOnlySequence<byte> encodedPublish = new(encodedWriter.WrittenMemory);
            CancellationToken cancellationToken = TestContext.Current.CancellationToken;

            for (int index = 0; index < 10; index++)
            {
                receiverWriter.Reset();
                await RouteEncodedPublishAsync(
                    broker,
                    sender,
                    receiverConnection,
                    encodedPublish,
                    cancellationToken);
            }

            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int index = 0; index < 1000; index++)
            {
                receiverWriter.Reset();
                await RouteEncodedPublishAsync(
                    broker,
                    sender,
                    receiverConnection,
                    encodedPublish,
                    cancellationToken);
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            await broker.DisposeAsync();
            Assert.Equal(0, allocated);
        }

        private static async ValueTask RouteEncodedPublishAsync(
            MqttZeroBroker broker,
            MqttZeroSessionState sender,
            MqttZeroClientConnection receiverConnection,
            ReadOnlySequence<byte> encodedPublish,
            CancellationToken cancellationToken)
        {
            if (!MqttPublishPacketCodec.TryDecode(
                encodedPublish,
                out MqttZeroPublishView view,
                out _))
            {
                throw new InvalidOperationException("The benchmark PUBLISH could not be decoded.");
            }

            MqttZeroMessageLease lease = broker.CreateMessageLease(receiverConnection, view);
            try
            {
                await broker.RoutePublishLeaseAsync(
                    sender,
                    receiverConnection,
                    lease,
                    cancellationToken);
            }
            finally
            {
                lease.Release();
            }
        }

    }
}
