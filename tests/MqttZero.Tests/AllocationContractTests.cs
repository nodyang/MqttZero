using System.Buffers;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Reflection;

namespace MqttZero.Tests
{
    public sealed class AllocationContractTests
    {
        [Fact]
        public void PooledMessageLeaseDoesNotAllocateAfterWarmup()
        {
            ReadOnlySequence<byte> topic = new("hot/topic"u8.ToArray());
            ReadOnlySequence<byte> payload = new(new byte[8192]);
            ReadOnlySequence<byte> properties = ReadOnlySequence<byte>.Empty;

            using (MqttZeroMessageLease warmup = MqttZeroMessageLease.CopyFrom(
                topic,
                properties,
                payload,
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                null,
                null))
            {
                Assert.Equal(8192, warmup.Payload.Length);
            }

            int total = 0;
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int index = 0; index < 1_000; index++)
            {
                using MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                    topic,
                    properties,
                    payload,
                    MqttQualityOfServiceLevel.AtLeastOnce,
                    false,
                    null,
                    null);
                total += lease.Payload.Length;
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.Equal(8_192_000, total);
            Assert.Equal(0, allocated);
        }

        [Fact]
        public void MessageLeaseReturnsBufferOnlyAfterLastReference()
        {
            MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>("payload"u8.ToArray()),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                null,
                null);
            lease.Retain();

            lease.Release();
            Assert.Equal("payload", System.Text.Encoding.UTF8.GetString(lease.Payload.Span));
            lease.Release();
        }

        [Fact]
        public void MessageLeaseReturnsBuffersToInjectedPoolAfterLastReference()
        {
            using MqttZeroBufferPool pool = new(1024 * 1024);
            MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>("payload"u8.ToArray()),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                null,
                null,
                pool);

            Assert.True(pool.RentedBytes > 0);
            lease.Release();

            Assert.Equal(0, pool.RentedBytes);
        }

        [Fact]
        public void MessageLeaseHandlesConcurrentReferencesAndFinalRelease()
        {
            using MqttZeroBufferPool pool = new(1024 * 1024);
            MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>("payload"u8.ToArray()),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                null,
                null,
                pool);

            Parallel.For(0, 1000, _ => lease.Retain());
            Parallel.For(0, 1000, _ => lease.Release());
            Assert.True(pool.RentedBytes > 0);

            lease.Release();
            Assert.Equal(0, pool.RentedBytes);
        }

        [Fact]
        public void MessageLeaseRejectsDuplicateRelease()
        {
            MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>("payload"u8.ToArray()),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                null,
                null);

            lease.Release();

            Assert.Throws<ObjectDisposedException>(() => lease.Release());
        }

        [Fact]
        public void BorrowedMessageLeaseDefersCopiesUntilOwnedCopyIsRequested()
        {
            using MqttZeroBufferPool pool = new(1024 * 1024);
            MqttZeroMessageLease borrowed = MqttZeroMessageLease.BorrowFrom(
                new ReadOnlySequence<byte>("topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>("payload"u8.ToArray()),
                MqttQualityOfServiceLevel.AtMostOnce,
                false,
                null,
                null,
                pool);

            Assert.True(borrowed.IsBorrowed);
            Assert.Equal(0, pool.RentedBytes);
            MqttZeroMessageLease owned = borrowed.CreateOwnedCopy();
            Assert.False(owned.IsBorrowed);
            Assert.True(pool.RentedBytes > 0);

            borrowed.Release();
            owned.Release();
            Assert.Equal(0, pool.RentedBytes);
        }

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

            Assert.Single(receiver.PendingMessages);
            Assert.True(pool.RentedBytes > 0);
            await broker.DisposeAsync();
            Assert.Equal(0, pool.RentedBytes);
        }

        [Fact]
        public void QueuedMessageRejectsDuplicateRelease()
        {
            MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                new ReadOnlySequence<byte>("topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                new ReadOnlySequence<byte>("payload"u8.ToArray()),
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

            message.Release();
            lease.Release();

            Assert.Throws<ObjectDisposedException>(() => message.Release());
        }

        [Fact]
        public void PublishFastCodecDoesNotAllocateAfterWarmup()
        {
            MqttPublishPacket packet = new()
            {
                TopicName = new ReadOnlySequence<byte>("hot/topic"u8.ToArray()),
                Payload = new ReadOnlySequence<byte>(new byte[8192]),
                MessageExpiryInterval = 60,
                SubscriptionIdentifier = 7
            };
            ArrayBufferWriter<byte> setupWriter = new();
            MqttPacketCodec.Encode(packet, setupWriter);
            ReadOnlySequence<byte> source = new(setupWriter.WrittenMemory);
            PreallocatedBufferWriter writer = new(16 * 1024);

            for (int index = 0; index < 10; index++)
            {
                _ = MqttPublishPacketCodec.TryDecode(source, out MqttZeroPublishView warmup, out _);
                writer.Reset();
                MqttPublishPacketCodec.Encode(warmup, writer);
            }

            int totalPayload = 0;
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int index = 0; index < 1_000; index++)
            {
                bool decoded = MqttPublishPacketCodec.TryDecode(
                    source,
                    out MqttZeroPublishView view,
                    out _);
                if (!decoded)
                {
                    throw new InvalidOperationException("The fast PUBLISH decoder failed.");
                }

                totalPayload += (int)view.Payload.Length;
                writer.Reset();
                MqttPublishPacketCodec.Encode(view, writer);
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.Equal(8_192_000, totalPayload);
            Assert.Equal(0, allocated);
        }

        [Theory]
        [InlineData(8 * 1024, 1)]
        [InlineData(8 * 1024, 10)]
        [InlineData(64 * 1024, 1)]
        [InlineData(64 * 1024, 10)]
        public void FastPublishCodecHandlesSegmentedPayloadsWithoutAllocation(
            int payloadSize,
            int segmentCount)
        {
            MqttPublishPacket packet = new()
            {
                TopicName = new ReadOnlySequence<byte>("hot/topic"u8.ToArray()),
                Payload = new ReadOnlySequence<byte>(new byte[payloadSize]),
                MessageExpiryInterval = 60,
                SubscriptionIdentifier = 7
            };
            ArrayBufferWriter<byte> setupWriter = new();
            MqttPacketCodec.Encode(packet, setupWriter);
            ReadOnlySequence<byte> source = CreateSegmentedSequence(
                setupWriter.WrittenSpan.ToArray(),
                segmentCount);
            PreallocatedBufferWriter writer = new(payloadSize + 1024);

            for (int index = 0; index < 10; index++)
            {
                _ = MqttPublishPacketCodec.TryDecode(source, out MqttZeroPublishView warmup, out _);
                writer.Reset();
                MqttPublishPacketCodec.Encode(warmup, writer);
            }

            int totalPayload = 0;
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int index = 0; index < 100; index++)
            {
                bool decoded = MqttPublishPacketCodec.TryDecode(
                    source,
                    out MqttZeroPublishView view,
                    out _);
                if (!decoded)
                {
                    throw new InvalidOperationException("The segmented PUBLISH decoder failed.");
                }

                totalPayload += (int)view.Payload.Length;
                writer.Reset();
                MqttPublishPacketCodec.Encode(view, writer);
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.Equal(payloadSize * 100, totalPayload);
            Assert.Equal(0, allocated);
        }

        [Fact]
        public void PooledQosOneQueueAndInflightDoNotAllocateAfterWarmup()
        {
            MqttZeroSessionState session = new("qos-one");
            ReadOnlySequence<byte> topic = new("hot/topic"u8.ToArray());
            ReadOnlySequence<byte> payload = new(new byte[1024]);

            for (int index = 0; index < 10; index++)
            {
                ExerciseQosOneQueueAndInflight(session, topic, payload);
            }

            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int index = 0; index < 1000; index++)
            {
                ExerciseQosOneQueueAndInflight(session, topic, payload);
            }

            long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.Empty(session.PendingMessages);
            Assert.Empty(session.InflightMessages);
            Assert.Equal(0, allocated);
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

        private static void ExerciseQosOneQueueAndInflight(
            MqttZeroSessionState session,
            ReadOnlySequence<byte> topic,
            ReadOnlySequence<byte> payload)
        {
            MqttZeroMessageLease lease = MqttZeroMessageLease.CopyFrom(
                topic,
                ReadOnlySequence<byte>.Empty,
                payload,
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                null,
                null);
            MqttZeroQueuedMessage queued = MqttZeroQueuedMessage.Rent(
                lease,
                MqttQualityOfServiceLevel.AtLeastOnce,
                false,
                null,
                null);
            session.PendingMessages.Add(queued);
            session.PendingMessages.RemoveAt(0);
            queued.PacketIdentifier = 1;
            session.InflightMessages.Add(1, queued);
            session.InflightMessages.Remove(1, out MqttZeroQueuedMessage? inflight);
            Assert.NotNull(inflight);
            inflight.Release();
            lease.Release();
        }

        private static ReadOnlySequence<byte> CreateSegmentedSequence(
            byte[] bytes,
            int segmentCount)
        {
            if (segmentCount == 1)
            {
                return new ReadOnlySequence<byte>(bytes);
            }

            int segmentLength = (bytes.Length + segmentCount - 1) / segmentCount;
            SequenceSegment? first = null;
            SequenceSegment? last = null;
            for (int offset = 0; offset < bytes.Length; offset += segmentLength)
            {
                int length = Math.Min(segmentLength, bytes.Length - offset);
                SequenceSegment segment = new(bytes.AsMemory(offset, length));
                if (first == null)
                {
                    first = segment;
                }
                else
                {
                    last!.SetNext(segment);
                }

                last = segment;
            }

            return new ReadOnlySequence<byte>(first!, 0, last!, last!.Memory.Length);
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

        private sealed class NoOpPublishInterceptor : IMqttZeroPublishInterceptor
        {
            public ValueTask<MqttZeroInterceptionResult> InterceptAsync(
                MqttZeroPublishContext context,
                CancellationToken cancellationToken)
            {
                return ValueTask.FromResult(MqttZeroInterceptionResult.Continue);
            }
        }

        private sealed class AllocationPipeWriter : PipeWriter
        {
            private readonly byte[] _buffer;
            private int _written;

            public AllocationPipeWriter(int capacity)
            {
                _buffer = new byte[capacity];
            }

            public override void Advance(int bytes)
            {
                _written += bytes;
            }

            public override void CancelPendingFlush()
            {
            }

            public override void Complete(Exception? exception = null)
            {
            }

            public override ValueTask CompleteAsync(Exception? exception = null)
            {
                return ValueTask.CompletedTask;
            }

            public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
            {
                Reset();
                return ValueTask.FromResult(new FlushResult(false, false));
            }

            public override Memory<byte> GetMemory(int sizeHint = 0)
            {
                return _buffer.AsMemory(_written);
            }

            public override Span<byte> GetSpan(int sizeHint = 0)
            {
                return _buffer.AsSpan(_written);
            }

            public override ValueTask<FlushResult> WriteAsync(
                ReadOnlyMemory<byte> source,
                CancellationToken cancellationToken = default)
            {
                source.Span.CopyTo(_buffer.AsSpan(_written));
                _written += source.Length;
                return ValueTask.FromResult(new FlushResult(false, false));
            }

            public void Reset()
            {
                _written = 0;
            }
        }

        private sealed class PreallocatedBufferWriter : IBufferWriter<byte>
        {
            private readonly byte[] _buffer;
            private int _written;

            public PreallocatedBufferWriter(int capacity)
            {
                _buffer = new byte[capacity];
            }

            public void Advance(int count)
            {
                _written += count;
            }

            public Memory<byte> GetMemory(int sizeHint = 0)
            {
                return _buffer.AsMemory(_written);
            }

            public Span<byte> GetSpan(int sizeHint = 0)
            {
                return _buffer.AsSpan(_written);
            }

            public void Reset()
            {
                _written = 0;
            }
        }

        private sealed class SequenceSegment : ReadOnlySequenceSegment<byte>
        {
            public SequenceSegment(ReadOnlyMemory<byte> memory)
            {
                Memory = memory;
            }

            public void SetNext(SequenceSegment next)
            {
                next.RunningIndex = RunningIndex + Memory.Length;
                Next = next;
            }
        }
    }
}
