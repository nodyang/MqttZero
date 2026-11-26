using MqttZero.Memory;
using MqttZero.Protocol;
using System.Buffers;

namespace MqttZero.Tests.Memory
{
    public sealed class MessageLeaseTests
    {
        [Fact]
        public void BorrowedMultiSegmentPayloadIsOnlyVisibleThroughTheSequence()
        {
            ReadOnlySequence<byte> payload = CreateSequence(
                new byte[4],
                new byte[4]);
            MqttZeroMessageLease lease = MqttZeroMessageLease.BorrowFrom(
                new ReadOnlySequence<byte>("lease/topic"u8.ToArray()),
                ReadOnlySequence<byte>.Empty,
                payload,
                MqttQualityOfServiceLevel.AtMostOnce,
                retain: false,
                messageExpiryInterval: null,
                subscriptionIdentifier: null);
            try
            {
                Assert.True(lease.IsBorrowed);
                Assert.Equal(8, lease.PayloadSequence.Length);
                Assert.True(lease.Payload.IsEmpty);

                lease.EnsureOwned();

                Assert.False(lease.IsBorrowed);
                Assert.Equal(8, lease.Payload.Length);
                Assert.Equal(8, lease.PayloadSequence.Length);
            }
            finally
            {
                lease.Release();
            }
        }

        private static ReadOnlySequence<byte> CreateSequence(
            byte[] first,
            byte[] second)
        {
            SequenceSegment firstSegment = new(first, 0);
            SequenceSegment secondSegment = new(second, first.Length);
            firstSegment.SetNext(secondSegment);
            return new ReadOnlySequence<byte>(
                firstSegment,
                0,
                secondSegment,
                second.Length);
        }

        private sealed class SequenceSegment : ReadOnlySequenceSegment<byte>
        {
            public SequenceSegment(byte[] memory, long runningIndex)
            {
                Memory = memory;
                RunningIndex = runningIndex;
            }

            public void SetNext(SequenceSegment next)
            {
                Next = next;
            }
        }
    }
}
