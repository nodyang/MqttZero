using MqttZero.BrokerBenchmarks;

namespace MqttZero.BrokerBenchmarks.Tests;

public sealed class BenchmarkDeliveryEnvelopeTests
{
    [Fact]
    public void EnvelopeRoundTripsSequenceAndTimestamp()
    {
        byte[] payload = new byte[32];

        BenchmarkDeliveryEnvelope.Write(
            payload,
            sequence: 42,
            publishTimestamp: 123_456_789);

        Assert.True(BenchmarkDeliveryEnvelope.TryRead(
            payload,
            out long sequence,
            out long publishTimestamp));
        Assert.Equal(42, sequence);
        Assert.Equal(123_456_789, publishTimestamp);
    }

    [Fact]
    public void WriteRejectsPayloadsShorterThanTheEnvelope()
    {
        Assert.Throws<ArgumentException>(() =>
            BenchmarkDeliveryEnvelope.Write(
                new byte[BenchmarkDeliveryEnvelope.HeaderLength - 1],
                sequence: 1,
                publishTimestamp: 1));
    }

    [Fact]
    public void TryReadRejectsPayloadsShorterThanTheEnvelope()
    {
        Assert.False(BenchmarkDeliveryEnvelope.TryRead(
            new byte[BenchmarkDeliveryEnvelope.HeaderLength - 1],
            out long sequence,
            out long publishTimestamp));
        Assert.Equal(0, sequence);
        Assert.Equal(0, publishTimestamp);
    }
}
