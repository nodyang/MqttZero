using MqttZero.Protocol;
using MqttZero.Tests.Support;
using System.Buffers;
using System.Text;

namespace MqttZero.Tests.Protocol;

public sealed class MqttPublishPropertiesCodecTests
{
    [Fact]
    public void RoundTripsCompletePublishProperties()
    {
        MqttPublishPacket packet = new()
        {
            TopicName = new ReadOnlySequence<byte>("properties/topic"u8.ToArray()),
            Payload = new ReadOnlySequence<byte>("payload"u8.ToArray()),
            Properties = new MqttPublishProperties
            {
                PayloadFormatIndicator = MqttPayloadFormatIndicator.Utf8,
                MessageExpiryInterval = 30,
                ContentType = "application/json",
                ResponseTopic = "response/topic",
                CorrelationData = "correlation"u8.ToArray(),
                UserProperties =
                [
                    new MqttUserProperty("trace", "first"),
                    new MqttUserProperty("trace", "second")
                ]
            },
            SubscriptionIdentifiers = [7, 9],
            TopicAlias = 3
        };

        byte[] encoded = ProtocolPacketTestHelper.Encode(packet);
        bool decoded = MqttPacketCodec.TryDecode(
            new ReadOnlySequence<byte>(encoded),
            out MqttPacket? result,
            out long consumed);

        Assert.True(decoded, Convert.ToHexString(encoded));
        MqttPublishPacket value = Assert.IsType<MqttPublishPacket>(result);
        Assert.Equal(encoded.Length, consumed);
        Assert.Equal(MqttPayloadFormatIndicator.Utf8, value.Properties.PayloadFormatIndicator);
        Assert.Equal(30U, value.Properties.MessageExpiryInterval);
        Assert.Equal("application/json", value.Properties.ContentType);
        Assert.Equal("response/topic", value.Properties.ResponseTopic);
        Assert.Equal("correlation", Encoding.UTF8.GetString(value.Properties.CorrelationData!.Value.Span));
        Assert.Collection(
            value.Properties.UserProperties,
            property =>
            {
                Assert.Equal("trace", property.Name);
                Assert.Equal("first", property.Value);
            },
            property =>
            {
                Assert.Equal("trace", property.Name);
                Assert.Equal("second", property.Value);
            });
        Assert.Equal(new uint[] { 7, 9 }, value.SubscriptionIdentifiers);
        Assert.Equal((ushort)3, value.TopicAlias);
    }
}
