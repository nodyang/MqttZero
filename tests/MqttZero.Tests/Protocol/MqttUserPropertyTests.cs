using MqttZero.Protocol;

namespace MqttZero.Tests.Protocol;

public sealed class MqttUserPropertyTests
{
    [Theory]
    [InlineData("invalid\0name", "value")]
    [InlineData("name", "invalid\0value")]
    public void RejectsInvalidMqttUtf8(string name, string value)
    {
        Assert.Throws<ArgumentException>(() => new MqttUserProperty(name, value));
    }

    [Fact]
    public void RejectsUnpairedSurrogates()
    {
        Assert.Throws<ArgumentException>(
            () => new MqttUserProperty(
                new string((char)0xD800, 1),
                "value"));
        Assert.Throws<ArgumentException>(
            () => new MqttUserProperty(
                "name",
                new string((char)0xDC00, 1)));
    }

    [Fact]
    public void EmptyPropertiesUseSharedDefaults()
    {
        MqttPublishProperties properties = MqttPublishProperties.Empty;

        Assert.Null(properties.PayloadFormatIndicator);
        Assert.Null(properties.MessageExpiryInterval);
        Assert.Null(properties.ContentType);
        Assert.Null(properties.ResponseTopic);
        Assert.Null(properties.CorrelationData);
        Assert.Empty(properties.UserProperties);
    }
}
