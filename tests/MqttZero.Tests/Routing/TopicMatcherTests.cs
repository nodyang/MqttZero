using MqttZero.Broker;
using MqttZero.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Routing;
using MqttZero.Sessions;
using System.Text;

namespace MqttZero.Tests.Routing
{
    public sealed class TopicMatcherTests
    {
        [Theory]
        [InlineData("devices/+/temperature", "devices/a/temperature", true)]
        [InlineData("devices/#", "devices/a/temperature", true)]
        [InlineData("devices/+/temperature", "devices/a/humidity", false)]
        [InlineData("devices/#", "devices", true)]
        [InlineData("+", "a", true)]
        [InlineData("#", "a/b", true)]
        public void MatchesMqttWildcardFilters(string filter, string topic, bool expected)
        {
            bool actual = MqttZeroTopicMatcher.Matches(
                Encoding.UTF8.GetBytes(filter),
                Encoding.UTF8.GetBytes(topic));

            Assert.Equal(expected, actual);
        }
    }
}
