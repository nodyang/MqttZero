using MqttZero.AspNetCore;

namespace MqttZero.Tests.AspNetCore
{
    public sealed class MqttZeroTransportOptionsTests
    {
        [Fact]
        public void WebSocketFragmentationDefaultsToDisabled()
        {
            MqttZeroWebSocketOptions options = new();
            MqttZeroTransportInfo transport = new()
            {
                IsWebSocket = true
            };

            Assert.False(options.ResolveAllowPacketFragmentation(transport));
        }

        [Fact]
        public void TcpEndpointFragmentationDefaultsToEnabled()
        {
            MqttZeroEndpointOptions options = new();
            MqttZeroTransportInfo transport = new();

            Assert.True(options.ResolveAllowPacketFragmentation(transport));
        }
    }
}
