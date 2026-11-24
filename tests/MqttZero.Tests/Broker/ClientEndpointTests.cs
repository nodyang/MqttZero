using MqttZero.Server;
using MqttZero.Tests.Support;
using System.Net;
using System.Net.Sockets;

namespace MqttZero.Tests.Broker
{
    public sealed class ClientEndpointTests
    {
        [Fact]
        public void ClientSnapshotPreservesTypedRemoteEndPoint()
        {
            DnsEndPoint remoteEndPoint = new("broker.example", 1883);

            MqttZeroClientSnapshot snapshot = new()
            {
                ClientId = "typed-client",
                RemoteEndPoint = remoteEndPoint
            };

            Assert.Same(remoteEndPoint, snapshot.RemoteEndPoint);
        }

        [Fact]
        public void ClientSnapshotPreservesUnixDomainSocketEndPoint()
        {
            UnixDomainSocketEndPoint remoteEndPoint = new("mqttzero.sock");

            MqttZeroClientSnapshot snapshot = new()
            {
                ClientId = "unix-client",
                RemoteEndPoint = remoteEndPoint
            };

            Assert.Same(remoteEndPoint, snapshot.RemoteEndPoint);
        }

        [Fact]
        public async Task ClientConnectionPreservesTypedRemoteEndPoint()
        {
            IPEndPoint remoteEndPoint = new(IPAddress.Loopback, 1883);
            await using MqttZeroClientConnection connection = new(
                new AllocationPipeWriter(1024),
                remoteEndPoint,
                false,
                false,
                0,
                32,
                1024 * 1024,
                TimeProvider.System);

            Assert.Same(remoteEndPoint, connection.RemoteEndPoint);
        }
    }
}
