using MqttZero.AspNetCore;
using Microsoft.AspNetCore.Server.Kestrel.Https;
using System.Net;
using System.Net.Sockets;
using System.Security.Authentication;

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

        [Fact]
        public void WebSocketSelectorOverridesDefault()
        {
            MqttZeroWebSocketOptions options = new()
            {
                AllowPacketFragmentationSelector = static transport =>
                    transport.IsSecure
            };

            Assert.True(options.ResolveAllowPacketFragmentation(
                new MqttZeroTransportInfo
                {
                    IsSecure = true,
                    IsWebSocket = true
                }));
            Assert.False(options.ResolveAllowPacketFragmentation(
                new MqttZeroTransportInfo
                {
                    IsWebSocket = true
                }));
        }

        [Fact]
        public void EndpointSelectorReceivesTransportDetails()
        {
            MqttZeroTransportInfo expected = new()
            {
                RemoteEndPoint = new IPEndPoint(IPAddress.Loopback, 1883),
                IsSecure = true
            };
            MqttZeroEndpointOptions options = new()
            {
                AllowPacketFragmentationSelector = transport =>
                    ReferenceEquals(transport, expected)
            };

            Assert.True(options.ResolveAllowPacketFragmentation(expected));
        }

        [Fact]
        public void TransportSelectorsRejectNullInput()
        {
            Assert.Throws<ArgumentNullException>(() =>
                new MqttZeroEndpointOptions()
                    .ResolveAllowPacketFragmentation(null!));
            Assert.Throws<ArgumentNullException>(() =>
                new MqttZeroWebSocketOptions()
                    .ResolveAllowPacketFragmentation(null!));
        }

        [Fact]
        public void AcceptedSocketOptionsApplyStandardValuesBeforeCallback()
        {
            using Socket socket = new(
                AddressFamily.InterNetwork,
                SocketType.Stream,
                ProtocolType.Tcp);
            bool callbackInvoked = false;
            MqttZeroAcceptedSocketOptions options = new()
            {
                NoDelay = true,
                KeepAlive = true,
                AcceptedReuseAddress = true,
                DualMode = true,
                Configure = (configuredSocket, endpoint) =>
                {
                    callbackInvoked = true;
                    Assert.Same(socket, configuredSocket);
                    Assert.Null(endpoint);
                    Assert.True(configuredSocket.NoDelay);
                }
            };

            options.Apply(socket, null);

            Assert.True(callbackInvoked);
            Assert.True(socket.NoDelay);
            Assert.NotEqual(
                0,
                Convert.ToInt32(
                    socket.GetSocketOption(
                        SocketOptionLevel.Socket,
                        SocketOptionName.ReuseAddress),
                    System.Globalization.CultureInfo.InvariantCulture));
        }

        [Fact]
        public void AcceptedSocketOptionsRejectNonPositiveKeepAliveDuration()
        {
            using Socket socket = new(
                AddressFamily.InterNetwork,
                SocketType.Stream,
                ProtocolType.Tcp);
            MqttZeroAcceptedSocketOptions options = new()
            {
                KeepAliveTime = TimeSpan.Zero
            };

            Assert.Throws<ArgumentOutOfRangeException>(
                () => options.Apply(socket, null));
        }

        [Fact]
        public void TlsOptionsExposeDocumentedDefaults()
        {
            MqttZeroTlsOptions options = new();

            Assert.Null(options.ServerCertificate);
            Assert.Equal(SslProtocols.None, options.SslProtocols);
            Assert.Equal(
                ClientCertificateMode.NoCertificate,
                options.ClientCertificateMode);
            Assert.True(options.CheckCertificateRevocation);
        }

        [Fact]
        public void TlsOptionsRequireCertificateOrSelector()
        {
            MqttZeroTlsOptions options = new();

            Assert.Throws<InvalidOperationException>(
                () => options.CreateKestrelOptions());
        }

        [Fact]
        public void TlsCallbackCanProvideCertificateSelector()
        {
            MqttZeroTlsOptions options = new()
            {
                Configure = configured =>
                    configured.ServerCertificateSelector =
                        static (_, _) => null
            };

            HttpsConnectionAdapterOptions kestrel =
                options.CreateKestrelOptions();

            Assert.NotNull(kestrel.ServerCertificateSelector);
        }
    }
}
