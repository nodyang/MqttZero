using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Connections.Features;
using Microsoft.AspNetCore.Http.Connections;
using Microsoft.Extensions.Hosting;

namespace MqttZero.AspNetCore;

/// <summary>
/// Bridges Kestrel connections into the MQTTZero broker.
/// </summary>
public sealed class MqttZeroConnectionHandler : ConnectionHandler
{
    private readonly IHostApplicationLifetime _applicationLifetime;
    private readonly MqttZeroBroker _broker;

    /// <summary>
    /// Initializes a new connection handler.
    /// </summary>
    /// <param name="broker">The MQTTZero broker.</param>
    /// <param name="applicationLifetime">The host application lifetime.</param>
    public MqttZeroConnectionHandler(
        MqttZeroBroker broker,
        IHostApplicationLifetime applicationLifetime)
    {
        _broker = broker;
        _applicationLifetime = applicationLifetime;
    }

    /// <inheritdoc />
    public override async Task OnConnectedAsync(ConnectionContext connection)
    {
        using CancellationTokenSource cancellation = CancellationTokenSource.CreateLinkedTokenSource(
            connection.ConnectionClosed,
            _applicationLifetime.ApplicationStopping);

        ITransferFormatFeature? transferFormatFeature = connection.Features.Get<ITransferFormatFeature>();
        if (transferFormatFeature != null)
        {
            transferFormatFeature.ActiveFormat = TransferFormat.Binary;
        }

        Microsoft.AspNetCore.Http.HttpContext? httpContext = connection.GetHttpContext();
        bool isWebSocket = httpContext?.WebSockets.IsWebSocketRequest == true;
        bool isSecure = httpContext?.Request.IsHttps == true;

        await _broker.AcceptConnectionAsync(
            connection.Transport.Input,
            connection.Transport.Output,
            connection.RemoteEndPoint?.ToString(),
            isSecure,
            isWebSocket,
            cancellation.Token).ConfigureAwait(false);
    }
}
