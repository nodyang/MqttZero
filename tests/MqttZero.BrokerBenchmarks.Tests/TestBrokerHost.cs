using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MqttZero.AspNetCore;
using MqttZero.BrokerBenchmarks;
using MqttZero.Server;
using MqttZero.Server.Broker;
using System.Net;
using System.Net.Sockets;

namespace MqttZero.BrokerBenchmarks.Tests;

internal sealed class TestBrokerHost : IAsyncDisposable
{
    private readonly WebApplication _application;

    private TestBrokerHost(
        WebApplication application,
        int port,
        MqttZeroBroker broker)
    {
        _application = application;
        Port = port;
        Broker = broker;
    }

    public int Port { get; }

    public MqttZeroBroker Broker { get; }

    public static async Task<TestBrokerHost> StartAsync(
        Action<MqttZeroOptions>? configure = null)
    {
        int port = GetFreePort();
        WebApplicationBuilder builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(kestrel =>
        {
            kestrel.Listen(
                IPAddress.Loopback,
                port,
                listen => listen.UseMqttZero());
        });
        builder.Services.AddMqttZero(configure);
        builder.Services.Configure<HostOptions>(
            options => options.ShutdownTimeout =
                TimeSpan.FromSeconds(3));
        WebApplication application = builder.Build();
        await application.StartAsync(TestContext.Current.CancellationToken);
        return new TestBrokerHost(
            application,
            port,
            application.Services.GetRequiredService<MqttZeroBroker>());
    }

    public RawMqttClient CreateRawClient()
    {
        return new RawMqttClient(new RawMqttClientOptions
        {
            Host = "127.0.0.1",
            Port = Port
        });
    }

    public static async Task WaitUntilAsync(
        Func<bool> predicate,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        using CancellationTokenSource timeoutSource =
            CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutSource.CancelAfter(timeout);
        while (!predicate())
        {
            await Task.Delay(
                TimeSpan.FromMilliseconds(10),
                timeoutSource.Token);
        }
    }

    public async Task StopAsync()
    {
        await _application.StopAsync(CancellationToken.None);
    }

    public async ValueTask DisposeAsync()
    {
        await _application.StopAsync(CancellationToken.None);
        await _application.DisposeAsync();
    }

    private static int GetFreePort()
    {
        using Socket socket = new(
            AddressFamily.InterNetwork,
            SocketType.Stream,
            ProtocolType.Tcp);
        socket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        return ((IPEndPoint)socket.LocalEndPoint!).Port;
    }
}
