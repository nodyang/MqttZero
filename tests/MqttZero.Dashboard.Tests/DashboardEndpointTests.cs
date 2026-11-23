using System.Net;
using System.Net.Sockets;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using MqttZero.AspNetCore;
using MqttZero.AspNetCore.Dashboard;

namespace MqttZero.Dashboard.Tests
{
    public sealed class DashboardEndpointTests
    {
        [Fact(Timeout = 15_000)]
        public async Task ServesOverviewApiAndEmbeddedVueIndex()
        {
            int port = GetFreePort();
            WebApplicationBuilder builder = WebApplication.CreateBuilder();
            builder.Logging.ClearProviders();
            builder.WebHost.UseUrls($"http://127.0.0.1:{port}");
            builder.Services.AddMqttZero();
            builder.Services.AddMqttZeroDashboard(options =>
            {
                options.ReadAuthorizationPolicy = "Read";
                options.OperateAuthorizationPolicy = "Operate";
            });
            builder.Services.AddAuthorizationBuilder()
                .AddPolicy("Read", static policy => policy.RequireAssertion(static _ => true))
                .AddPolicy("Operate", static policy => policy.RequireAssertion(static _ => true));

            await using WebApplication app = builder.Build();
            app.MapMqttZeroDashboard("/mqttzero");

            CancellationToken cancellationToken = TestContext.Current.CancellationToken;
            await app.StartAsync(cancellationToken);

            using HttpClient client = new();
            using HttpResponseMessage overviewResponse = await client.GetAsync(
                $"http://127.0.0.1:{port}/mqttzero/api/v1/overview",
                cancellationToken);
            string overview = await overviewResponse.Content.ReadAsStringAsync(cancellationToken);

            using HttpResponseMessage indexResponse = await client.GetAsync(
                $"http://127.0.0.1:{port}/mqttzero/",
                cancellationToken);
            string index = await indexResponse.Content.ReadAsStringAsync(cancellationToken);

            Assert.Equal(HttpStatusCode.OK, overviewResponse.StatusCode);
            Assert.Contains("\"connectedClients\":0", overview, StringComparison.Ordinal);
            Assert.Equal(HttpStatusCode.OK, indexResponse.StatusCode);
            Assert.Contains("<div id=\"app\"></div>", index, StringComparison.Ordinal);

            await app.StopAsync(cancellationToken);
        }

        private static int GetFreePort()
        {
            TcpListener listener = new(IPAddress.Loopback, 0);
            listener.Start();
            int port = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();
            return port;
        }
    }
}
