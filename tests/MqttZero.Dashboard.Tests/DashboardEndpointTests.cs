using System.Net;
using System.Net.Http.Json;
using System.Net.Sockets;
using System.Text.Json;
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

            using HttpResponseMessage tokenResponse = await client.GetAsync(
                $"http://127.0.0.1:{port}/mqttzero/api/v1/antiforgery",
                cancellationToken);
            using JsonDocument tokenDocument = JsonDocument.Parse(
                await tokenResponse.Content.ReadAsStringAsync(cancellationToken));
            string requestToken = tokenDocument.RootElement.GetProperty("token").GetString()!;
            using HttpRequestMessage publishRequest = new(
                HttpMethod.Post,
                $"http://127.0.0.1:{port}/mqttzero/api/v1/publish")
            {
                Content = JsonContent.Create(new
                {
                    topic = "dashboard/test",
                    payload = "hello",
                    qualityOfServiceLevel = 0,
                    retain = false
                })
            };
            publishRequest.Headers.Add("RequestVerificationToken", requestToken);
            using HttpResponseMessage publishResponse = await client.SendAsync(
                publishRequest,
                cancellationToken);
            using HttpResponseMessage sessionsResponse = await client.GetAsync(
                $"http://127.0.0.1:{port}/mqttzero/api/v1/sessions",
                cancellationToken);
            string sessions = await sessionsResponse.Content.ReadAsStringAsync(cancellationToken);

            Assert.Equal(HttpStatusCode.OK, overviewResponse.StatusCode);
            Assert.Contains("\"connectedClients\":0", overview, StringComparison.Ordinal);
            Assert.Equal(HttpStatusCode.OK, indexResponse.StatusCode);
            Assert.Contains("<div id=\"app\"></div>", index, StringComparison.Ordinal);
            Assert.Equal(HttpStatusCode.NoContent, publishResponse.StatusCode);
            Assert.Equal(HttpStatusCode.OK, sessionsResponse.StatusCode);
            Assert.Equal("[]", sessions);

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
