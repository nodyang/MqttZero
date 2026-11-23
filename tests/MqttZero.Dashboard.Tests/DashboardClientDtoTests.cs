using MqttZero.AspNetCore.Dashboard;
using System.Text.Json;

namespace MqttZero.Dashboard.Tests
{
    public sealed class DashboardClientDtoTests
    {
        [Fact]
        public void RemoteEndPointRemainsAStringInDashboardJson()
        {
            DashboardClientDto[] clients =
            [
                new(
                    "client-1",
                    "127.0.0.1:1883",
                    false,
                    false,
                    2)
            ];

            string json = JsonSerializer.Serialize(
                clients,
                DashboardJsonSerializerContext.Default.DashboardClientDtoArray);
            using JsonDocument document = JsonDocument.Parse(json);
            JsonElement remoteEndPoint = document.RootElement[0]
                .GetProperty("remoteEndPoint");

            Assert.Equal(JsonValueKind.String, remoteEndPoint.ValueKind);
            Assert.Equal("127.0.0.1:1883", remoteEndPoint.GetString());
        }
    }
}
