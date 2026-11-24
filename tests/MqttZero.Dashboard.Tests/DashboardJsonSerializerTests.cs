using MqttZero.AspNetCore.Dashboard;
using MqttZero.Protocol;
using System.Text.Json;

namespace MqttZero.Dashboard.Tests
{
    public sealed class DashboardJsonSerializerTests
    {
        [Fact]
        public void SourceGeneratedContextSerializesAllDashboardDtos()
        {
            DashboardOverviewDto overview = new("Running", 2, 1);
            DashboardClientDto client = new("client", null, false, false, 1);
            DashboardClientPageDto clientPage = new([client], 1, 10, 1);
            DashboardSessionDto session = new(
                "client",
                true,
                false,
                true,
                60,
                null,
                1,
                0,
                0,
                false,
                null);
            DashboardSessionPageDto sessionPage = new([session], 1, 10, 1);
            DashboardPublishRequest publish = new()
            {
                Topic = "dashboard/test",
                Payload = "value",
                QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce,
                Retain = true
            };

            Assert.Contains(
                "\"state\":\"Running\"",
                JsonSerializer.Serialize(
                    overview,
                    DashboardJsonSerializerContext.Default.DashboardOverviewDto));
            Assert.Contains(
                "\"items\":[",
                JsonSerializer.Serialize(
                    clientPage,
                    DashboardJsonSerializerContext.Default.DashboardClientPageDto));
            Assert.Contains(
                "\"items\":[",
                JsonSerializer.Serialize(
                    sessionPage,
                    DashboardJsonSerializerContext.Default.DashboardSessionPageDto));
            Assert.Contains(
                "\"topic\":\"dashboard/test\"",
                JsonSerializer.Serialize(
                    publish,
                    DashboardJsonSerializerContext.Default.DashboardPublishRequest));
        }
    }
}
