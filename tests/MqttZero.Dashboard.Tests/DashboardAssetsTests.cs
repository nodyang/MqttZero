using MqttZero.AspNetCore.Dashboard;

namespace MqttZero.Dashboard.Tests
{
    public sealed class DashboardAssetsTests
    {
        [Fact]
        public void ProductionVueAssetsAreEmbedded()
        {
            string[] resources = typeof(MqttZeroDashboardOptions).Assembly
                .GetManifestResourceNames()
                .Select(static name => name.Replace('\\', '/'))
                .ToArray();

            Assert.Contains("MqttZeroDashboard/index.html", resources);
            Assert.Contains(resources, static name =>
                name.StartsWith("MqttZeroDashboard/assets/", StringComparison.Ordinal) &&
                name.EndsWith(".js", StringComparison.Ordinal));
            Assert.Contains(resources, static name =>
                name.StartsWith("MqttZeroDashboard/assets/", StringComparison.Ordinal) &&
                name.EndsWith(".css", StringComparison.Ordinal));
        }
    }
}
