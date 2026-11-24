using MqttZero.AspNetCore.Dashboard;

namespace MqttZero.Dashboard.Tests
{
    public sealed class DashboardOptionsTests
    {
        [Fact]
        public void DefaultsMatchDashboardCapacity()
        {
            MqttZeroDashboardOptions options = new();

            Assert.Equal(50, options.DefaultPageSize);
            Assert.Equal(200, options.MaxPageSize);
        }

        [Fact]
        public void UnimplementedDashboardOptionsAreRemoved()
        {
            Assert.Null(typeof(MqttZeroDashboardOptions).GetProperty(
                "EventBufferCapacity"));
            Assert.Null(typeof(MqttZeroDashboardOptions).GetProperty(
                "EventBatchInterval"));
            Assert.Null(typeof(MqttZeroDashboardOptions).GetProperty(
                "PayloadPreviewLimit"));
        }
    }
}
