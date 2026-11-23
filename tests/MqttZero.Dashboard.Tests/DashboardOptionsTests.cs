using MqttZero.AspNetCore.Dashboard;

namespace MqttZero.Dashboard.Tests
{
    public sealed class DashboardOptionsTests
    {
        [Fact]
        public void DefaultsMatchDashboardCapacity()
        {
            MqttZeroDashboardOptions options = new();

            Assert.Equal(10_000, options.EventBufferCapacity);
            Assert.Equal(TimeSpan.FromMilliseconds(250), options.EventBatchInterval);
            Assert.Equal(64 * 1024, options.PayloadPreviewLimit);
            Assert.Equal(50, options.DefaultPageSize);
            Assert.Equal(200, options.MaxPageSize);
        }
    }
}
