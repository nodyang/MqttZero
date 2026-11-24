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

        [Fact]
        public void EmptyPathResolvesToIndexHtml()
        {
            Assert.True(MqttZeroDashboardAssets.TryOpen(
                string.Empty,
                out Stream? stream,
                out string contentType));
            Assert.NotNull(stream);
            Assert.Equal("text/html; charset=utf-8", contentType);
            stream.Dispose();
        }

        [Fact]
        public void RootSlashResolvesToIndexHtml()
        {
            Assert.True(MqttZeroDashboardAssets.TryOpen(
                "/",
                out Stream? stream,
                out string contentType));
            Assert.NotNull(stream);
            Assert.Equal("text/html; charset=utf-8", contentType);
            stream.Dispose();
        }

        [Fact]
        public void LeadingSlashIsRemovedForExistingAssets()
        {
            string assetName = typeof(MqttZeroDashboardOptions).Assembly
                .GetManifestResourceNames()
                .Select(static name => name.Replace('\\', '/'))
                .Single(static name =>
                    name.StartsWith("MqttZeroDashboard/assets/", StringComparison.Ordinal) &&
                    name.EndsWith(".js", StringComparison.Ordinal));
            string path = "/" + assetName["MqttZeroDashboard/".Length..];

            Assert.True(MqttZeroDashboardAssets.TryOpen(
                path,
                out Stream? stream,
                out string contentType));
            Assert.NotNull(stream);
            Assert.Equal("text/javascript; charset=utf-8", contentType);
            stream.Dispose();
        }

        [Theory]
        [InlineData("file.html", "text/html; charset=utf-8")]
        [InlineData("file.css", "text/css; charset=utf-8")]
        [InlineData("file.js", "text/javascript; charset=utf-8")]
        [InlineData("file.json", "application/json; charset=utf-8")]
        [InlineData("file.svg", "image/svg+xml")]
        [InlineData("file.png", "image/png")]
        [InlineData("file.ico", "image/x-icon")]
        [InlineData("file.bin", "application/octet-stream")]
        public void AssetPathsResolveDocumentedContentTypes(
            string path,
            string expectedContentType)
        {
            _ = MqttZeroDashboardAssets.TryOpen(
                path,
                out Stream? stream,
                out string contentType);
            stream?.Dispose();

            Assert.Equal(expectedContentType, contentType);
        }
    }
}
