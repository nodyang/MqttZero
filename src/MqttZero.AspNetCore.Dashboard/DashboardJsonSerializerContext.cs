using MqttZero.Broker;
using MqttZero.Protocol;
using System.Text.Json.Serialization;

namespace MqttZero.AspNetCore.Dashboard;

/// <summary>
/// Source-generated JSON metadata for dashboard API DTOs.
/// </summary>
[JsonSerializable(typeof(DashboardOverviewDto))]
[JsonSerializable(typeof(DashboardClientDto[]))]
[JsonSerializable(typeof(DashboardSessionDto[]))]
[JsonSerializable(typeof(DashboardPublishRequest))]
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
public partial class DashboardJsonSerializerContext : JsonSerializerContext
{
}
