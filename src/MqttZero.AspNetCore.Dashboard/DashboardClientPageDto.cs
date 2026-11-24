namespace MqttZero.AspNetCore.Dashboard;

/// <summary>
/// Represents one page of connected dashboard clients.
/// </summary>
/// <param name="Items">The client items.</param>
/// <param name="Page">The one-based page number.</param>
/// <param name="PageSize">The page size.</param>
/// <param name="Total">The total client count.</param>
public sealed record DashboardClientPageDto(
    IReadOnlyList<DashboardClientDto> Items,
    int Page,
    int PageSize,
    int Total);
