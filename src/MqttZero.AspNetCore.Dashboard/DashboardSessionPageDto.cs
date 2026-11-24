namespace MqttZero.AspNetCore.Dashboard;

/// <summary>
/// Represents one page of dashboard sessions.
/// </summary>
/// <param name="Items">The session items.</param>
/// <param name="Page">The one-based page number.</param>
/// <param name="PageSize">The page size.</param>
/// <param name="Total">The total session count.</param>
public sealed record DashboardSessionPageDto(
    IReadOnlyList<DashboardSessionDto> Items,
    int Page,
    int PageSize,
    int Total);
