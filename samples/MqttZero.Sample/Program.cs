using System.Net;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using MqttZero.AspNetCore;
using MqttZero.AspNetCore.Dashboard;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);
int httpPort = builder.Configuration.GetValue("SampleEndpoints:HttpPort", 5209);
int mqttPort = builder.Configuration.GetValue("SampleEndpoints:MqttPort", 1883);

builder.WebHost.ConfigureKestrel(kestrel =>
{
    kestrel.ListenLocalhost(httpPort);
    kestrel.Listen(IPAddress.Any, mqttPort, listen => listen.UseMqttZero(options =>
    {
        options.AcceptedSockets.NoDelay = true;
        options.AcceptedSockets.KeepAlive = true;
    }));
});

builder.Services.AddMqttZero(options =>
{
    options.AcceptNewConnections = true;
});

builder.Services.AddMqttZeroDashboard(options =>
{
    options.ReadAuthorizationPolicy = "MqttZeroDashboardRead";
    options.OperateAuthorizationPolicy = "MqttZeroDashboardOperate";
});

builder.Services.AddAuthorizationBuilder()
    .AddPolicy("MqttZeroDashboardRead", policy =>
    {
        // Replace with the host application's real authentication policy.
        policy.RequireAssertion(static _ => true);
    })
    .AddPolicy("MqttZeroDashboardOperate", policy =>
    {
        // Replace with the host application's real operator policy.
        policy.RequireAssertion(static _ => true);
    });
builder.Services.AddHealthChecks()
    .AddMqttZeroHealthCheck(tags: ["ready"]);

WebApplication app = builder.Build();

app.UseWebSockets();
app.MapMqttZero("/mqtt");
app.MapMqttZeroDashboard("/mqttzero");
app.MapHealthChecks(
    "/health/live",
    new HealthCheckOptions
    {
        Predicate = static _ => false
    });
app.MapHealthChecks(
    "/health/ready",
    new HealthCheckOptions
    {
        Predicate = static registration => registration.Tags.Contains("ready")
    });
app.MapGet("/", static () => Results.Redirect("/mqttzero/"));

await app.RunAsync();
