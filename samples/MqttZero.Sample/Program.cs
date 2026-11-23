using System.Net;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using MqttZero.AspNetCore;
using MqttZero.AspNetCore.Dashboard;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

builder.WebHost.ConfigureKestrel(kestrel =>
{
    kestrel.Listen(IPAddress.Any, 1883, listen => listen.UseMqttZero());
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

WebApplication app = builder.Build();

app.UseWebSockets();
app.MapMqttZero("/mqtt");
app.MapMqttZeroDashboard("/mqttzero");
app.MapGet("/", static () => Results.Redirect("/mqttzero/"));

await app.RunAsync();
