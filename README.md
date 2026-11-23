# MQTTZero

MQTTZero 是一个面向 ASP.NET Core Kestrel 的独立 MQTT 5 Broker。生产代码不依赖 MQTTnet，目标框架为 .NET 10。

## 当前能力

- Kestrel 原始 TCP、TLS 和 MQTT over WebSocket。
- MQTT 5 CONNECT、PUBLISH、QoS 0/1、SUBSCRIBE、UNSUBSCRIBE、ACK、PING 和 DISCONNECT。
- 持久 Session、Session Expiry、Session Present、客户端 ID 接管和离线 QoS 1 队列。
- QoS 1 Inflight `DUP` 重发、KeepAlive 超时、Will/Will Delay 和 Message Expiry。
- UTF-8 Topic 通配符、Topic Alias、共享订阅轮询、Retained 回放和内存 PayloadOwner。
- DI 发布拦截器、Broker 生命周期、客户端快照和健康检查。
- 可选 Vue 3 + Element Plus 管理面板，构建资源内嵌在 NuGet 程序集中。

## 快速开始

```csharp
using System.Net;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using MqttZero.AspNetCore;
using MqttZero.AspNetCore.Dashboard;

WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

builder.WebHost.ConfigureKestrel(kestrel =>
{
    kestrel.Listen(IPAddress.Any, 1883, listen => listen.UseMqttZero());
});

builder.Services.AddMqttZero();
builder.Services.AddMqttZeroDashboard(options =>
{
    options.ReadAuthorizationPolicy = "MqttZeroDashboardRead";
    options.OperateAuthorizationPolicy = "MqttZeroDashboardOperate";
});
builder.Services.AddAuthorizationBuilder()
    .AddPolicy("MqttZeroDashboardRead", policy => policy.RequireAuthenticatedUser())
    .AddPolicy("MqttZeroDashboardOperate", policy => policy.RequireRole("MqttAdmin"));

WebApplication app = builder.Build();
app.UseWebSockets();
app.MapMqttZero("/mqtt");
app.MapMqttZeroDashboard("/mqttzero");
await app.RunAsync();
```

完整示例位于 `samples/MqttZero.Sample`。

## 性能与分配

`MqttZeroPayloadOwnerFactory` 从池中提供单段或多段 Payload。入站 PUBLISH 的 Topic、Properties 和 Payload 通过 `ReadOnlySequence<byte>` 切片解析，避免自动复制成中间数组。

运行基准：

```powershell
dotnet run -c Release --project benchmarks/MqttZero.Benchmarks
```

当前版本优先建立端到端协议正确性基线。20 万消息/秒和严格零分配回归门禁仍需在目标硬件上补齐，不应把开发机单次结果当作容量承诺。

## 构建与测试

```powershell
dotnet build MqttZero.slnx -c Release
dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release
dotnet test tests/MqttZero.Dashboard.Tests/MqttZero.Dashboard.Tests.csproj -c Release
```

Vue 源码构建：

```powershell
Set-Location dashboard/MqttZero.Dashboard
npm ci
npm run build
```

构建后的 Vue 资源会写入 `src/MqttZero.AspNetCore.Dashboard/wwwroot` 并嵌入 Dashboard 程序集；运行时消费者不需要 Node。

## 安全

- Dashboard 默认不上线，必须显式 `MapMqttZeroDashboard` 并配置读写授权策略。
- 写操作要求 Antiforgery Token。
- 日志不得记录密码、认证数据或完整 Payload。
- 示例中的放行策略只用于本地体验，生产部署必须替换为真实身份与角色策略。

## 限制

当前不包含 QoS 2、Enhanced Authentication、MQTT 3.1.1、跨进程持久化、集群、桥接和客户端 SDK。MQTT 5 会话、Will、KeepAlive、Topic Alias、共享订阅和 QoS 1 恢复已实现；完整 MQTT 5 属性语义、Dashboard Live Events 和严格性能门禁仍在后续范围内。
