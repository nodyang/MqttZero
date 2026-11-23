# MQTTZero

MQTTZero 是一个面向 ASP.NET Core Kestrel 的独立 MQTT 5 Broker。生产代码不依赖 MQTTnet，目标框架为 .NET 10。当前版本为 `0.4.0-alpha.1`。

## 当前能力

- Kestrel 原始 TCP、TLS 和 MQTT over WebSocket。
- MQTT 5 CONNECT、PUBLISH、QoS 0/1、SUBSCRIBE、UNSUBSCRIBE、ACK、PING 和 DISCONNECT。
- 持久 Session、Session Expiry、Session Present、客户端 ID 接管和离线 QoS 1 队列。
- QoS 1 Inflight `DUP` 重发、KeepAlive 超时、Will/Will Delay 和 Message Expiry。
- UTF-8 Topic 通配符、Topic Alias、共享订阅轮询、Retained 回放和内存 PayloadOwner。
- DI 发布拦截器、Broker 生命周期、客户端快照和健康检查。
- 池化字节池、引用计数 Payload Lease、无分配 PUBLISH 解码/路由/QoS 1 队列/编码和内存 Transport 热路径。
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

## 0.4.0 命名空间迁移

`0.4.0-alpha.1` 将大型源码按职责拆分到 Server 项目子目录。`MqttZeroOptions`、Snapshot、PayloadOwner、ClientConnection、RetainedMessage，以及 `AddMqttZero`、`MapMqttZero`、`UseMqttZero` 等入口保持原 namespace。

| 类型 | 新 namespace |
|---|---|
| MQTT Packet、Codec、QoS、ReasonCode | `MqttZero.Protocol` |
| `MqttZeroBroker`、`MqttZeroBrokerState` | `MqttZero.Broker` |
| Session、Subscription、Will、QueuedMessage | `MqttZero.Sessions` |
| TopicMatcher、RouteScope、RouteSelector | `MqttZero.Routing` |
| BufferPool、MessageLease | `MqttZero.Memory` |
| Publish Interceptor API | `MqttZero.Interceptors` |

引用迁移后的类型时需要增加对应 `using`。本版本不提供旧 namespace 转发类型。

## 性能与分配

阶段二的硬门禁覆盖 Broker 稳态同步热路径：

- `MqttPublishPacketCodec` 直接解析 `ReadOnlySequence<byte>`，不创建完整报文或 Topic 数组。
- Topic/Properties/Payload 由引用计数 `MqttZeroMessageLease` 共享；在线订阅者、离线队列和 Inflight 消息持有同一份内存。
- 路由使用每 Broker 的池化 `MqttZeroRouteScope` 和不可变订阅快照，不为每条消息创建 `List`、`Dictionary` 或候选对象。
- QoS 1 描述符、PubAck 和出站 PUBLISH 编码复用池化对象，并直接写入连接自己的 `IBufferWriter`。
- `MqttZeroBufferPool` 以 `ArrayPool<byte>` 为底层来源执行字节预算；耗尽时暂停读取并等待释放，不静默回退到普通数组。

零分配口径仅包含预热后的单线程同步 Broker 核心热路径。Kestrel/TLS/WebSocket、连接建立、订阅变更、Retained 更新、管理 API、日志、异常、Observer 和异步拦截器续接允许分配。WebSocket 为维持单帧语义允许在边界复制一次 Payload。

运行分配哨兵：

```powershell
./perf/verify-allocation.ps1
```

运行完整基准：

```powershell
dotnet run -c Release --project benchmarks/MqttZero.Benchmarks -- `
  --filter '*' --job short --exporters csv json
```

已提交的短作业数据和解释位于 `perf/baseline`。当前实测中，fast PUBLISH 解码、Topic 路由、QoS 1 队列和内存端到端均达到 `0 B/op`。8 KiB 内存端到端约为 MQTTnet 同工作负载 codec+匹配基线的 `1.69x`。MQTTZero 解码显著快于 MQTTnet 且无分配，但编码和“全部订阅者均命中”的线性路由尚未达到计划中的全部相对吞吐门槛；这些缺口保留在基线文档中，不应被解释为已经达成。

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

当前不包含 QoS 2、Enhanced Authentication、MQTT 3.1.1、跨进程持久化、集群、桥接和客户端 SDK。Session、Will、QoS 1 队列在进程重启后丢失。Dashboard、管理 API、异步拦截器续接和网络框架边界不属于 `0 B/op` 承诺。完整 MQTT 5 属性语义与 Dashboard Live Events 仍在后续范围内。
