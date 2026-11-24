# MQTTZero

MQTTZero 是一个面向 ASP.NET Core Kestrel 的独立 MQTT 5 Broker。生产代码不依赖 MQTTnet，目标框架为 .NET 10。当前版本为 `0.5.0-alpha.1`。

## 当前能力

- Kestrel 原始 TCP、TLS 和 MQTT over WebSocket。
- MQTT 5 CONNECT、PUBLISH、QoS 0/1、SUBSCRIBE、UNSUBSCRIBE、ACK、PING 和 DISCONNECT。
- 持久 Session、Session Expiry、Session Present、客户端 ID 接管和离线 QoS 1 队列。
- QoS 1 Inflight `DUP` 重发、KeepAlive 超时、Will/Will Delay 和 Message Expiry。
- UTF-8 Topic 通配符、Topic Alias、共享订阅轮询、Retained 回放和内存 PayloadOwner。
- DI 发布拦截器、Broker 生命周期、客户端快照和健康检查。
- 统一 `MqttZeroPublishMessage` 发布入口，以及 `ReadOnlySequence<byte>`、PipeWriter 工厂和 Stream 便捷扩展。
- Kestrel Accepted Socket、静态 TLS、分片策略和 Unix Domain Socket 原生承载配置。
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
    kestrel.Listen(IPAddress.Any, 1883, listen => listen.UseMqttZero(options =>
    {
        options.AcceptedSockets.NoDelay = true;
        options.AcceptedSockets.KeepAlive = true;
    }));
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

## 发布 API

`MqttZeroBroker.PublishAsync` 接收单个消息实体，后续消息字段扩展不会改变核心方法签名：

```csharp
using MqttZero.Broker;
using MqttZero.Protocol;
using MqttZero.Publishing;
using System.Buffers;

MqttZeroBroker broker = app.Services.GetRequiredService<MqttZeroBroker>();
await broker.PublishAsync(
    new MqttZeroPublishMessage
    {
        Topic = "devices/a/state",
        Payload = new ReadOnlySequence<byte>("online"u8.ToArray()),
        QualityOfServiceLevel = MqttQualityOfServiceLevel.AtLeastOnce,
        Retain = true
    });
```

`ReadOnlySequence<byte>`、PipeWriter 工厂和 Stream 继续通过 `PublishAsync` 扩展调用：

```csharp
await broker.PublishAsync(
    "devices/a/state",
    new ReadOnlySequence<byte>("online"u8.ToArray()),
    MqttQualityOfServiceLevel.AtMostOnce,
    retain: false);

await broker.PublishAsync(
    "devices/a/state",
    payloadStream,
    MqttQualityOfServiceLevel.AtLeastOnce,
    retain: false);
```

Stream 由调用方拥有；扩展会在发布前将内容收集到池化多段 Payload，并执行最大包大小限制。

## Kestrel 入口配置

`UseMqttZero` 支持连接级 TCP 选项和静态 TLS。Listening Socket 的 `ReuseAddress` 等选项仍由 Kestrel `SocketTransportOptions` 管理。

```csharp
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

kestrel.Listen(IPAddress.Any, 8883, listen => listen.UseMqttZero(options =>
{
    options.AcceptedSockets.KeepAlive = true;
    options.Tls = new MqttZeroTlsOptions
    {
        ServerCertificate = certificate,
        SslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13
    };
}));
```

证书在监听器启动时使用，不提供热替换。WebSocket 端点可按连接选择是否允许 MQTT PUBLISH 分片：

```csharp
app.MapMqttZero("/mqtt", options =>
{
    options.AllowPacketFragmentationSelector = transport =>
        !transport.IsSecure;
});
```

WebSocket 未设置选择器时默认不允许分片，从而保持 MQTT PUBLISH 单帧语义；TCP/TLS 端点未设置选择器时仍允许分片。

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

## 0.5.0 属性强类型迁移

`0.5.0-alpha.1` 删除了以下原始类型 API，不提供字符串或字节兼容重载：

| 属性 | 0.4 API | 0.5 API |
|---|---|---|
| 客户端远端地址 | `string? RemoteEndPoint` | `EndPoint? RemoteEndPoint` |
| Retain Handling | `byte RetainHandling` | `MqttRetainHandling RetainHandling` |
| MQTT 协议版本 | `byte ProtocolVersion` | `MqttProtocolVersion ProtocolVersion` |

```csharp
using System.Net;
using MqttZero.Protocol;

EndPoint? remoteEndPoint = client.RemoteEndPoint;
MqttRetainHandling retainHandling = request.RetainHandling;
MqttProtocolVersion protocolVersion = packet.ProtocolVersion;
```

Broker 的高级接入 API 现在接收原始 `EndPoint?`：

```csharp
await broker.AcceptConnectionAsync(
    input,
    output,
    remoteEndPoint,
    isSecure: false,
    isWebSocket: false,
    cancellationToken);
```

Dashboard REST JSON 保持不变：`remoteEndPoint` 仍是 `"host:port"` 字符串，UI 不需要迁移。

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

已提交的基准数据和解释位于 `perf/baseline`。当前实测中，fast PUBLISH 解码、Header 编码、Topic 路由、共享订阅、QoS 1 队列、扇出和内存端到端均达到 `0 B/op`。8 KiB 内存端到端约 `317 ns`，约为 MQTTnet 同工作负载 codec+匹配基线的 `1.46x`。Header-only 小报文仍约为 MQTTnet 的 `1.07–1.14x`，公平 Full Writer 和所有路由对比也尚未达到全部计划门槛；这些缺口保留在基线文档中，不应被解释为已经达成。

全局 Packet Writer 池经顺序和 128 并发基准验证后未引入，原因和结果见 `perf/baseline/packet-writer-pool-0.5.0-alpha.1.md`。完整 Broker 对 Broker 的 TCP/QoS/多连接比较方法见 `perf/baseline/broker-comparison-methodology.md`。

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
