# MQTTZero / MQTTnet Broker Comparison

该基准用于比较 MQTTZero 与 MQTTnet 完整 TCP Broker，不再使用“MQTTnet codec + topic matcher”作为端到端基线。

## 公平性口径

- Broker 在独立子进程中启动；客户端负载生成器不与被测 Broker 共享进程。
- 两个 Broker 均绑定 `127.0.0.1`，关闭日志，使用 MQTT 5 客户端。
- 客户端 clean start，Topic 固定为 `bench/topic`，不启用 TLS、Retained、共享订阅或会话恢复。
- MQTTZero `MaxMessageQueueLength` 与 MQTTnet `MaxPendingMessagesPerClient` 均设为 100,000。
- QoS 0 使用最多约 1,024 条未消费消息的闭环限制，避免仅因负载生成器压垮服务端队列而失去可比性。
- 默认预热 15 秒、测量 60 秒；每个矩阵单元独立启动和关闭 Broker。

## 默认矩阵

- QoS：0、1
- 订阅者：1、100、1000
- Payload：32 B、8 KiB、64 KiB
- 发布者：1

## 运行

完整矩阵：

```powershell
dotnet run -c Release --project benchmarks/MqttZero.BrokerBenchmarks -- `
  compare
```

快速验证单个场景：

```powershell
dotnet run -c Release --project benchmarks/MqttZero.BrokerBenchmarks -- `
  compare --qos 1 --subscribers 10 --payload 1024 --warmup 2 --seconds 10
```

输出为 JSON，包含发布数、接收数、失败数、每秒吞吐以及延迟 p50/p95/p99。

## 工具链验证

2026-11-24 使用 `--qos 1 --subscribers 2 --payload 256 --warmup 1 --seconds 2` 对两个 Broker 各执行一次快速比较，均完成连接、订阅、发布和接收，失败数为 0。该短时运行只用于验证进程隔离、负载生成和结果采集，不作为性能结论。

## 结果记录

容量结论必须附带执行日期、机器型号、操作系统、.NET 版本、完整命令和三次运行结果。短时快速验证只用于检查工具链，不作为性能声明。
