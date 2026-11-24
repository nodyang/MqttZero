# MQTTZero Session Outbound Pump Broker Comparison

## 环境

- 日期：2026-11-24
- Windows 11，12th Gen Intel Core i5-12450H，12 逻辑核 / 8 物理核
- .NET SDK 10.0.400，Release
- MQTTnet 5.2.0.1603
- 单发布者，单订阅者，独立 Broker 子进程，`127.0.0.1`

## 修复前记录

QoS 1、64 KiB、单订阅者时，MQTTZero 在发布者输入链路中同步等待订阅者网络 Flush：

- MQTTZero：约 `103.73 msg/s`，p99 约 `302843 us`
- MQTTnet：约 `804.45 msg/s`，p99 约 `4975.8 us`

该结果用于定位同步 Flush 耦合，不作为最终回归门禁。

## 修复后结果

运行命令：

```powershell
dotnet run -c Release --project benchmarks/MqttZero.BrokerBenchmarks -- `
  compare --qos 1 --subscribers 1 --payload 65536 --warmup 5 --seconds 15
```

QoS 1、64 KiB：

| Broker | 吞吐 | p50 | p95 | p99 | 失败数 |
| --- | ---: | ---: | ---: | ---: | ---: |
| MQTTZero | 882.19 msg/s | 743.0 us | 3235.7 us | 6635.2 us | 0 |
| MQTTnet | 927.43 msg/s | 723.0 us | 2977.6 us | 4282.7 us | 0 |

MQTTZero 相对 MQTTnet 约 `0.95x`，相对修复前至少 `8.5x`；p99 低于 `20 ms` 门禁。

运行命令：

```powershell
dotnet run -c Release --project benchmarks/MqttZero.BrokerBenchmarks -- `
  compare --qos 1 --subscribers 1 --payload 32,8192 --warmup 3 --seconds 10
```

QoS 1、单订阅者：

| Payload | MQTTZero | MQTTnet | 相对值 |
| ---: | ---: | ---: | ---: |
| 32 B | 1964.29 msg/s | 1818.00 msg/s | 1.08x |
| 8 KiB | 1586.98 msg/s | 1603.85 msg/s | 0.99x |

运行命令：

```powershell
dotnet run -c Release --project benchmarks/MqttZero.BrokerBenchmarks -- `
  compare --qos 0 --subscribers 1 --payload 65536 --warmup 5 --seconds 15
```

QoS 0、64 KiB：

| Broker | 吞吐 | p99 | 失败数 |
| --- | ---: | ---: | ---: |
| MQTTZero | 1106.68 msg/s | 5345.1 us | 0 |
| MQTTnet | 857.83 msg/s | 1597.3 us | 0 |

QoS 0 使用未消费消息上限控制生产速度，结束时可能有少量消息仍在链路中；吞吐按测量阶段成功发布数计算。

## 结论与边界

- 订阅者 PUBLISH 全部先入队，发布方 PUBACK 不再等待订阅者网络 Flush。
- 每个活跃 Session 只有一个 outbound pump；控制包仍使用连接写锁直接发送。
- pump 的异步续接、线程调度和连接生命周期对象不属于逐消息同步入队 `0 B/op` 预算。
- 本次只记录单机短时对照，不构成跨硬件容量承诺。
