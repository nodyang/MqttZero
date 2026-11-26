# MQTTZero 与 MQTTnet 复测（8 核，2026-11-26）

## 环境

- Windows `10.0.26300`，.NET `10.0.15`，8 个逻辑处理器
- Server GC、并发 GC、分层 PGO
- 自动 Shard 数与 Kestrel I/O 队列数均为 8
- 客户端：MQTTnet 5.2.0，进程内单连接发布
- 预热 5 秒、测量 20 秒、3 轮，取中位数
- 提交：`b735aa9`（本轮只改基准、缓冲池与停机路径，未改出站热路径）

命令：

```powershell
dotnet benchmarks/MqttZero.BrokerBenchmarks/bin/Release/net10.0/MqttZero.BrokerBenchmarks.dll compare `
  --client mqttnet --qos <qos> --subscribers <n> --payload <bytes> `
  --warmup 5 --seconds 20 --repetitions 3 --max-pending 1024 `
  --output artifacts/compare-target/<cell>.json
```

## 结果

| 单元格 | MQTTZero msg/s | MQTTnet msg/s | 吞吐比 | 发布 p99 µs（Zero/net） | 发布 p99 比 | 交付 p99 µs（Zero/net） | 交付 p99 比 | Broker 峰值工作集 MB | 排空 ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| QoS 0 / 1 / 32 B | 7,773.49 | 7,991.72 | 0.973 | 254 / 250 | 1.02 | 820 / 849 | 0.97 | 79 / 52 | 0.0 / 0.0 |
| QoS 0 / 100 / 8 KiB | 222.85 | 257.30 | 0.866 | 935 / 863 | 1.08 | 145,896 / 70,675 | 2.06 | 112 / 104 | 23.8 / 0.0 |
| QoS 1 / 100 / 64 KiB | 31.52 | 36.44 | 0.865 | 149,545 / 111,032 | 1.35 | 118,462 / 143,815 | 0.82 | 76 / 93 | 0.0 / 12.4 |

三个单元格均为零失败、零缺口、零重复、零序号回退。
严格门禁（吞吐比 > 1 且发布 p99 比 < 1）在三个单元格都未通过。

## 与 12 核基线的关系

`master-hotpath-hardening-2026-11-26.md` 在 12 核机器上记录了完整 27 格：
8 格领先超过 5%，16 格在 ±5% 内，3 格落后超过 5%
（QoS 0 / 100 / 64 KiB 0.920，QoS 2 / 1 / 8 KiB 0.929，
QoS 2 / 500 / 64 KiB 0.926）。

同一 QoS 0 / 100 / 8 KiB 单元格在 12 核上是 1.366（领先），在本机 8 核上是
0.866（落后）。这说明该单元格对核数与 Shard 映射敏感，不能把 12 核的结论
外推到 8 核部署。本机 8 核的 Shard 4 与 I/O 队列 4 对照也没有改善目标格
（见 `ab-config-2026-11-26.md`）。

## 原始结果

- `compare-8core-qos0-1-32-2026-11-26.json`
- `compare-8core-qos0-100-8k-2026-11-26.json`
- `compare-8core-qos1-100-64k-2026-11-26.json`
