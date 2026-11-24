# Packet Writer Pool Decision

记录日期：2026-11-24

## 目的

对比每连接创建一个 `MqttZeroPacketWriter` 与使用全局 Writer 池的创建、使用和回收成本。

## 环境口径

- BenchmarkDotNet `0.15.8`，`ShortRun`：3 warmup、3 iteration、1 launch
- .NET `10.0.15`，X64 RyuJIT
- 每次基准执行 10,000 次 Writer 使用
- `Parallelism=1` 与 `Parallelism=128`

运行命令：

```powershell
dotnet run -c Release --project benchmarks/MqttZero.Benchmarks -- `
  --filter '*PacketWriterLifecycleBenchmarks*' --job short
```

## 结果

| 方案 | 并发 | 中位数 | 分配 |
|---|---:|---:|---:|
| 每连接创建 | 1 | 276.4 us | 3048.28 KB |
| 全局池 | 1 | 193.2 us | 1.47 KB |
| 每连接创建 | 128 | 489.6 us | 3057.49 KB |
| 全局池 | 128 | 1475.0 us | 9.77 KB |

## 决策

不引入生产 Writer 池。

单线程池化减少了约 30% 时间和几乎全部分配，但 128 并发下因队列竞争退化约 3.22 倍，未满足“顺序与并发场景均改善”的门槛。生产代码继续使用每连接 Writer；保留该基准用于后续重新评估。
