# MQTTZero 0.3.0-alpha.1 Baseline

这些结果是阶段二 `0.3.0-alpha.1` 的短作业 BenchmarkDotNet 基线，不作为跨硬件容量承诺。

## 环境

- Windows 11，12th Gen Intel Core i5-12450H，12 逻辑核 / 8 物理核
- .NET SDK 10.0.400
- .NET 10.0.15，X64 RyuJIT x86-64-v3
- BenchmarkDotNet 0.15.8，`ShortRun`：3 warmup、3 iteration、1 launch
- MQTTnet 5.2.0.1603

## 结论

- `DecodeFast`、`EncodeFast`、`RouteMqttZero` 和内存端到端 `PublishQos0` 均为 `0 B/op`。
- 8 KiB 单段 fast PUBLISH 解码约 `78.77 ns`；MQTTnet 解码约 `431.91 ns`。
- 64 KiB 十段 fast PUBLISH 解码约 `107.19 ns`；MQTTnet 解码约 `2900.36 ns`。
- 8 KiB 内存端到端 QoS 0 路径约 `368.0 ns`，`0 B/op`；同工作负载 MQTTnet decode+match+encode 约 `620.7 ns` 且每次分配约 `8368 B`，当前相对吞吐约 `1.69x`。
- 1/100/1000 订阅者路由分别约 `32.339 ns`、`1685.367 ns`、`17969.005 ns`，均为 `0 B/op`。

## 尚未达成的计划门槛

- MQTTZero 的“将 Payload 复制进目标 Writer”编码与 MQTTnet 的“Payload 以独立 Sequence 返回”不是完全同构；当前 fast encode 的绝对耗时高于 MQTTnet，不能宣称编码 `1.5x`。
- 当前路由在每个消息上展开全部匹配投递候选；与 MQTTnet 仅执行字符串匹配的基线相比，100/1000 全命中场景未达到 `1.5x`。
- 端到端目前使用 MQTTnet packet codec 加 Topic 匹配作为公开可比基线，而不是另一个完整 Broker；`1.2x` 仅在该明确口径下成立。

只有同时满足零分配、性能门槛和稳定性要求的后续版本，才能把上述项目改写为通过。

## 文件

- `codec-0.3.0-alpha.1.*`
- `routing-0.3.0-alpha.1.*`
- `memory-transport-0.3.0-alpha.1.*`
