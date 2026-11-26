# 会话过期索引与临界区（2026-11-26）

## 改动

出站 drain 的临界区里，`RemoveExpired` 原本在队列中存在任何带 Message Expiry
的消息时，从队头走到队尾逐条判断。改为会话级到期小顶堆 + 惰性失效后，
每批只付一次 `TryPeek`，外加真正到期的条目。另外把 inflight 上限提到批次
循环外计算一次。

## 新增基准参数

- `--message-expiry <seconds>`：发布时带 Message Expiry Interval
- `--subscriber-delay-us <µs>`：订阅端在记录交付延迟后延迟返回

raw 客户端与 `--message-expiry` 组合会被参数校验直接拒绝。

## 尝试的 profile

```powershell
MqttZero.BrokerBenchmarks ab `
  --baseline-broker <master dll> --candidate-broker <dll> `
  --qos 0 --subscribers 1 --payload 32 --message-expiry 300 `
  --subscriber-delay-us 200 --max-pending 100000 `
  --warmup 5 --seconds 20 --rounds 3 --drain-timeout 120
```

结果（3 轮交替）：

| 指标 | 基线 `99af7b5` | 候选 | 变化 |
|---|---:|---:|---:|
| 端到端吞吐 | 约 7,460-7,600 msg/s | 约 7,600 msg/s | -3.7% ~ 持平 |
| 交付 p99 | 约 1,095 µs | 约 985 µs | 噪声区间 |

## 结论

这个 profile **没有**把队列压深：吞吐约 7.5k msg/s、交付 p99 约 1 ms，
按此估算队列深度只有个位数消息，O(队列) 扫描从来没有跑在长队列上。
订阅端的 `ApplicationMessageReceivedAsync` 延迟也没有对 broker 出口形成
背压（否则吞吐应被限制在 5k msg/s 以下），说明 MQTTnet 客户端不会因为
处理延时停读。

因此该 profile 无法证明这项改动的吞吐收益，也不应该用它下结论。改动的
依据是复杂度本身（O(队列) → O(到期)）以及队列层的确定性测试；要端到端
复现需要真正停滞的消费者（例如 raw 客户端暂停读取），这属于后续的基准
工具工作。

## 保留内容

- `--message-expiry` 与 `--subscriber-delay-us` 两个参数保留，供后续
  "停滞消费者" profile 复用。
- 原始结果：`session-expiry-2026-11-26.json`。
