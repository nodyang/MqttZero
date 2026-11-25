# Egress Closure Baseline

## Target

- MQTT 5
- QoS 1
- 100 subscribers
- 64 KiB payload
- One publisher
- Three repetitions, five-second warmup, twenty-second measurement
- MQTTnet Client for both Brokers

## Environment

- Windows 10.0.26300
- .NET 10.0.15
- 12 logical processors
- Server GC, concurrent GC and TieredPGO enabled
- Default twelve event-loop shards and twelve Kestrel IO queues

## Command

```powershell
dotnet run -c Release --no-build --project benchmarks/MqttZero.BrokerBenchmarks -- `
  compare --client mqttnet --qos 1 --subscribers 100 --payload 65536 `
  --warmup 5 --seconds 20 --repetitions 3 --max-pending 1024 `
  --output perf/baseline/egress-closure-qos1-100-64k-2026-11-25.json
```

## Median Baseline

| Broker | Throughput msg/s | p99 us | Failures |
|---|---:|---:|---:|
| MQTTZero | 106.92 | 15,648.2 | 0 |
| MQTTnet | 110.07 | 20,117.5 | 0 |

The strict performance-claim gate is red because MQTTZero throughput is
`0.971x` MQTTnet. The p99 ratio is `0.778x`. This is the no-regression and
five-percent tail-improvement reference for the Egress closure experiment.

Raw JSON: `perf/baseline/egress-closure-qos1-100-64k-2026-11-25.json`.
