# Corrected Egress Development Baseline

## Purpose

Record the first same-machine baseline after correcting the Broker benchmark
contract:

- MQTTnet Client reuses the caller payload through
  `WithPayload(ReadOnlySequence<byte>)`.
- Publish-window throughput and measured end-to-end throughput are separate.
- Publisher API-call p99 is not described as end-to-end delivery p99.
- Every run waits for `PublishedMessages * Subscribers` and records the final
  measurement drain duration.
- The measurement publishing loop now uses the measured publish counter for
  its `max-pending` backpressure check.

## Environment

- Windows `10.0.26300`
- .NET Runtime `10.0.15`
- 12 logical processors
- Server GC, concurrent GC and TieredPGO enabled
- 12 Event Loop Shards and 12 Kestrel IO queues
- Outbound batch defaults: `512 KiB`, `128` messages, `500 us`
- MQTTnet Client `5.2.0.1603`

## Development Profile

```powershell
dotnet benchmarks/MqttZero.BrokerBenchmarks/bin/Release/net10.0/MqttZero.BrokerBenchmarks.dll compare `
  --client mqttnet --qos 0,1 --subscribers 1,100,500 --payload 65536 `
  --warmup 2 --seconds 6 --repetitions 2 --max-pending 1024 `
  --start-port 59000 `
  --output perf/baseline/egress-corrected-development-2026-11-26.json
```

The two-run throughput spread exceeded ten percent for:

- QoS `0`, 1 subscriber: MQTTnet `802.87` versus `912.45` msg/s.
- QoS `1`, 500 subscribers: MQTTnet `4.49` versus `3.73` msg/s.

A third run was recorded for only those cells:

- `egress-corrected-development-third-q0-1-2026-11-26.json`
- `egress-corrected-development-third-q1-500-2026-11-26.json`

Other cells use the midpoint of two runs. Final performance claims still
require three repetitions.

## Consolidated Medians

| QoS | Subscribers | Payload | Runs MQTTZero/MQTTnet | MQTTZero E2E msg/s | MQTTnet E2E msg/s | Throughput ratio | MQTTZero publish p99 us | MQTTnet publish p99 us | p99 ratio |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 1 | 64 KiB | 3 / 3 | 969.48 | 837.87 | 1.16x | 4,721.7 | 2,959.8 | 1.60x |
| 0 | 100 | 64 KiB | 2 / 2 | 32.04 | 28.88 | 1.11x | 38,023.4 | 58,377.6 | 0.65x |
| 0 | 500 | 64 KiB | 2 / 2 | 5.41 | 5.20 | 1.04x | 28,083.0 | 19,435.6 | 1.44x |
| 1 | 1 | 64 KiB | 2 / 2 | 932.91 | 907.17 | 1.03x | 9,856.2 | 6,401.0 | 1.54x |
| 1 | 100 | 64 KiB | 2 / 2 | 21.33 | 22.95 | 0.93x | 70,747.4 | 61,658.4 | 1.15x |
| 1 | 500 | 64 KiB | 3 / 3 | 4.55 | 4.49 | 1.01x | 255,783.4 | 304,216.1 | 0.84x |

All runs had zero failed operations and zero incomplete deliveries.

## Findings

- The old QoS 0 large-payload comparison was dominated by unchecked
  measurement backlog. With corrected backpressure, QoS 0 / 100 and 500 are at
  or slightly ahead of MQTTnet in end-to-end throughput.
- `QoS 1 / 100 / 64 KiB` is now the clear throughput red cell at `0.93x`.
- Publish-call p99 remains worse for QoS 0 single/500 and QoS 1 single/100.
- QoS 1 / 500 throughput and p99 both pass the provisional comparison, but the
  absolute tail is still high and only the p99 ratio is below MQTTnet.
- The previous claim that high fanout should improve by more than ten percent
  through fair scheduling alone is not supported by this corrected baseline.
  Fairness remains an architectural safety property, but its performance gate
  must be interpreted per cell rather than as a universal high-fanout win.

