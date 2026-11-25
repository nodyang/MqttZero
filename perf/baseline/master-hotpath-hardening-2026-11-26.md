# MQTTZero Master Hot-Path Hardening Baseline

Date: 2026-11-26

Branch: `fix/master-hotpath-hardening`

Base: `master` at `eeb8cc6`

## Environment

- Windows `10.0.26300`
- .NET `10.0.15`, `win-x64`
- 12 logical processors
- Server GC enabled, concurrent GC enabled, tiered PGO enabled
- Event Loop Shards: automatic, resolved to 12
- Kestrel I/O queues: automatic, resolved to 12
- Outbound batch default: 128 messages, 512 KiB, 500 us

## Development Matrix

Each QoS was run separately with the same MQTTnet client:

```powershell
dotnet benchmarks/MqttZero.BrokerBenchmarks/bin/Release/net10.0/MqttZero.BrokerBenchmarks.dll compare `
  --client mqttnet --qos <0|1|2> --subscribers 1,100,500 `
  --payload 32,8192,65536 --warmup 2 --seconds 6 `
  --repetitions 2 --max-pending 1024
```

Each cell started with two repetitions. Cells with more than ten-percent
end-to-end throughput spread, failed runs or incomplete delivery automatically
received a third repetition. All 27 cells delivered every expected message and
reported zero failed operations.

| QoS | Subscribers | Payload | Runs Z/N | MQTTZero msg/s | MQTTnet msg/s | Throughput ratio | MQTTZero p99 us | MQTTnet p99 us | p99 ratio |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 1 | 32 | 2/2 | 31,842.17 | 32,715.17 | 0.973 | 158 | 176 | 0.897 |
| 0 | 1 | 8,192 | 3/3 | 3,029.53 | 2,274.50 | 1.332 | 559 | 512 | 1.092 |
| 0 | 1 | 65,536 | 3/3 | 1,017.42 | 1,047.72 | 0.971 | 4,132 | 2,964 | 1.394 |
| 0 | 100 | 32 | 2/2 | 418.81 | 329.15 | 1.272 | 150 | 233 | 0.644 |
| 0 | 100 | 8,192 | 2/2 | 178.14 | 130.44 | 1.366 | 12,202 | 25,346 | 0.481 |
| 0 | 100 | 65,536 | 3/3 | 26.70 | 29.03 | 0.920 | 16,277 | 75,072 | 0.217 |
| 0 | 500 | 32 | 3/3 | 58.35 | 51.98 | 1.123 | 204 | 176 | 1.159 |
| 0 | 500 | 8,192 | 3/3 | 27.66 | 25.44 | 1.087 | 13,121 | 12,056 | 1.088 |
| 0 | 500 | 65,536 | 3/3 | 5.01 | 5.08 | 0.988 | 37,105 | 28,966 | 1.281 |
| 1 | 1 | 32 | 3/3 | 2,083.32 | 1,853.76 | 1.124 | 830 | 977 | 0.850 |
| 1 | 1 | 8,192 | 3/3 | 1,674.16 | 1,732.89 | 0.966 | 1,025 | 1,008 | 1.017 |
| 1 | 1 | 65,536 | 3/3 | 944.15 | 936.70 | 1.008 | 10,208 | 8,047 | 1.269 |
| 1 | 100 | 32 | 3/3 | 110.24 | 107.70 | 1.024 | 11,002 | 12,688 | 0.867 |
| 1 | 100 | 8,192 | 3/3 | 73.79 | 77.70 | 0.950 | 23,934 | 30,721 | 0.779 |
| 1 | 100 | 65,536 | 3/3 | 21.75 | 22.72 | 0.958 | 69,275 | 82,131 | 0.843 |
| 1 | 500 | 32 | 3/3 | 18.82 | 18.80 | 1.001 | 70,455 | 85,923 | 0.820 |
| 1 | 500 | 8,192 | 3/3 | 15.00 | 14.57 | 1.030 | 96,083 | 102,763 | 0.935 |
| 1 | 500 | 65,536 | 3/3 | 4.21 | 3.99 | 1.056 | 274,875 | 379,355 | 0.725 |
| 2 | 1 | 32 | 3/3 | 926.40 | 896.26 | 1.034 | 2,117 | 2,224 | 0.952 |
| 2 | 1 | 8,192 | 3/3 | 841.83 | 906.64 | 0.929 | 1,983 | 1,669 | 1.188 |
| 2 | 1 | 65,536 | 2/2 | 601.18 | 622.89 | 0.965 | 7,956 | 6,726 | 1.183 |
| 2 | 100 | 32 | 2/2 | 55.26 | 54.36 | 1.017 | 26,627 | 33,476 | 0.795 |
| 2 | 100 | 8,192 | 2/2 | 47.01 | 43.47 | 1.081 | 33,734 | 44,058 | 0.766 |
| 2 | 100 | 65,536 | 3/3 | 17.30 | 17.73 | 0.976 | 72,661 | 71,570 | 1.015 |
| 2 | 500 | 32 | 2/2 | 10.49 | 10.27 | 1.022 | 128,036 | 156,884 | 0.816 |
| 2 | 500 | 8,192 | 3/3 | 8.63 | 8.66 | 0.997 | 165,382 | 163,464 | 1.012 |
| 2 | 500 | 65,536 | 2/2 | 3.46 | 3.73 | 0.926 | 350,936 | 333,716 | 1.052 |

These are development baselines, not a claim that every strict leadership gate
passes.

## Confirmation Profile

Target: QoS 1, 100 subscribers, 64 KiB payload.

```powershell
dotnet benchmarks/MqttZero.BrokerBenchmarks/bin/Release/net10.0/MqttZero.BrokerBenchmarks.dll compare `
  --client mqttnet --qos 1 --subscribers 100 --payload 65536 `
  --warmup 5 --seconds 20 --repetitions 2 --max-pending 1024
```

| Broker | End-to-end msg/s | Publish p99 us | Drain ms |
|---|---:|---:|---:|
| MQTTZero | 21.56, 22.85; median 22.21 | 65,049, 56,819; median 60,934 | 0.02, 18.67 |
| MQTTnet | 23.09, 24.58; median 23.84 | 64,126, 57,128; median 60,627 | 30.26, 13.28 |

MQTTZero throughput was `0.932x` MQTTnet and p99 was `1.005x` MQTTnet.

## Target Diagnostics

The in-process outbound diagnostic for the same target reported:

- 32,800 PUBLISH deliveries
- 19,656 asynchronous Flushes
- 21,483 Egress Actor drains
- 17,062 Egress Scheduler passes
- 1.67 messages per Egress batch
- Session lock wait average `0.16 us`, maximum `83.9 us`

Flush lifecycle and scheduling dominated this cell. Session critical-section
work was not selected for experimentation.

## Rejected Flush Completion Experiment

A reusable Flush observer reduced the measured custom Flush operation cost from
about `2.8 KiB` per asynchronous Flush to one runtime continuation allocation of
about `32 B`. The target throughput medians were:

- Baseline: `22.21 msg/s`, p99 `60.93 ms`
- Experiment: `22.08 msg/s`, p99 `61.41 ms`

The experiment failed the required ten-percent primary-metric improvement and
was removed from the production path. Its raw result is retained in
`hardening-qos1-100-64k-flush-reuse-2026-11-26.json`.

## Final Target Check

After cleanup and the owning-Shard stop fix, a two-repetition development check
produced:

- MQTTZero: `22.33`, `21.52`; median `21.93 msg/s`
- MQTTZero p99: `58,383`, `69,361`; median `63,872 us`
- MQTTnet: `22.87`, `23.53`; median `23.20 msg/s`
- MQTTnet p99: `63,544`, `59,097`; median `61,321 us`

Relative to the confirmation baseline, throughput changed by `-1.3%` and p99
changed by `+4.8%`, both within the selected five-percent non-regression limit.
Every measurement completed delivery with zero failed operations.

## Raw Results

- `master-hardening-rebaseline-qos0-2026-11-26.json`
- `master-hardening-rebaseline-qos1-2026-11-26.json`
- `master-hardening-rebaseline-qos2-2026-11-26.json`
- `hardening-qos1-100-64k-confirmation-2026-11-26.json`
- `hardening-qos1-100-64k-flush-reuse-2026-11-26.json`
- `hardening-final-qos1-100-64k-2026-11-26.json`
