# MQTTZero 0.11.0-alpha.3 Memory Closure Baseline

Date: 2026-11-26

Branch: `perf/memory-closure`

Base: `master` at `d69f5bc`

## Memory Stability

Command:

```powershell
pwsh -NoProfile -File ./eng/verify-memory-stability.ps1 -Waves 10 -Messages 100
```

Both scenarios completed ten waves, drained every expected message and reported
zero failed operations. Broker-owned counters remained bounded.

| Scenario | Final committed buffers | Final route retention | Final lease pool | Final message pool | Shared cursors |
|---|---:|---:|---:|---:|---:|
| QoS 0 / 100 / 8 KiB | 0.11 MB | 0 | 5 | 100 | 0 |
| QoS 1 / 100 / 64 KiB | 5.40 MB | 0 | 43 | 2,210 | 0 |

Object pool counts reached a stable plateau. Committed BufferPool bytes never
exceeded `MaxBufferedBytes`, and RoutePlan retention returned to zero after
drain. Managed heap and RSS include the cohosted MQTTnet client and are reported
as diagnostics rather than Broker ownership gates.

## Development Matrix

QoS 0 used the full 27-cell matrix. Failed/importantly divergent cells received
automatic third repetitions. Per the user's later instruction, QoS 1 and QoS 2
used one repetition and only 32 B/8 KiB payloads.

All runs completed subscriber drain with zero failed operations.

QoS 0 medians:

| Subscribers | Payload | MQTTZero msg/s | MQTTnet msg/s | Ratio |
|---:|---:|---:|---:|---:|
| 1 | 32 | 31,912.51 | 32,352.32 | 0.99 |
| 1 | 8,192 | 2,604.35 | 2,214.06 | 1.18 |
| 1 | 65,536 | 1,014.75 | 1,060.40 | 0.96 |
| 100 | 32 | 425.46 | 339.40 | 1.25 |
| 100 | 8,192 | 162.33 | 136.35 | 1.19 |
| 100 | 65,536 | 28.24 | 29.76 | 0.95 |
| 500 | 32 | 60.00 | 51.69 | 1.16 |
| 500 | 8,192 | 28.06 | 25.87 | 1.08 |
| 500 | 65,536 | 5.39 | 5.44 | 0.99 |

QoS 1 and QoS 2 one-run ratios remained between `0.90x` and `1.16x` versus
MQTTnet, with no failed operations or incomplete drains.

## Same-Machine Master Controls

The old absolute baseline showed environment drift on a few small-packet cells.
A clean `master` export was built and measured on the same machine immediately
before the final comparisons:

| Cell | Master median | Memory branch median | Change |
|---|---:|---:|---:|
| QoS 0 / 1 / 8 KiB | 2,879.03 msg/s | 2,788.55-3,151.83 | within 5% |
| QoS 0 / 100 / 8 KiB | 168.09 msg/s | 163.61 msg/s | -2.7% |
| QoS 2 / 500 / 32 B | 9.61 msg/s | 9.58 msg/s | -0.3% |

No critical cell regressed by more than five percent against the same-machine
master control. The initial cross-time differences were therefore not accepted
as release regressions without a current control.

## Raw Results

- `memory-closure-qos0-2026-11-26.json`
- `memory-closure-qos1-small-2026-11-26.json`
- `memory-closure-qos2-small-2026-11-26.json`
- `memory-closure-qos2-32k-fanout-confirm-2026-11-26.json`
- `memory-closure-qos0-8k-optimized-2026-11-26.json`
- `memory-control-master-qos0-8k.json`
- `memory-control-master-qos2-32.json`
