# Egress Connection Writer Result

## Decision

Rejected. The complete connection writer passed the target QoS 1 / 100
subscriber / 64 KiB cell, but regressed the single-subscriber 64 KiB cells
beyond the five-percent stop threshold. The production change was removed.

## Target Cell

Three repetitions, five-second warmup, twenty-second measurement:

| Broker | Throughput msg/s | p99 us |
|---|---:|---:|
| MQTTZero Writer | 106.58 | 23,518.1 |
| MQTTnet | 101.04 | 33,261.7 |

Throughput ratio: `1.055x`. p99 ratio: `0.707x`.

The normalized ratio improved from `0.971x` to `1.055x`, but the absolute
MQTTZero result was not three percent above the previous baseline because the
machine's MQTTnet baseline also moved between runs.

## Regression Cells

| QoS | Subscribers | Payload | MQTTZero msg/s | Previous MQTTZero msg/s | Change |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 32 B | 8,543.5 | ~7,808 | +9.4% |
| 1 | 100 | 32 B | 633.3 | ~660 | -4.0% |
| 0 | 1 | 64 KiB | 3,159.8 | ~3,593 | -12.1% |
| 1 | 1 | 64 KiB | 1,707.2 | ~2,090 | -18.3% |

The single-subscriber 64 KiB regressions violate the five-percent stop rule.
The target improvements do not justify accepting those regressions.

## Evidence

- `egress-writer-result-2026-11-25.json`
- `egress-writer-regression-single-32-2026-11-25.json`
- `egress-writer-regression-100-32-2026-11-25.json`
- `egress-writer-regression-single-64k-2026-11-25.json`

## Retained Code

- Deterministic control-frame test.
- Send-stage diagnostics.
- Baseline and experiment records.

The `MqttZeroConnectionWriter` production implementation was not retained.
