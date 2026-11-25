# Adaptive Egress Send Experiment Result

## Decision

Rejected. Backlog continuation and Flush-completion reuse were removed from
production code. No Task 3 implementation was retained.

QoS 1-only backlog continuation produced a short-run improvement, but it did
not survive a 5-second warmup / 20-second measurement comparison. The longer
run also removed the apparent single-run benefit.

## Experiment Variants

1. Backlog continuation for every QoS.
2. Backlog continuation restricted to batches containing only QoS 1/2.
3. QoS 1-only selective variant over 5-second warmup / 20-second measurement.

All runs used the MQTTnet Client, 64-KiB payloads and complete measurement
drain validation.

## Short Development Matrix

QoS 0/1, subscribers 1/100/500, two repetitions:

| Variant | QoS | Subscribers | MQTTZero E2E msg/s | MQTTnet E2E msg/s | Ratio |
|---|---:|---:|---:|---:|---:|
| All-QoS Backlog | 0 | 100 | 28.96 | 28.65 | 1.01x |
| All-QoS Backlog | 1 | 100 | 24.10 | 24.00 | 1.00x |
| QoS 1-only Backlog | 0 | 100 | 28.47 | 29.51 | 0.96x |
| QoS 1-only Backlog | 1 | 100 | 22.65 | 22.52 | 1.01x |

The corrected baseline used for same-machine integration comparison was
`egress-corrected-development-2026-11-26.md`. Short-run changes were dominated
by machine drift and did not meet the ten-percent improvement gate.

## Long Comparison

QoS 0/1, 100 subscribers, 64 KiB, 5-second warmup, 20-second measurement,
two repetitions:

| QoS | MQTTZero E2E msg/s | MQTTnet E2E msg/s | Ratio | MQTTZero publish p99 us | MQTTnet publish p99 us | p99 ratio |
|---:|---:|---:|---:|---:|---:|---:|
| 0 | 26.77 | 28.69 | 0.93x | 72,920.6 | 44,634.1 | 1.63x |
| 1 | 22.40 | 23.68 | 0.95x | 65,997.9 | 59,669.9 | 1.11x |

No ten-percent throughput or p99 improvement remained under the longer run.

## Raw Evidence

- `egress-adaptive-qos01-1-100-500-64k-2026-11-26.json`
- `egress-adaptive-qos1-only-qos01-1-100-500-64k-2026-11-26.json`
- `egress-adaptive-qos1-only-100-64k-2026-11-26.json`
- `egress-adaptive-qos1-only-100-64k-long-2026-11-26.json`

## Task 4 Decision

Not started. QoS 0 fanout remains around `0.93–0.95x` MQTTnet in the longer
run, but the remaining difference is not explained by the extra Payload copy:
the measured 64-KiB payload-copy lower bound is approximately `1.17 us`, while
the relevant publish p99 is tens of milliseconds. A borrowed-input Lease fast
path would carry substantial ownership and backpressure risk without evidence
that it can recover the measured tail.

