# Egress Fairness Experiment Result

## Decision

Rejected. Bounded fair Shard scheduling did not produce the required repeatable
performance improvement and regressed two critical development cells by more
than five percent. The forced-fairness scheduling behavior was reverted.

Priority queue depth, oldest-work age and low-priority wait diagnostics were
retained.

## Experiment

- QoS `0/1`
- Subscribers `100/500`
- Payload `64 KiB`
- Warmup `2 s`, measurement `6 s`, repetitions `2`
- MQTTnet Client
- Same-machine corrected baseline:
  `egress-corrected-development-2026-11-26.md`
- Raw JSON:
  `egress-fairness-qos01-100-500-64k-2026-11-26.json`

## Results

| QoS | Subscribers | Baseline E2E msg/s | Fair E2E msg/s | Change | Baseline p99 us | Fair p99 us | Change |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 100 | 32.04 | 30.38 | -5.2% | 38,023.4 | 58,162.3 | +53.0% |
| 0 | 500 | 5.41 | 5.46 | +0.9% | 28,083.0 | 32,857.8 | +17.0% |
| 1 | 100 | 21.33 | 22.55 | +5.7% | 70,747.4 | 54,925.5 | -22.4% |
| 1 | 500 | 4.56 | 4.30 | -5.7% | 252,870.6 | 289,312.5 | +14.4% |

No cell achieved a ten-percent throughput improvement. QoS `0/100` and
QoS `1/500` regressed by more than the five-percent stop threshold.

## Follow-up Attribution

After reverting forced fairness, a `QoS 1 / 100 / 64 KiB` outbound diagnostic
recorded:

| Metric | Result |
|---|---:|
| Publish throughput | 29.99 msg/s |
| Publish p99 | 65,097.7 us |
| Average egress batch messages | 1.75 |
| Data batches | 17,048 |
| Flush waits | 17,048 |
| Egress Actor drains | 18,237 |
| Egress Scheduler passes | 10,514 |
| Average encode per publish | 37.92 us |

Every data batch waited on an asynchronous Flush. This supports proceeding to
the conditional Direct/Backlog Egress experiment rather than retaining forced
priority switching.

