# Egress Batch Selector Experiment

## Decision

Rejected. The extracted publish-batch selector preserved correctness but made
the target outbound workload slower, so it was removed from the production
send path and the implementation was reverted.

## Target Baseline

- QoS 1
- 100 subscribers
- 64 KiB payload
- Three repetitions, five-second warmup, twenty-second measurement
- MQTTZero median: `106.92 msg/s`, p99 `15,648.2 us`

## Experimental Result

Independent Release build, outbound diagnostics, three-second warmup and
ten-second measurement:

| Metric | Result |
|---|---:|
| Throughput | 98.03 msg/s |
| Publish p50 | 7,654.8 us |
| Publish p99 | 23,451.8 us |
| Average egress batch messages | 1.1896 |
| Egress flush waits | 81,978 |
| Data batches | 81,978 |

The extracted selector did not reduce the number of batches or make a batch
larger. It added another layer to the hot path and regressed the target cell,
so the five-percent no-regression gate failed.

## Retained Evidence

The selector tests and implementation were removed with the reverted commit.
This record remains to prevent repeating the same extraction without a new
performance hypothesis.
