# Egress Performance Investigation Closure

## Decision

The egress performance investigation is closed without a new production hot
path claim.

Retained changes:

- Corrected Broker benchmark payload reuse and end-to-end delivery accounting.
- Two-repetition development profile with throughput-spread tie-break rule.
- Per-cell benchmark progress and five-second stage heartbeat.
- Priority queue depth, oldest-work age and low-priority wait diagnostics.

Rejected and removed:

- Universal connection writer.
- Forced-fair Shard scheduling.
- Adaptive Direct/Backlog Egress execution.
- Reusable Flush awaiter.
- Borrowed-input QoS 0 fast path, not started because its cost model was not
  justified.

## Corrected Performance Position

The corrected 64-KiB development baseline is recorded in
`egress-corrected-development-2026-11-26.md`.

Key changes from the corrected benchmark:

- QoS 0 high-fanout throughput was previously understated by unchecked
  measurement backlog.
- QoS 1 / 100 / 64 KiB remains the clearest throughput red cell.
- Publish-call p99 and end-to-end throughput are now reported separately.
- Every completed run validates final received count and records measurement
  drain duration.

Rejected experiment evidence:

- `egress-writer-result-2026-11-25.md`
- `egress-fairness-result-2026-11-26.md`
- `egress-adaptive-send-result-2026-11-26.md`

## Verification

- Locked restore: passed.
- Release solution build: passed, zero warnings and errors.
- Core tests: `579/579`.
- Dashboard .NET tests: `24/24`.
- Broker benchmark tests: `35/35`.
- Allocation sentinels: `22/22`.
- `dotnet format --verify-no-changes`: passed.
- Vue tests: `3/3`.
- Vue production build: passed.
- `npm audit --audit-level=high`: zero vulnerabilities.
- NuGet vulnerable and deprecated package scans: zero findings.
- Package verification: four NuGet packages built and two repository-external
  consumers built successfully.

## Not Performed

- The complete 27-cell, three-repetition performance-claim matrix.
- A QoS 2 performance-claim run.
- Version promotion, performance tagging or merge based on a performance gate.

These were intentionally omitted because all candidate production hot-path
experiments were rejected. QoS 2 correctness remains covered by the core test
suite.

