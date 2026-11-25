# MQTTZero Master Hot-Path Code Review

## Scope

- Branch: `master`
- Commit: `1a6b71e`
- Review date: 2026-11-26
- Focus: correctness, concurrency, benchmark validity and end-to-end send-path
  performance
- Skills applied: `csharp-code-rules`

This review is read-only. No production change is included.

## Executive Summary

The Sharded Actor, Route Epoch and pooled message ownership foundations remain
appropriate. The highest-risk problems are no longer Encode micro-optimizations:

1. A failed `BeginFlush` can release a QoS 0 message twice.
2. Egress shutdown can mutate a non-thread-safe control queue/list from outside
   the owning Shard.
3. The benchmark still has measurement contamination and sampling gaps.
4. Shard priority diagnostics add unconditional atomics to production traffic.

The next work should be correctness and benchmark hardening. Further Egress
redesign should wait until those are fixed and remeasured.

## Findings

| ID | Severity | Area | Finding |
|---|---|---|---|
| P0-1 | Critical | Lease ownership | QoS 0 release occurs before `BeginFlush`; a synchronous error can release the same message twice |
| P0-2 | Critical | Egress concurrency | Off-Shard stop can mutate `_controlQueue` and `_controlBatch` while Actor execution reads them |
| P1-1 | High | Benchmark | Latency samples stop at 100,000 publishes, biasing high-throughput p99 |
| P1-2 | High | Benchmark | Progress heartbeat runs inside the measured publisher loop and scans all subscribers |
| P1-3 | High | Diagnostics | Shard priority counters execute unconditional `Interlocked` operations when diagnostics are disabled |
| P1-4 | High | Scheduling | High-priority queue still has absolute priority and can indefinitely delay low-priority Egress |
| P1-5 | Medium | Diagnostics | Outbound benchmark does not require final subscriber drain before reporting |
| P2-1 | Medium | Egress | Every asynchronous Flush still creates an operation object and async Task |
| P2-2 | Medium | Delivery | Session lock covers batch selection, encoded-length work and Inflight insertion |
| P2-3 | Medium | Encoding | `SendPublishBatchAsync` and `TryEncodePublishBatch` duplicate packet emission logic |
| P2-4 | Low | Cleanup | Obsolete `MqttZeroSessionOutboundPump` remains in production assembly with no production caller |

## P0-1: QoS 0 Double Release

In `MqttZeroBroker.Delivery.cs`, QoS 0 messages are released after encoding and
before `egress.BeginFlush(...)`. If `BeginFlush` throws, the enclosing catch
releases every QoS 0 message in the batch again.

Sequence:

```text
Encode batch
  -> Release QoS 0 queued messages
  -> BeginFlush throws
  -> Catch releases the same QoS 0 messages again
```

`BeginFlush` can intentionally throw when the PipeWriter completed or when a
Flush is already pending. The result can be a negative Lease reference count,
premature pool reuse and bounded-buffer corruption.

Required correction:

- Track whether batch Release already occurred.
- Perform QoS 0 release only after the Flush handoff is accepted, or make the
  catch path aware that Release completed.
- Add a deterministic test where the flush ValueTask is already completed with
  `FlushResult.IsCompleted=true`.

## P0-2: Off-Shard Control Queue Mutation

`MqttZeroEgressActor.StopAsync()` can call `FailQueuedControls()` from the
caller thread. Session detach and broker lifecycle call this method outside the
Actor's owning Shard.

At the same time, the Actor may be executing `RunControlBatchAsync()` on its
Shard. `Queue<T>` and `List<T>` are not thread-safe, so the stop path can race
with:

- control enqueue/dequeue;
- `_controlBatch.Clear()` and iteration;
- frame completion and release;
- final stop checks.

The Flush-completion callback also reads actor/control state on an arbitrary
completion thread. State mutation and final stop decisions should return to the
Actor's Shard.

Required correction:

- Completion callback only clears the FlushPending state and posts a reusable
  completion Work item to the owning Shard.
- Stop first marks stopping, then posts the stop/cleanup command to the Shard.
- All `_controlQueue`, `_controlBatch`, executing/ready and final-release
  transitions remain Shard-confined.
- Add deterministic interleaving tests for Flush completion, control enqueue,
  Stop, detach and takeover.

## P1-1: Benchmark Latency Sampling Cap

`BenchmarkLoadRunner` allocates `long[100_000]` and records only the first
100,000 publish-call latencies. High-throughput 32-B runs already exceed this in
the six-second development profile.

Consequences:

- p50/p95/p99 describe only the beginning of the measurement window;
- later backlog or degradation is invisible;
- the fixed 800-KB array bypasses the project array-pool rule.

Correction:

- Use a fixed-bucket latency histogram for p50/p95/p99, or a pooled/reservoir
  implementation with explicit precision/error documentation.
- Keep the measurement hot path allocation-free.

## P1-2: Benchmark Heartbeat Contamination

The five-second progress callback runs directly after a measured publish call.
It scans every subscriber via `CountReceived()` and synchronously writes to
standard error.

This callback is inside the measured publisher loop and can distort exactly the
p99 metric it is intended to monitor.

Correction:

- Publish progress snapshots through atomic counters.
- Render them from a separate low-frequency reporter task or the parent process.
- Do not call subscriber scans or console I/O in the measured publish loop.

## P1-3: Unconditional Shard Diagnostics Cost

`MqttZeroEventLoop` always performs priority queue counter atomics and
`_lowPriorityQueue.IsEmpty` checks on enqueue/dequeue, even when fanout
diagnostics are disabled.

Correction:

- Pass `fanoutDiagnostics.Enabled` into the Shard or a dedicated
  `trackPriorityDiagnostics` flag.
- Skip queue-depth and wait-count atomics entirely when disabled.
- Keep direct EventLoop tests diagnostic-enabled through constructor defaults.

## P1-4: Low-Priority Egress Can Starve

The high-priority queue is always drained before the low-priority queue.
Sustained Fanout work can therefore delay Egress indefinitely.

Forced fairness was rejected by its attempted performance gate, but the
starvation risk remains a correctness/progress property. It should be handled
with a lower-overhead design after benchmark and ownership fixes, not by
reintroducing the rejected implementation unchanged.

## P1-5: Outbound Benchmark Incomplete Delivery

The `outbound` diagnostic stops publishers and captures counters/received
messages without waiting for all subscribers to drain. It can report success
with `ReceivedMessages < PublishedMessages * Subscribers`.

Correction:

- Apply the same final-drain contract as `BenchmarkLoadRunner`.
- Report received/expected and drain time.

## P2 Findings

### Flush Operation Allocation

Every asynchronous Flush creates:

- a `MqttZeroEgressFlushOperation`;
- an async Task/state machine;
- a completion callback invocation on a non-Shard thread.

The previous custom awaiter attempt showed no proven improvement and was
removed. A future pooled `IValueTaskSource` or reusable completion Work must be
validated against both allocation and p99 gates.

### Session Critical Section

The batch selector holds `session.SyncRoot` while:

- scanning expiry state;
- calculating encoded lengths;
- allocating packet identifiers;
- inserting Inflight entries;
- filling the outbound list.

The measured average wait is low, but the maximum observed wait has been above
one millisecond. This remains a possible tail source under concurrent PUBACK
and reconnect activity.

### Duplicate Encode Paths

`SendPublishBatchAsync` and `TryEncodePublishBatch` contain nearly identical
encode loops. One path writes and flushes; the other writes and lets the Actor
flush. Duplicate logic increases the chance of behavior drift.

### Dead Production Type

`MqttZeroSessionOutboundPump` has only test callers. It should be removed after
its historical tests are retired or converted to the current Egress Actor.

## Verified Strengths

- No format or C# style violations were found in the final build.
- One top-level type per production file and CRLF rules pass.
- Publish decode and routing remain zero-allocation in their focused tests.
- Route Epoch and pooled Lease ownership provide a sound architectural base.
- Benchmark payload reuse and final-drain accounting are substantially better
  than their previous implementation.

## Recommended Order

1. Fix P0-1 and P0-2 with deterministic tests.
2. Fix benchmark sampling, heartbeat and outbound-drain validity.
3. Disable Shard diagnostic atomics when diagnostics are off.
4. Repeat the corrected 64-KiB development baseline.
5. Only then evaluate Flush-state ownership, Session critical-section reduction
   and low-priority progress.

