# MQTTZero Master Hot-Path Hardening Plan

> **Status:** Complete. Correctness, benchmark integrity, diagnostics gating,
> rebaseline, conditional experiment, cleanup and final verification are
> complete. Vue checks were explicitly waived by the user.
>
> **Base:** `master` at `eeb8cc6`
>
> **Baseline report:** `perf/baseline/master-hotpath-hardening-2026-11-26.md`
>
> **Source review:**
> `docs/superpowers/reviews/2026-11-26-master-hotpath-code-review.md`

## Goal

Fix correctness and measurement-integrity defects before starting another
performance redesign.

Public APIs, MQTT wire behavior, Dashboard contracts and documented defaults
remain unchanged. New supporting types are internal.

## Non-Goals

- Do not reopen Socket transport.
- Do not replace all connections with one Writer loop.
- Do not reintroduce the rejected forced-fairness or Backlog implementations.
- Do not optimize Route Trie or Encode until current benchmark defects are
  fixed.
- Do not raise the package version or make a performance claim before the final
  gates pass.

## Task 1: Fix QoS 0 Double Release

### Files

- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Delivery.cs`
- Test: `tests/MqttZero.Tests/Broker/OutboundDeliveryTests.cs`
- Test: `tests/MqttZero.Tests/Memory/MemoryLeaseAllocationTests.cs`

### TDD

- [x] Add a deterministic test where the subscriber PipeWriter is already
  completed and `BeginFlush` throws after batch encoding.
- [x] Confirm the test detects a second
  pooled-message release before the fix.

### Implementation

- [x] Track whether QoS 0 batch messages were released.
- [x] Ensure normal, Flush-throw, FlushPending and cancellation paths release
  each QoS 0 queued message exactly once.
- [x] Preserve QoS 1/2 Inflight ownership and rollback behavior.
- [x] Keep the synchronized success path allocation-free.

### Verification

- [x] Focused OutboundDelivery and Lease tests pass.
- [x] BufferPool budget returns to zero after success, failure and stop.
- [x] Allocation sentinels remain green.

### Commit

`fix: release qos0 delivery exactly once`

## Task 2: Make Egress Completion Shard-Confined

### Files

- Create: `src/MqttZero.Server/Egress/MqttZeroEgressCompletionWork.cs`
- Modify: `src/MqttZero.Server/Egress/MqttZeroEgressActor.cs`
- Modify: `src/MqttZero.Server/Egress/MqttZeroEgressScheduler.cs`
- Modify as needed:
  `src/MqttZero.Server/Broker/MqttZeroBroker.Sessions.cs`
- Test: `tests/MqttZero.Tests/Egress/*.cs`

### Required Ownership

- [x] Only the owning Shard may mutate `_controlQueue`, `_controlBatch`,
  executing/ready state and final stop state.
- [x] Flush completion performs only its completion-source bookkeeping before
  posting a reusable Work item.
- [x] Stop marks a stopping intent, then posts a Shard command.
- [x] No fire-and-forget exception path or lost wake is introduced.

### Tests

- [x] Flush completion races with Control enqueue.
- [x] Flush completion races with Stop and detach.
- [x] Repeated Stop is idempotent.
- [x] Takeover stops the old Actor before the replacement can write.
- [x] Queued control frames complete or fail exactly once.
- [x] Every queued Lease/frame is released after cancellation or failure.

### Gate

- [x] No thread-unsafe collection access outside the owning Shard.
- [x] Flush-control ordering tests remain green.
- [x] No more than five-percent regression in the corrected 64-KiB cells.
- [x] Target `QoS 1 / 100 / 64 KiB` median improved from `21.33` to
  `22.00 msg/s` in the two-run development check.

### Commit

`fix: confine egress completion and stop to the owning shard`

## Task 3: Restore Benchmark Sampling Integrity

### Files

- Modify: `benchmarks/MqttZero.BrokerBenchmarks/BenchmarkLoadRunner.cs`
- Modify: `benchmarks/MqttZero.BrokerBenchmarks/Program.cs`
- Modify: `benchmarks/MqttZero.BrokerBenchmarks/BrokerBenchmarkResult.cs`
- Test: `tests/MqttZero.BrokerBenchmarks.Tests/*.cs`

### Changes

- [x] Replace the first-100,000 sample limit with a bounded latency histogram
  or documented pooled reservoir.
- [x] Remove subscriber scans and console I/O from the measured publish loop.
- [x] Move progress rendering to a low-frequency external reporter or an atomic
  snapshot consumer.
- [x] Make the `outbound` mode wait for final subscriber drain.
- [x] Report `ReceivedMessages`, expected count and measurement drain.
- [x] Implement the two-run spread rule as executable tooling: automatically
  request a third repetition when end-to-end throughput spread exceeds ten
  percent or a run is invalid.
- [x] Ensure JSON remains source-generated.

### Tests

- [x] p99 remains correct when more than 100,000 samples are produced.
- [x] Progress callback no longer executes inside the measured publish call.
- [x] Incomplete outbound drain cannot produce a passing/successful result.
- [x] Two-run spread policy selects a third run only when required.
- [x] Short outbound smoke completed with `ReceivedMessages ==
  ExpectedReceivedMessages`.

### Commit

`fix: make broker benchmark sampling and progress trustworthy`

## Task 4: Gate Shard Priority Diagnostics

### Files

- Modify: `src/MqttZero.Server/Scheduling/MqttZeroEventLoop.cs`
- Modify: `src/MqttZero.Server/Scheduling/MqttZeroShardSet.cs`
- Test: `tests/MqttZero.Tests/Scheduling/MqttZeroEventLoopTests.cs`
- Test: `tests/MqttZero.Tests/Broker/BrokerHotPathAllocationTests.cs`

### Changes

- [x] Pass `MqttZeroFanoutDiagnostics.Enabled` into each Event Loop Shard.
- [x] Skip priority queue-depth and wait atomics when diagnostics are disabled.
- [x] Keep direct Event Loop tests diagnostic-enabled by default.
- [x] Do not gate operational counters needed for health or shutdown.

### Verification

- [x] Disabled diagnostics perform no priority Interlocked operations.
- [x] Enabled diagnostics still report queue depth, ages and wait count.
- [x] The 32-B/8-KiB development matrix completed without failures or
  incomplete deliveries.
- [x] Allocation sentinels remain green.

### Commit

`perf: disable shard priority diagnostics by default`

## Task 5: Rebaseline Before Another Egress Experiment

### Benchmark

- [x] Re-run corrected QoS 0/1/2 × 1/100/500 × 32 B/8 KiB/64 KiB development
  matrix after Tasks 1-4.
- [x] Use two repetitions; auto-run a third only for the documented spread or
  invalid-result condition.
- [x] Run the 5-second warmup / 20-second confirmation profile for
  `QoS 1 / 100 / 64 KiB`.
- [x] Record publish-window throughput, end-to-end throughput, publish-call p99,
  drain time and broker CPU/allocation.

### Decision Gate

- [x] Do not implement pooled Flush state, direct Actor follow-up, or Session
  lock extraction unless the new diagnostics show a repeatable target-cell
  benefit.
- [x] Any experiment that fails to improve the target by ten percent or regresses
  another critical cell by more than five percent is removed.

## Task 6: Select Exactly One Conditional Hot-Path Experiment

Use the rebaseline to choose one path. Multiple paths may be investigated in
diagnostics but only one production change may enter implementation at a time.

### 6A: Low-Priority Progress

Select only when queue-age diagnostics show repeated Egress waits caused by
continuous high-priority work.

- [ ] Design a lower-overhead progress mechanism different from the rejected
  eight-item forced-fairness implementation.
- [ ] Prefer deadline/age-based promotion with a strict high-priority burst or
  time bound.
- [ ] Prove that 32-B Fanout throughput does not regress by more than five
  percent.
- [ ] Prove that at least one affected 64-KiB p99 cell improves by ten percent.

### 6B: Flush Lifecycle (Selected)

Select only when per-Flush allocation or scheduling is a dominant corrected
diagnostic.

- [x] Preserve one active Flush per connection.
- [x] Use a pooled completion operation or Shard completion Work only after
  proving its allocation and p99 benefit.
- [x] Do not repeat the rejected universal Writer architecture.

The target diagnostic selected 6B: 19,656 asynchronous Flushes and 21,483
Actor drains produced only 1.67 messages per Egress batch. A reusable Flush
observer reduced custom per-Flush allocation from about 2.8 KiB to a residual
32-B runtime continuation, but target throughput moved from 22.21 to
22.08 msg/s and p99 moved from 60.93 to 61.41 ms. The experiment failed the
ten-percent improvement gate and was removed from the production path.

### 6C: Session Critical Section

Select only when maximum lock wait or Inflight insertion is a repeatable tail
contributor.

- [ ] Keep connection-generation checks under `session.SyncRoot`.
- [ ] Move encoded-length calculation and allocation-free rent work outside the
  lock where ownership remains valid.
- [ ] Revalidate Maximum Packet Size against the current connection before
  enqueue.

### Stop Rule

- [x] Remove the selected experiment if its primary metric improves by less
  than ten percent or any critical cell regresses by more than five percent.

## Task 7: Cleanup

- [x] Remove obsolete `MqttZeroSessionOutboundPump` after replacing or deleting
  its tests with current Egress Actor coverage.
- [x] Consolidate duplicate `SendPublishBatchAsync` and
  `TryEncodePublishBatch` emission logic behind one tested primitive.
- [x] Preserve no allocation in the synchronized encode/write core.

### Commit

`refactor: remove obsolete outbound send paths`

## Final Verification

- [x] Locked restore.
- [x] Release solution build.
- [x] Core, Dashboard and Benchmark tests.
- [ ] Vue tests and production build. User explicitly waived frontend
  verification.
- [x] TCP/TLS/WebSocket integration tests through the full core suite.
- [x] Allocation sentinels.
- [x] Duplicate/missing Lease-release stress.
- [x] Concurrency stress for Flush/Control/Stop/Takeover.
- [x] `dotnet format --verify-no-changes`.
- [x] NuGet vulnerability/deprecation checks; npm installation reported zero
  vulnerabilities, and frontend audit/build was waived with Vue verification.
- [x] Three-package/two-consumer package verification.
- [x] Record final benchmark JSON and a concise performance-positioning update
  in `perf/baseline/master-hotpath-hardening-2026-11-26.md`.

## Assumptions

- Correctness fixes precede new performance claims.
- Publish-call latency and end-to-end delivery remain separate metrics.
- Zero allocation does not imply zero copy.
- Development baseline uses two rounds; final claims require three.
- Kestrel remains the production transport.

