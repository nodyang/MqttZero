# MQTTZero Master Hot-Path Hardening Plan

> **Status:** Active. Tasks 1-2 are complete; Task 3 is next.
>
> **Base:** `master` at `1a6b71e`
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

- [ ] Replace the first-100,000 sample limit with a bounded latency histogram
  or documented pooled reservoir.
- [ ] Remove subscriber scans and console I/O from the measured publish loop.
- [ ] Move progress rendering to a low-frequency external reporter or an atomic
  snapshot consumer.
- [ ] Make the `outbound` mode wait for final subscriber drain.
- [ ] Report `ReceivedMessages`, expected count and measurement drain.
- [ ] Implement the two-run spread rule as executable tooling: automatically
  request a third repetition when end-to-end throughput spread exceeds ten
  percent or a run is invalid.
- [ ] Ensure JSON remains source-generated.

### Tests

- [ ] p99 remains correct when more than 100,000 samples are produced.
- [ ] Progress callback does not execute inside the measured publish call.
- [ ] Incomplete outbound drain cannot produce a passing/successful result.
- [ ] Two-run spread policy selects a third run only when required.

### Commit

`fix: make broker benchmark sampling and progress trustworthy`

## Task 4: Gate Shard Priority Diagnostics

### Files

- Modify: `src/MqttZero.Server/Scheduling/MqttZeroEventLoop.cs`
- Modify: `src/MqttZero.Server/Scheduling/MqttZeroShardSet.cs`
- Test: `tests/MqttZero.Tests/Scheduling/MqttZeroEventLoopTests.cs`
- Test: `tests/MqttZero.Tests/Broker/BrokerHotPathAllocationTests.cs`

### Changes

- [ ] Pass `MqttZeroFanoutDiagnostics.Enabled` into each Event Loop Shard.
- [ ] Skip priority queue-depth and wait atomics when diagnostics are disabled.
- [ ] Keep direct Event Loop tests diagnostic-enabled by default.
- [ ] Do not gate operational counters needed for health or shutdown.

### Verification

- [ ] Disabled diagnostics perform no priority Interlocked operations.
- [ ] Enabled diagnostics still report queue depth, ages and wait count.
- [ ] 32-B and 8-KiB hot-path benchmarks do not regress.
- [ ] Allocation sentinels remain green.

### Commit

`perf: disable shard priority diagnostics by default`

## Task 5: Rebaseline Before Another Egress Experiment

### Benchmark

- [ ] Re-run corrected QoS 0/1/2 × 1/100/500 × 32 B/8 KiB/64 KiB development
  matrix after Tasks 1-4.
- [ ] Use two repetitions; auto-run a third only for the documented spread or
  invalid-result condition.
- [ ] Run the 5-second warmup / 20-second confirmation profile for
  `QoS 1 / 100 / 64 KiB`.
- [ ] Record publish-window throughput, end-to-end throughput, publish-call p99,
  drain time and broker CPU/allocation.

### Decision Gate

- [ ] Do not implement pooled Flush state, direct Actor follow-up, or Session
  lock extraction unless the new diagnostics show a repeatable target-cell
  benefit.
- [ ] Any experiment that fails to improve the target by ten percent or regresses
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

### 6B: Flush Lifecycle

Select only when per-Flush allocation or scheduling is a dominant corrected
diagnostic.

- [ ] Preserve one active Flush per connection.
- [ ] Use a pooled completion operation or Shard completion Work only after
  proving its allocation and p99 benefit.
- [ ] Do not repeat the rejected universal Writer architecture.

### 6C: Session Critical Section

Select only when maximum lock wait or Inflight insertion is a repeatable tail
contributor.

- [ ] Keep connection-generation checks under `session.SyncRoot`.
- [ ] Move encoded-length calculation and allocation-free rent work outside the
  lock where ownership remains valid.
- [ ] Revalidate Maximum Packet Size against the current connection before
  enqueue.

### Stop Rule

- [ ] Remove the selected experiment if its primary metric improves by less
  than ten percent or any critical cell regresses by more than five percent.

## Task 7: Cleanup

- [ ] Remove obsolete `MqttZeroSessionOutboundPump` after replacing or deleting
  its tests with current Egress Actor coverage.
- [ ] Consolidate duplicate `SendPublishBatchAsync` and
  `TryEncodePublishBatch` emission logic behind one tested primitive.
- [ ] Preserve no allocation in the synchronized encode/write core.

### Commit

`refactor: remove obsolete outbound send paths`

## Final Verification

- [ ] Locked restore.
- [ ] Release solution build.
- [ ] Core, Dashboard and Benchmark tests.
- [ ] Vue tests and production build.
- [ ] TCP/TLS/WebSocket integration tests.
- [ ] Allocation sentinels.
- [ ] Duplicate/missing Lease-release stress.
- [ ] Concurrency stress for Flush/Control/Stop/Takeover.
- [ ] `dotnet format --verify-no-changes`.
- [ ] NuGet vulnerability/deprecation and npm audit.
- [ ] Three-package/two-consumer package verification.
- [ ] Record final benchmark JSON and a concise performance-positioning update.

## Assumptions

- Correctness fixes precede new performance claims.
- Publish-call latency and end-to-end delivery remain separate metrics.
- Zero allocation does not imply zero copy.
- Development baseline uses two rounds; final claims require three.
- Kestrel remains the production transport.

