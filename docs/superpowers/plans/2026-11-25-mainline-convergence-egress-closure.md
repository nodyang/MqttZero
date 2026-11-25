# MQTTZero Egress Performance Reset Implementation Plan

> **Status: CLOSED - HISTORICAL.**
>
> Do not execute Tasks 4 through 6. The complete connection writer in Task 4
> was implemented and measured, but it was rejected because single-subscriber
> 64-KiB cells regressed by more than five percent. The production
> implementation was removed.
>
> Continue with:
> `docs/superpowers/plans/2026-11-26-egress-fairness-adaptive-recovery.md`.

> **Historical execution note:** This plan is retained for provenance and must
> not be resumed. Use the superseding plan for all new work.

**Historical goal:** 清除当时 Egress 实验中的无效中间抽象，建立同口径基线，
用证据确定瓶颈，并且只执行一次完整的 Connection Writer 纵向替换。

**Architecture:** Stage 1 已合入 `master` 并发布 `0.11.0-alpha.2`。本计划不再将“所有权容器、选批器、Writer”拆成互相依赖的半成品；先回到“现有发送路径 + 诊断能力”的干净基座，只有归因证明 Writer 是瓶颈时，才用一个完整状态机替换旧发送路径。

**Tech Stack:** .NET 10, Kestrel PipeWriter, MQTT 5, Event Loop Shards, MQTTnet 5.2.0.1603, xUnit v3, BenchmarkDotNet, PowerShell 7.

**Spec:** `docs/sharded-actor-broker-plan.md`, `perf/baseline/egress-closure-baseline-2026-11-25.md`, and `perf/baseline/egress-batch-selector-experiment-2026-11-25.md`.

## Global Constraints

- Work directly on branch `perf/egress-actor-closure`; do not create worktrees.
- Do not change public APIs, `MqttZeroOptions` defaults, MQTT wire format, Dashboard, management APIs, interceptors, observers or storage contracts.
- Kestrel remains the production transport.
- Preserve one active writer per connection, QoS ordering, QoS 1/2 inflight state, control-frame priority and full MQTT packet boundaries.
- Preserve zero allocation after warmup for deterministic synchronized MQTTZero hot paths.
- Do not add speculative helper layers without a production caller.
- Do not reuse the rejected batch selector or multi-batch Actor experiment.
- Do not compare a three-repetition baseline with a single short diagnostic run.
- Any performance experiment must be reverted if it fails its gate.
- C# files use CRLF, one top-level type per file, XML comments on public APIs, mandatory braces and `csharp-code-rules`.
- Run commands with PowerShell 7 from `D:\github\mqttzero`.

---

## Current State

- `master` contains the mainline convergence and `0.11.0-alpha.2`.
- Current branch: `perf/egress-actor-closure`.
- Core tests currently pass: `580/580`.
- Target baseline, three repetitions, QOS 1 / 100 subscribers / 64 KiB:
  - MQTTZero: `106.92 msg/s`, p99 `15,648.2 us`.
  - MQTTnet: `110.07 msg/s`, p99 `20,117.5 us`.
- Throughput gap is about 2.9%; MQTTZero p99 is already better than MQTTnet.
- Send-stage attribution shows:
  - Session lock average wait is below 1 us.
  - Inflight update is below 1 us.
  - Average egress batch is about 1.2 messages.
  - Flush waits are approximately equal to data batches.
  - Encoding is measurable but not the sole cause.
- Rejected experiments:
  - Four batches per Actor turn: no improvement; reverted.
  - Extracted publish-batch selector: target regressed; reverted.
- Completed in this execution:
  - Task 1 cleanup verified with `578/578` core tests and `22/22` allocation sentinels.
  - Task 3 selected the writer/flush hypothesis from the 20-second diagnostic.
  - A complete connection writer reached `1.055x` target throughput and `0.707x`
    target p99 against MQTTnet.
  - The writer was rejected because single-subscriber 64 KiB throughput fell
    more than five percent. Evidence is in
    `perf/baseline/egress-writer-result-2026-11-25.md`.
- Known plan defects to remove:
  - `MqttZeroConnectionEgress` is currently only a wrapper and does not own the send loop.
  - `MqttZeroEgressOwnership` has no production caller.
  - The previous Task 13 referenced a publish-batch type that was deleted.

---

## Task 1: Return to a Clean Diagnostic Baseline

**Files:**
- Delete: `src/MqttZero.Server/Egress/IMqttZeroEgressOwner.cs`
- Delete: `src/MqttZero.Server/Egress/MqttZeroEgressOwnership.cs`
- Delete: `src/MqttZero.Server/Egress/MqttZeroConnectionEgress.cs`
- Delete: `tests/MqttZero.Tests/Egress/ConnectionEgressOwnershipTests.cs`
- Delete: `tests/MqttZero.Tests/Egress/MqttZeroConnectionEgressTests.cs`
- Modify: `src/MqttZero.Server/Sessions/MqttZeroSessionState.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker*.cs`
- Test: `tests/MqttZero.Tests/Broker/OutboundDeliveryTests.cs`
- Test: `tests/MqttZero.Tests/Broker/ConnectedClientUnitTests.cs`

**Interfaces:**
- `MqttZeroSessionState.Egress` returns to `MqttZeroEgressActor?`.
- `MqttZeroBroker.StartEgress` returns `MqttZeroEgressActor`.
- Keep Task 10 diagnostics and the full Egress test suite.

- [ ] **Step 1: Record the pre-cleanup test baseline**

  Run:

  ```powershell
  dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore
  ```

  Expected: all current tests pass before removing the wrapper.

- [ ] **Step 2: Remove the unused ownership wrapper**

  Replace `MqttZeroConnectionEgress` with `MqttZeroEgressActor` at the Session
  and Broker lifecycle boundaries. Remove the two scaffold types and their
  tests. Keep `MqttZeroEgressActorTests`, `MqttZeroEgressFlushTests`,
  `MqttZeroEgressSchedulerTests` and `MqttZeroEgressBatchBudgetTests`.

- [ ] **Step 3: Verify the cleaned baseline**

  Run:

  ```powershell
  dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore `
    --filter "FullyQualifiedName~Egress|FullyQualifiedName~OutboundDelivery|FullyQualifiedName~BrokerLifecycle"
  ```

  Expected: all selected tests pass and every remaining Egress type has a
  production caller.

- [ ] **Step 4: Run the allocation sentinel**

  ```powershell
  ./perf/verify-allocation.ps1
  ```

  Expected: zero failures and zero allocation in all sentinel tests.

- [ ] **Step 5: Commit**

  ```powershell
  git add src/MqttZero.Server tests/MqttZero.Tests
  git commit -m "refactor: remove unused egress ownership scaffolding"
  ```

## Task 2: Re-establish an Apples-to-Apples Baseline

**Files:**
- Create: `perf/baseline/egress-reset-qos1-100-64k-2026-11-25.json`
- Modify: `perf/baseline/egress-closure-baseline-2026-11-25.md`

**Interfaces:**
- Produces: a three-repetition baseline measured after Task 1 cleanup.
- Produces: the only reference used by later performance decisions.

- [ ] **Step 1: Build the benchmark with an isolated output directory**

  ```powershell
  dotnet build benchmarks/MqttZero.BrokerBenchmarks/MqttZero.BrokerBenchmarks.csproj `
    -c Release --no-restore -p:BaseOutputPath=artifacts/egress-reset-bench/
  ```

  Expected: build succeeds with zero warnings and zero errors.

- [ ] **Step 2: Run the exact target comparison**

  ```powershell
  dotnet artifacts/egress-reset-bench/Release/net10.0/MqttZero.BrokerBenchmarks.dll compare `
    --client mqttnet --qos 1 --subscribers 100 --payload 65536 `
    --warmup 5 --seconds 20 --repetitions 3 --max-pending 1024 `
    --start-port 59000 `
    --output perf/baseline/egress-reset-qos1-100-64k-2026-11-25.json
  ```

  Expected: JSON contains six independent Broker runs and three median
  summaries.

- [ ] **Step 3: Run the diagnostic companion**

  ```powershell
  dotnet artifacts/egress-reset-bench/Release/net10.0/MqttZero.BrokerBenchmarks.dll outbound `
    --diagnostics true --qos 1 --subscribers 100 --payload 65536 `
    --warmup 5 --seconds 20 --max-pending 1024 `
    --port 59100
  ```

  This result is supplementary and must not be compared directly with the
  three-repetition Broker median.

- [ ] **Step 4: Apply the stopping decision**

  Stop the production-code work and record the baseline as complete when both
  conditions are true:

  ```text
  MQTTZero throughput >= MQTTnet throughput * 0.99
  MQTTZero p99 <= MQTTnet p99
  ```

  The remaining difference is measurement noise or a non-priority optimization.
  Continue to Task 3 only when the throughput gap is reproducible and greater
  than one percent.

- [ ] **Step 5: Commit the measured baseline**

  ```powershell
  git add perf/baseline
  git commit -m "docs: refresh egress performance baseline"
  ```

## Task 3: Attribute the Remaining Throughput Gap

**Files:**
- Modify: `perf/baseline/egress-closure-baseline-2026-11-25.md`
- Read: `src/MqttZero.Server/Broker/MqttZeroBroker.Delivery.cs`
- Read: `src/MqttZero.Server/Egress/MqttZeroEgressActor.cs`
- Read: `src/MqttZero.Server/Clients/MqttZeroClientConnection.cs`

**Interfaces:**
- Consumes: Task 2 JSON and outbound diagnostics.
- Produces: one selected hypothesis with measured evidence.

- [ ] **Step 1: Record the stage ratios**

  Record these values for the target run:

  ```text
  Total publish time
  Session lock wait
  Pending selection
  Inflight update
  Encode time
  Flush wait count
  Data batch count
  Average messages per batch
  Egress scheduler passes
  Egress drain count
  ```

- [ ] **Step 2: Select exactly one primary hypothesis**

  Use this decision table:

  ```text
  Encode average > 20% of publish p50
    -> encoder/writer-copy hypothesis

  Flush waits > 80% of data batches and average batch < 2
    -> per-message writer/flush hypothesis

  Scheduler passes > drains / 2
    -> scheduler/wake hypothesis

  Session lock average > 5 us or Inflight average > 5 us
    -> session-state hypothesis

  None of the above and throughput gap <= 1%
    -> stop performance work
  ```

- [ ] **Step 3: Reject unsupported work**

  If the selected hypothesis is not writer/flush, do not proceed to Task 4.
  The next plan must target the measured encoder, scheduler or session-state
  hotspot instead.

- [ ] **Step 4: Commit the attribution**

  ```powershell
  git add perf/baseline/egress-closure-baseline-2026-11-25.md
  git commit -m "docs: attribute remaining egress gap"
  ```

## Task 4: CLOSED - Complete Connection Writer Slice

> Closed on 2026-11-26. The implementation and gate result are recorded in
> `perf/baseline/egress-writer-result-2026-11-25.md`. Do not execute this task
> again as a universal replacement.

The following precondition and file list record the historical implementation
that was actually executed. They are not current instructions.

**Files:**
- Create: `src/MqttZero.Server/Egress/MqttZeroConnectionWriter.cs`
- Create: `src/MqttZero.Server/Egress/MqttZeroConnectionWriterState.cs`
- Modify: `src/MqttZero.Server/Egress/MqttZeroEgressActor.cs`
- Modify: `src/MqttZero.Server/Clients/MqttZeroClientConnection.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Delivery.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Connections.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Sessions.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Lifecycle.cs`
- Test: `tests/MqttZero.Tests/Egress/*.cs`
- Test: `tests/MqttZero.Tests/Broker/OutboundDeliveryTests.cs`
- Test: `tests/MqttZero.Tests/Broker/BrokerLifecycleTests.cs`

**Interfaces:**
- Produces: `MqttZeroConnectionWriter(MqttZeroSessionState, MqttZeroClientConnection, MqttZeroEgressScheduler, MqttZeroFanoutDiagnostics?)`.
- Produces: one public-to-assembly method `DrainAsync(CancellationToken)` returning `ValueTask<bool>`.
- Consumes: existing Session Pending/Inflight state and existing connection encode/write methods.
- Does not produce a second compatibility path.

- [ ] **Step 1: Add deterministic lifecycle tests**

  Add tests for:

  ```text
  Control frame waiting ends the current data batch.
  Synchronous Flush continues the same writer turn.
  Asynchronous Flush suspends data until completion.
  No second Flush starts while one is pending.
  Queue exhaustion does not reschedule.
  Signal during Flush is preserved.
  Stop and Takeover stop the writer before replacement.
  ```

- [ ] **Step 2: Implement the complete writer**

  The writer owns data selection, expiry, Packet Identifier allocation,
  Inflight insertion, encoding, PipeWriter writing and Flush lifecycle.
  `MqttZeroEgressActor` becomes only the Shard scheduling adapter for this
  writer. Control frames are written only at full MQTT packet boundaries.

- [ ] **Step 3: Replace the old path atomically**

  `StartEgress` constructs `MqttZeroConnectionWriter`. Delete
  `DrainQueuedMessagesOnceAsync` from the Broker so there is one production
  send path. Do not retain the wrapper or a feature flag.

- [ ] **Step 4: Run focused correctness tests**

  ```powershell
  dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore `
    --filter "FullyQualifiedName~Egress|FullyQualifiedName~OutboundDelivery|FullyQualifiedName~BrokerLifecycle|FullyQualifiedName~Qos"
  ```

  Expected: all tests pass.

- [ ] **Step 5: Run allocation and lifecycle stress**

  ```powershell
  ./perf/verify-allocation.ps1
  dotnet test tests/MqttZero.Tests/MqttZero.Tests.csproj -c Release --no-restore `
    --filter "FullyQualifiedName~Reconnect|FullyQualifiedName~Lease|FullyQualifiedName~Takeover"
  ```

  Expected: allocation sentinels pass and no Lease remains after lifecycle
  stress.

- [ ] **Step 6: Commit**

  ```powershell
  git add src/MqttZero.Server tests/MqttZero.Tests
  git commit -m "perf: replace broker drain with connection writer"
  ```

## Task 5: CLOSED - Unified Writer Gate

> The gate ran and rejected the implementation. Keep the recorded result; do
> not treat the unchecked historical steps below as pending work.

**Files:**
- Create: `perf/baseline/egress-writer-result-2026-11-25.json`
- Create: `perf/baseline/egress-writer-result-2026-11-25.md`

**Interfaces:**
- Consumes: the same three-repetition configuration used in Task 2.
- Produces: keep/revert decision.

- [ ] **Step 1: Run the target comparison**

  ```powershell
  dotnet artifacts/egress-reset-bench/Release/net10.0/MqttZero.BrokerBenchmarks.dll compare `
    --client mqttnet --qos 1 --subscribers 100 --payload 65536 `
    --warmup 5 --seconds 20 --repetitions 3 --max-pending 1024 `
    --start-port 59200 `
    --output perf/baseline/egress-writer-result-2026-11-25.json
  ```

- [ ] **Step 2: Run the critical regression matrix**

  ```powershell
  dotnet artifacts/egress-reset-bench/Release/net10.0/MqttZero.BrokerBenchmarks.dll compare `
    --client mqttnet --qos 0,1 --subscribers 1,100 `
    --payload 32,8192,65536 --warmup 3 --seconds 10 `
    --repetitions 3 --max-pending 1024 --start-port 59300 `
    --output perf/baseline/egress-writer-regression-2026-11-25.json
  ```

- [ ] **Step 3: Apply the merge gate**

  Keep the writer only if all conditions are true:

  ```text
  Target throughput >= MQTTnet throughput
  Target p99 <= MQTTnet p99
  Target throughput improves by at least 3% over Task 2 MQTTZero baseline
  Every other single-subscriber and 100-subscriber cell regresses by at most 5%
  Failed operations, drain timeouts and lease leaks are zero
  ```

- [ ] **Step 4: Revert on failure**

  If any gate fails:

  ```powershell
  git revert --no-edit <connection-writer-commit>
  ```

  Record the failed result and stop performance work on this branch. Do not
  start another speculative rewrite.

- [ ] **Step 5: Commit a passing result**

  ```powershell
  git add perf/baseline
  git commit -m "perf: validate connection writer"
  ```

## Task 6: CLOSED - Historical Branch Completion

> Superseded by the fairness and adaptive-send recovery plan. This section is
> retained only for project history.

**Files:**
- Modify: `README.md`
- Modify: `docs/performance-positioning.md`
- Modify: `perf/baseline/README.md`

**Interfaces:**
- Produces: final branch decision without changing production APIs.

- [ ] **Step 1: Complete a passing branch**

  Run locked restore, Release build, all core tests, Dashboard tests, Vue
  tests, allocation sentinels, format, package verification and an outside-
  repository consumer build. Merge to `master` only after every gate passes.

- [ ] **Step 2: Complete a failed branch**

  Keep this branch unmerged, remove the failed writer implementation, preserve
  the failed benchmark report and update documentation to state the measured
  limit.

- [ ] **Step 3: Final verification**

  ```powershell
  git status --short --branch
  git log --oneline -8
  ```

  Expected: clean worktree and an intentional branch decision.
