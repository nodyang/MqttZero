# MQTTZero Egress Fairness and Adaptive Send Recovery Plan

> **Status:** Proposed. No production hot-path implementation starts until the
> benchmark correction task is complete.
>
> **Date:** 2026-11-26
>
> **Branch:** `perf/egress-actor-closure`
>
> **Supersedes for execution:**
> `docs/superpowers/plans/2026-11-25-mainline-convergence-egress-closure.md`.
> The universal connection-writer experiment in that plan was completed,
> rejected, and removed.

## Goal

Recover Egress progress and tail latency without repeating the rejected
one-size-fits-all connection-writer design.

The next architecture is:

```text
one Session
  -> one serialized Egress owner
  -> adaptive Direct or Backlog execution policy
  -> fair Shard progress for Fanout and Egress work
  -> one active PipeWriter/Flush lifecycle
```

This plan first fixes benchmark semantics, then addresses possible Shard
starvation, then evaluates an adaptive send state machine. QoS 0-specific work
and larger route rewrites remain conditional.

## Evidence

- Sharded routing and the current Egress Actor remain useful foundations.
- QoS 1 / 100 subscribers / 64 KiB is close to parity:
  MQTTZero `106.92 msg/s`, p99 `15,648.2 us`; MQTTnet
  `110.07 msg/s`, p99 `20,117.5 us`.
- A complete connection writer improved that target cell to `1.055x`
  throughput and `0.707x` p99, but regressed QoS 0 and QoS 1 single-subscriber
  64-KiB throughput by about `12.1%` and `18.3%`.
- The writer regression is recorded in
  `perf/baseline/egress-writer-result-2026-11-25.md`.
- Large-payload batches average about `1.2` messages and nearly every batch
  waits on an asynchronous Flush.
- The current Event Loop always drains the high-priority queue before the
  low-priority queue. Fanout Scheduler work is high priority and Egress
  Scheduler work is low priority, so sustained Fanout traffic can delay
  subscriber delivery.
- The current Egress Actor is a scheduling adapter. Pending selection and
  Inflight insertion still live in `MqttZeroBroker.Delivery`, while encoding
  and PipeWriter writes live in `MqttZeroClientConnection`.
- The MQTTnet benchmark client still copies every payload with
  `payload.ToArray()`.
- The benchmark's recorded latency is the publisher API-call duration. It is
  not end-to-end subscriber delivery latency.
- Measurement currently stops without recording a final subscriber-drain
  duration or requiring received messages to equal published messages times
  subscriber count.

## Architecture Decisions

### Keep

- Keep Kestrel as the only production transport.
- Keep stable Session Shards and immutable Route Epochs.
- Keep Pooled Leases and one shared owned payload for QoS 1/2 fanout.
- Keep control frames and data frames on one serialized connection boundary.
- Keep no more than one active Flush per connection.
- Keep deterministic synchronized hot paths at `0 B/op` after warmup.

### Close

- Do not repeat the universal full connection-writer replacement.
- Do not reopen the direct large-payload `PipeWriter.WriteAsync` experiment.
- Do not reopen the SocketAsyncEventArgs transport experiment.
- Do not tune batch constants before fair scheduling and corrected benchmarks
  are available.
- Do not move the next phase into Topic Trie, Session-lock, or transport work
  unless corrected stage diagnostics identify those areas as dominant.

### Replace

- Replace the universal all-cell performance gate with layered gates.
- Replace exclusive high/low queue priority with bounded, fair progress.
- If still required after fairness data, replace the single Egress execution
  policy with two policies under the same owner: Direct and Backlog.

## Performance Gates

### Ordinary Release And Regression Gate

- All correctness, transport, allocation-sentinel, formatting and packaging
  checks pass.
- No same-machine benchmark cell regresses measured end-to-end throughput by
  more than five percent unless the change is rejected.
- Failed operations, drain timeouts and Lease leaks are zero.

### Performance-Claim Gate

- A performance claim is limited to the measured cells and named metric.
- A throughput claim requires three repetitions with higher median end-to-end
  throughput and no worse median publish-call p99.
- A cell is described as ahead of MQTTnet only after every advertised metric
  passes its own three-repetition gate.
- No result may describe publish-call p99 as end-to-end delivery p99.
- Universal MQTTnet superiority across every QoS, subscriber count and payload
  size is not a release requirement.

### Development Repetition Profile

- Use two repetitions for ordinary development regression checks.
- Use the median of the two runs for the provisional comparison.
- Run a third repetition as the tie-breaker when
  `abs(run1 - run2) / max(run1, run2) > 0.10` for throughput or
  `> 0.20` for publish-call or end-to-end p99.
- Record both raw runs when the two-run result passes.
- Use three repetitions for final performance claims, published benchmark
  tables and release baselines.

### Stage Stop Rules

- A scheduler or Egress implementation that fails its target improvement and
  regresses another critical cell by more than five percent is removed.
- Diagnostics and tests that preserve useful evidence may remain.
- Failed production implementations are not merged as inactive code.

## Task 1: Correct The Benchmark Contract

### Files

- Modify: `benchmarks/MqttZero.BrokerBenchmarks/MqttNetBenchmarkMqttClient.cs`
- Modify: `benchmarks/MqttZero.BrokerBenchmarks/BenchmarkLoadRunner.cs`
- Modify: `benchmarks/MqttZero.BrokerBenchmarks/BrokerBenchmarkResult.cs`
- Modify: `benchmarks/MqttZero.BrokerBenchmarks/BenchmarkSummary.cs`
- Modify: `benchmarks/MqttZero.BrokerBenchmarks/BenchmarkGateEvaluator.cs`
- Modify: benchmark tests and documentation under
  `benchmarks/MqttZero.BrokerBenchmarks` and `perf/baseline`

### Changes

- [ ] Replace `WithPayload(payload.ToArray())` with the verified MQTTnet 5.2
  `WithPayload(ReadOnlySequence<byte>)` overload, using
  `new ReadOnlySequence<byte>(payload)`.
- [ ] Keep the source payload buffer stable and reused across publishes.
- [ ] Rename the existing latency output to make clear it is the publisher
  API-call / ACK latency.
- [ ] Record publish-window throughput separately from end-to-end completion
  throughput.
- [ ] After publishing stops, wait for all subscribers to receive
  `PublishedMessages * Subscribers`, with a bounded timeout.
- [ ] Record measurement drain duration and include it in end-to-end
  throughput.
- [ ] Require measurement received count to match the advertised count before
  producing a passing result.
- [ ] Preserve zero failures and the existing bounded-pending throttle.
- [ ] Update JSON schema and gate output without silently changing the meaning
  of an existing field.

### Verification

- [ ] Unit-test the new throughput and drain calculations.
- [ ] Verify the MQTTnet adapter no longer allocates a per-publish payload
  array.
- [ ] Run one short diagnostic of each metric before a formal comparison.
- [ ] Rebuild the same-machine development baseline with two repetitions.
- [ ] Apply the spread rule and run a third repetition only when required.
- [ ] Save the corrected baseline under `perf/baseline`.

### Initial Matrix

Run these 64-KiB cells before starting production hot-path work:

```text
QoS 0 / 1 subscriber
QoS 0 / 100 subscribers
QoS 0 / 500 subscribers
QoS 1 / 1 subscriber
QoS 1 / 100 subscribers
QoS 1 / 500 subscribers
```

### Stop Condition

If the corrected client removes the reported large-payload regression or
changes the dominant bottleneck, update this plan before implementing Task 2.

### Commit

`perf: measure broker acceptance and delivery separately`

## Task 2: Guarantee Fair Shard Progress

### Files

- Modify: `src/MqttZero.Server/Scheduling/MqttZeroEventLoop.cs`
- Modify: `src/MqttZero.Server/Scheduling/MqttZeroShardDiagnostics.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroFanoutDiagnostics.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroFanoutDiagnosticsSnapshot.cs`
- Test: Event Loop, Fanout Scheduler and Egress Scheduler tests
- Benchmark or outbound diagnostics surface

### Changes

- [ ] Replace exclusive high-priority preference with a bounded fair policy.
- [ ] Give low-priority Egress at least one guaranteed progress opportunity
  while high-priority Fanout remains continuously non-empty.
- [ ] Age or promote work that has waited beyond a bounded threshold.
- [ ] Keep control-frame and data ordering unchanged.
- [ ] Add queue-age diagnostics for both priorities.
- [ ] Add a starvation/wait counter that distinguishes "high queue busy" from
  "low Egress queue absent".
- [ ] Do not change Kestrel IOQueueCount, ThreadPool settings or default Batch
  values as part of this task.

### Tests

- [ ] A continuously refilled high-priority queue cannot prevent low-priority
  execution indefinitely.
- [ ] An aged low-priority item is promoted or served within the configured
  bound.
- [ ] High-priority-only traffic does not incur an empty low-priority delay.
- [ ] Low-priority-only traffic retains current ordering.
- [ ] Control frames and data batches preserve their required boundaries.
- [ ] Shard shutdown drains or cancels both priorities deterministically.
- [ ] Queue-age and starvation counters reset correctly.

### Performance Gate

- [ ] For corrected 64-KiB QoS 0 and QoS 1 fanout cells, at least one of
  throughput or publish-call p99 improves by ten percent.
- [ ] Neither throughput nor publish-call p99 regresses by more than five
  percent in those target cells.
- [ ] QoS 1 / 100 / 64 KiB remains no worse than the corrected baseline.
- [ ] All 32-B regression cells remain within five percent.
- [ ] No new connection failures, drain timeouts or Lease leaks occur.
- [ ] Development comparisons use two repetitions unless the spread rule
  requires a third run.

### Stop Condition

If both 100- and 500-subscriber large-payload cells show no repeatable benefit,
keep only diagnostics and tests, remove the scheduling change, and proceed to
Task 3 only if corrected latency attribution still shows Egress scheduling or
Flush continuation as dominant.

### Commit

`perf: guarantee fair fanout and egress progress`

## Task 3: Add Adaptive Direct And Backlog Egress Modes

This task is conditional on Task 1 and Task 2 evidence. It must not repeat the
rejected universal connection writer.

### Files

- Modify: `src/MqttZero.Server/Egress/MqttZeroEgressActor.cs`
- Modify: `src/MqttZero.Server/Broker/MqttZeroBroker.Delivery.cs`
- Modify: `src/MqttZero.Server/Clients/MqttZeroClientConnection.cs`
- Modify: `src/MqttZero.Server/Sessions/MqttZeroSessionState.cs`
- Modify or replace: `src/MqttZero.Server/Egress/MqttZeroEgressFlushOperation.cs`
- Test: `tests/MqttZero.Tests/Egress`
- Test: `tests/MqttZero.Tests/Broker/OutboundDeliveryTests.cs`
- Test: `tests/MqttZero.Tests/Broker/BrokerHotPathAllocationTests.cs`

### Required Ownership

- [ ] The Egress Actor remains the only owner of Session outbound execution.
- [ ] Pending, Inflight, control ordering and FlushPending have one owner.
- [ ] Exactly one of Direct or Backlog mode may execute at a time.
- [ ] Switching modes cannot reorder messages or release a Lease twice.
- [ ] Control frames end the current data segment at a complete MQTT packet
  boundary before the next data packet.

### Direct Mode

- [ ] Use when the Session has low queue depth, no existing FlushPending and
  no persistent backlog.
- [ ] Avoid introducing a permanent per-connection asynchronous loop.
- [ ] Avoid adding another handoff layer around the existing low-latency path.
- [ ] Preserve immediate Flush when the queue becomes empty.

### Backlog Mode

- [ ] Enter only after measurable backlog or repeated asynchronous Flush
  waits.
- [ ] Reuse one long-lived send state and one Flush completion path while the
  Session remains backlogged.
- [ ] Continue sending multiple batches within the Actor turn only when each
  batch is already available and the configured time budget permits.
- [ ] Return to Direct mode after the backlog is removed.
- [ ] Use an Actor-owned reusable completion source instead of creating a new
  flush-completion state machine for every batch when measurement confirms
  that path contributes to allocation or tail latency.
- [ ] Never start a second Flush while one is pending.

### Tests

- [ ] Idle Session takes the Direct mode.
- [ ] Snapshot of Queue depth, batch count and Flush behavior shows the
  expected mode transitions.
- [ ] Backlogged Session disables further batch collection without changing
  order.
- [ ] Direct-to-Backlog and Backlog-to-Direct transitions preserve QoS 0/1/2
  state.
- [ ] Flush blocking, cancellation, takeover and broker stop release all
  Leases.
- [ ] A control frame waiting during a multi-batch turn is sent before the next
  data packet.
- [ ] Queue exhaustion immediately ends the current send turn.
- [ ] Synchronized enqueue, signal and reusable Flush completion paths stay at
  `0 B/op` after warmup where applicable.

### Performance Gate

- [ ] The corrected QoS 1 single-subscriber 64-KiB cell improves by at least
  ten percent over the Task 1 baseline and does not regress by more than five
  percent relative to MQTTnet.
- [ ] The corrected QoS 0 single-subscriber 64-KiB cell does not regress and
  improves by at least ten percent or Task 4 remains required.
- [ ] QoS 1 / 100 and 500 / 64 KiB do not regress by more than five percent
  from Task 2.
- [ ] At least one critical large-payload tail-latency measurement improves.
- [ ] No current small-payload cell regresses by more than five percent.
- [ ] Development comparisons use two repetitions unless the spread rule
  requires a third run.

### Stop Condition

If either single-subscriber 64-KiB cell regresses as in the rejected universal
writer experiment, remove the Backlog implementation and leave Direct mode
unchanged.

### Commit

`perf: adapt egress execution to backlog pressure`

## Task 4: Evaluate A QoS 0 Light Delivery Path

Start only if corrected QoS 0 large-payload cells remain materially behind
after Tasks 1 through 3.

### Scope

- [ ] Restrict the experiment to online QoS 0 delivery.
- [ ] Require no persistence, no observer, no enqueue interceptor and no
  retained delivery.
- [ ] Keep one shared Owned Lease for multi-subscriber fanout.
- [ ] Use a lightweight per-Session queue node without creating QoS Inflight
  state for online QoS 0 recipients.
- [ ] Evaluate a single-subscriber borrowed-input path only when the publisher
  input cannot be advanced until the target PipeWriter copy has completed.
- [ ] Keep the normal owned-lease path as the fallback for all uncertainty,
  slow-consumer and extension cases.
- [ ] Do not use a borrowed input Lease after the publisher reader advances.

### Tests

- [ ] Single-subscriber borrowed input is not advanced before the target write
  copy completes.
- [ ] Slow subscribers create bounded backpressure rather than blocked
  publishers or leaked Leases.
- [ ] Multi-subscriber fanout copies Payload into owned storage once.
- [ ] Session detach, takeover, stop and writer failure release every Lease.
- [ ] QoS 0 offline-drop behavior is unchanged.
- [ ] Queue bounds and drop semantics are unchanged.

### Gate

- [ ] QoS 0 / 1 / 64 KiB improves by at least fifteen percent.
- [ ] QoS 0 / 100 / 64 KiB improves by at least fifteen percent.
- [ ] No small-payload cell regresses by more than five percent.
- [ ] Correctness and Lease-budget tests pass.
- [ ] Development comparisons use two repetitions unless the spread rule
  requires a third run.

### Stop Condition

If ownership cannot be proved with deterministic tests, do not use the borrowed
input path. Keep only the safe shared-owned QoS 0 fanout optimization and
record the limit.

## Task 5: Final Verification And Positioning

- [ ] Run corrected publish-call and end-to-end benchmarks.
- [ ] Run the critical matrix:
  QoS `0/1/2`, subscribers `1/100/500`, payload `32/8192/65536`.
- [ ] Run QoS 2 specifically for integration confidence even if QoS 2 is not a
  performance-claim target.
- [ ] Run all core, Dashboard and Vue tests.
- [ ] Run TCP, TLS and WebSocket integration tests.
- [ ] Run allocation sentinels.
- [ ] Run Release build, formatting, vulnerability/deprecation checks, package
  verification and an outside-repository consumer build.
- [ ] Update `docs/performance-positioning.md`.
- [ ] Update `perf/baseline/README.md`.
- [ ] Record raw JSON, machine, command, repetitions and medians.
- [ ] State only the cells and metrics that passed all gates.
- [ ] Do not raise the package version or merge a performance implementation
  solely because documentation and diagnostics were completed.

## Expected Commit Sequence

1. `docs: revise egress architecture and performance gates`
2. `perf: measure broker acceptance and delivery separately`
3. `perf: guarantee fair fanout and egress progress`
4. `perf: adapt egress execution to backlog pressure` - only if Task 3 passes
5. `perf: optimize online qos0 large-payload delivery` - only if Task 4 passes
6. `docs: record final egress performance position`

## Assumptions

- Kestrel remains both the production and comparison transport.
- The same MQTTnet Client implementation is used for both Brokers.
- A network send may require one per-subscriber payload copy.
- Zero allocation does not imply zero copy.
- Publish-call latency and end-to-end delivery latency are different metrics.
- The current strict 12-cell matrix is diagnostic evidence, not a requirement
  for every future release.
- Public APIs, MQTT wire format, Dashboard contracts and configuration defaults
  remain unchanged unless a separate approved plan says otherwise.

