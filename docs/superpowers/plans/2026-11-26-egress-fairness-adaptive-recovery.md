# MQTTZero Egress Fairness and Adaptive Send Recovery Plan

> **Status:** Closed. Task 1 delivered Benchmark corrections; Tasks 2 and 3
> were rejected after benchmark; Task 4 was not justified by the remaining cost
> model. Engineering verification passed without a performance claim.
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
- Run a third repetition as the tie-breaker when end-to-end throughput spread
  exceeds ten percent: `abs(run1 - run2) / max(run1, run2) > 0.10`.
- Also run a third repetition when either run has failures, incomplete
  delivery or a measurement-drain timeout.
- Record publish-call p99 for development, but do not use short-run tail noise
  as a third-repetition trigger.
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

- [x] Replace `WithPayload(payload.ToArray())` with the verified MQTTnet 5.2
  `WithPayload(ReadOnlySequence<byte>)` overload, using
  `new ReadOnlySequence<byte>(payload)`.
- [x] Keep the source payload buffer stable and reused across publishes.
- [x] Rename the existing latency output to make clear it is the publisher
  API-call / ACK latency.
- [x] Record publish-window throughput separately from end-to-end completion
  throughput.
- [x] After publishing stops, wait for all subscribers to receive
  `PublishedMessages * Subscribers`, with a bounded timeout.
- [x] Record measurement drain duration and include it in end-to-end
  throughput.
- [x] Require measurement received count to match the advertised count before
  producing a passing result.
- [x] Preserve zero failures and the existing bounded-pending throttle.
- [x] Fix the measurement publishing loop to use its own publish counter for
  `max-pending`; the previous loop read the reset warmup counter.
- [x] Emit per-cell start, connection, warmup, measurement, drain and completion
  progress without mixing text into JSON output.
- [x] Update JSON schema and gate output without silently changing the meaning
  of an existing field.

### Verification

- [x] Unit-test the new throughput and drain calculations.
- [x] Verify the MQTTnet adapter no longer allocates a per-publish payload
  array.
- [x] Run one short diagnostic of each metric before a formal comparison.
- [x] Rebuild the same-machine development baseline with two repetitions.
- [x] Apply the spread rule and run a third repetition only when required.
- [x] Save the corrected baseline under `perf/baseline`.

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

Development profile:

```powershell
--client mqttnet --qos 0,1 --subscribers 1,100,500 --payload 65536 `
  --warmup 2 --seconds 6 --repetitions 2 --max-pending 1024
```

Run a third repetition only when the development spread rule requires it.

### Corrected Baseline Result

- Raw two-run report:
  `perf/baseline/egress-corrected-development-2026-11-26.json`.
- Consolidated report:
  `perf/baseline/egress-corrected-development-2026-11-26.md`.
- QoS 0 / 1: MQTTZero `969.48`, MQTTnet `837.87` end-to-end msg/s;
  p99 `4,721.7 / 2,959.8 us`.
- QoS 0 / 100: MQTTZero `32.04`, MQTTnet `28.88` msg/s;
  p99 `38,023.4 / 58,377.6 us`.
- QoS 0 / 500: MQTTZero `5.41`, MQTTnet `5.20` msg/s;
  p99 `28,083.0 / 19,435.6 us`.
- QoS 1 / 1: MQTTZero `932.91`, MQTTnet `907.17` msg/s;
  p99 `9,856.2 / 6,401.0 us`.
- QoS 1 / 100: MQTTZero `21.33`, MQTTnet `22.95` msg/s;
  p99 `70,747.4 / 61,658.4 us`.
- QoS 1 / 500: MQTTZero `4.55`, MQTTnet `4.49` msg/s;
  p99 `255,783.4 / 304,216.1 us`.
- All runs had zero failures and complete delivery.

### Stop Condition

Completed. The corrected client materially changed the bottleneck attribution:
QoS 0 high-fanout throughput was previously understated by measurement
backlog, while QoS 1 / 100 / 64 KiB is now the clear throughput red cell.
Fair Shard progress remains justified as an architecture safety requirement,
but it no longer carries a universal expectation of a ten-percent high-fanout
throughput gain.

### Commit

`perf: measure broker acceptance and delivery separately`

## Task 2: Guarantee Fair Shard Progress

> **Status: Rejected.** The bounded forced-fairness implementation was removed
> after two-round benchmarking. Diagnostics and tests were retained. Evidence:
> `perf/baseline/egress-fairness-result-2026-11-26.md`.

### Files

- Modify: `src/MqttZero.Server/Scheduling/MqttZeroEventLoop.cs`
- Modify: `src/MqttZero.Server/Scheduling/MqttZeroShardDiagnostics.cs`
- Test: Event Loop, Fanout Scheduler and Egress Scheduler tests
- Benchmark: `perf/baseline/egress-fairness-qos01-100-500-64k-2026-11-26.json`

### Prototype Result

- [x] Prototype a bounded fair policy and verify it through deterministic
  starvation tests.
- [x] Keep control-frame, data and intra-priority ordering unchanged.
- [x] Confirm QoS 0/100 and QoS 1/500 regressed by more than five percent.
- [x] Confirm no affected cell gained ten-percent throughput.
- [x] Confirm QoS 1/100 gained 5.7% throughput and 22.4% p99, but not enough to
  offset the rejected cells.
- [x] Remove forced priority switching after the performance gate failed.
- [x] Retain high-priority and low-priority queue depth, oldest-work age and
  low-priority wait diagnostics.
- [x] Keep Kestrel IOQueueCount, ThreadPool settings and Batch defaults
  unchanged.
- [x] Confirm no connection failures, drain timeouts or Lease leaks.

### Follow-up Attribution

- [x] Refresh `QoS 1 / 100 / 64 KiB` outbound diagnostics after reverting the
  scheduling change.
- [x] Confirm every data batch waited on asynchronous Flush.
- [x] Confirm average egress batch size remains below two messages.
- [x] Proceed to Task 3 because Flush continuation and the Egress send state
  remain measured dominant costs.

### Commit

`perf: instrument shard priority wait`

## Task 3: Add Adaptive Direct And Backlog Egress Modes

> **Status: Rejected.** Backlog continuation and Flush-completion reuse were
> removed after long-run comparisons showed no retained improvement. Evidence:
> `perf/baseline/egress-adaptive-send-result-2026-11-26.md`.

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

- [x] Prototype one Egress owner with Direct/Backlog execution policies.
- [x] Preserve Pending, Inflight, control and FlushPending ordering during the
  prototype.
- [x] Verify control frames remain at complete packet boundaries.
- [x] Remove the production prototype after its performance gate failed.
- [x] Keep the original Direct execution path as the production behavior.
- [x] Verify lower queue depth uses the original Direct path.
- [x] Enter the prototype Backlog path only when a prior Drain reports both
  remaining data and continuable QoS 1/2 backlog.
- [x] Limit one Actor execution to four Backlog data batches or 250 us.
- [x] Return to Direct execution when the queue drains or the next batch is not
  continuable.
- [x] Never start a second Flush while one is pending.
- [x] Revert the reusable Flush awaiter after it did not provide a retained
  benefit.

### Tests

- [x] Direct execution remained the idle and non-continuable QoS 0 path.
- [x] Backlogged QoS 1 execution processed multiple batches in one Actor turn.
- [x] The four-batch budget rescheduled pending work.
- [x] Flush blocking, cancellation, takeover and broker stop release all
  Leases.
- [x] Queue exhaustion immediately ended the current send turn.

### Performance Gate

- [x] Measured QoS 1 single-subscriber 64-KiB: no durable ten-percent gain.
- [x] Measured QoS 0 single-subscriber 64-KiB: no durable ten-percent gain.
- [x] Measured QoS 1 / 100 and 500 / 64 KiB: short-run gains did not survive
  the 5-second warmup / 20-second measurement comparison.
- [x] No durable critical large-payload tail-latency improvement remained.
- [x] No current small-payload cell regressed by more than five percent.
- [x] Development comparisons used two repetitions.
- [x] A five-second warmup and twenty-second confirmation run was used before
  rejecting the short-run result.

### Stop Condition

Completed: no durable improvement remained, so the Backlog implementation and
Flush-completion reuse were removed.

### Commit

`perf: record adaptive send experiment`

## Task 4: Evaluate A QoS 0 Light Delivery Path

Start only if corrected QoS 0 large-payload cells remain materially behind
after Tasks 1 through 3.

> **Status: Not started.** The longer comparison left QoS 0 fanout around
> `0.93–0.95x` MQTTnet, while the measured 64-KiB copy lower bound is about
> `1.17 us` versus a p99 measured in tens of milliseconds. The borrowed-input
> ownership risk is not justified. Evidence:
> `perf/baseline/egress-adaptive-send-result-2026-11-26.md`.

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

- [x] Run corrected publish-call and end-to-end development benchmarks.
- [x] Run the six-cell 64-KiB critical baseline used by the experiments.
- [x] Leave the full 27-cell performance-claim matrix unrun because no
  production performance experiment passed its gate.
- [x] Retain QoS 2 correctness coverage through the core suite.
- [x] Run all core, Dashboard and Vue tests.
- [x] Run TCP, TLS and WebSocket integration tests through the core suite.
- [x] Run allocation sentinels.
- [x] Run Release build, formatting, vulnerability/deprecation checks, package
  verification and an outside-repository consumer build.
- [x] Update `docs/performance-positioning.md` during Task 1.
- [x] Update `perf/baseline/README.md`.
- [x] Record raw JSON, machine, commands, repetitions and medians for the
  executed development experiments.
- [x] State only the cells and metrics that passed their gates.
- [x] Do not raise the package version or merge a performance implementation
  solely because documentation and diagnostics were completed.
- [x] Close the investigation in `perf/baseline/egress-closure-final-2026-11-26.md`.

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

