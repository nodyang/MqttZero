# MQTTZero 0.11.0-alpha.3 Memory Closure

> **Status:** Complete.
>
> **Branch:** `perf/memory-closure`
>
> **Base:** `master` at `d69f5bc`

## Goal

Make pooled memory, route retention, shared cursors, object pools and Egress
Flush allocation observable and bounded without changing public APIs.

## Implemented

- Added internal Broker, BufferPool, RoutePlanBufferPool and object-pool memory
  snapshots.
- Added the Broker Benchmark `memory` mode with fixed-message repeated waves,
  forced-GC snapshots, internal ownership counters and RSS reporting.
- Reused one Egress Flush observer per Actor. Deterministic allocation tests
  permit only the runtime continuation allocation.
- Changed `MaxBufferedBytes` to include active and cached BufferPool bytes.
  Default backing allocation no longer retains arrays outside the pool budget.
- Replaced stale Semaphore permits with waiter-count plus return-generation
  signaling.
- Added bounded RoutePlanBuffer retention by count and estimated bytes.
- Pruned inactive Shared Subscription cursors after every Route Epoch update.
- Replaced all unbounded production object pools with bounded pools.
- Capped retained control frames at 64 per connection.
- Added queued-message ownership checks and a 10-wave memory stability script.

## Verification

- Core tests, Benchmark tests, memory stability and allocation sentinels pass.
- Ten-wave QoS 0 and QoS 1 runs plateau in Broker-owned committed buffers,
  RoutePlan retention and object-pool counters. Managed heap and RSS are
  diagnostics because the MQTTnet client shares the benchmark process.
- Same-machine `master` controls show no critical cell regression above 5%.
- Performance details and raw JSON are recorded in
  `perf/baseline/memory-closure-2026-11-26.md`.

## Commits

1. `perf: reuse egress flush completion`
2. `perf: budget pooled buffer retention`
3. `perf: bound route and object retention`
4. `perf: instrument broker memory ownership`
5. `fix: reject queued message ownership reuse`
6. `test: add memory stability soak`
7. `perf: reduce memory pool accounting overhead`
8. `release: prepare MQTTZero 0.11.0-alpha.3`
9. `test: use deterministic memory benchmark waves`
