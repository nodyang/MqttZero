# MQTTZero Performance Positioning

MQTTZero is an MQTT 5 broker for ASP.NET Core Kestrel. Its release quality is
defined by protocol correctness, bounded resources, zero allocation in selected
synchronized hot paths and stable operation.

## Supported Strengths

- MQTT 5 protocol completeness, including QoS 1/2, sessions, retained messages,
  wills, message expiry and property semantics.
- Zero-allocation fast paths for PUBLISH decode, route planning, QoS queues,
  acknowledgement decoding and memory transport.
- Shared reference-counted payload ownership and bounded pooled memory.
- Kestrel TCP, TLS and WebSocket integration with ASP.NET Core dependency
  injection.
- Routing and retained-message indexes designed for subscription rebuilds.

## Release Gate

An ordinary alpha release requires all correctness tests, allocation sentinels,
real transport tests and the Dashboard tests to pass. It also requires that no
benchmark cell regress throughput by more than five percent from the
same-machine stable baseline, with zero failed operations, connection failures,
drain timeouts or lease leaks.

## Performance-Claim Gate

The nightly comparison against MQTTnet is intentionally stricter. A cell may be
described as ahead of MQTTnet only after three repetitions show higher median
throughput and no worse p99. The strict matrix does not block an ordinary alpha
release.

Known gaps include large-payload high-fanout throughput and tail latency. Those
cells are recorded in `perf/baseline` and are not implied by the general
zero-allocation claim.

## Next Architecture Step

The complete connection-writer experiment was measured and rejected. It
improved QoS 1 / 100 subscribers / 64 KiB, but regressed single-subscriber
64-KiB QoS 0 and QoS 1 throughput beyond the five-percent gate. The production
implementation was removed.

The next track is defined in
`docs/superpowers/plans/2026-11-26-egress-fairness-adaptive-recovery.md`:

1. Correct the benchmark payload-copy and delivery-drain contract.
2. Guarantee fair progress between high-priority Fanout work and low-priority
   Egress work.
3. Only if measured evidence still requires it, add Direct and Backlog modes
   under one Egress owner instead of replacing every path with one Writer.

Development regression checks may use two repetitions to keep iteration cost
bounded. Final performance claims and published comparison matrices still
require three repetitions. A two-run development sample must be repeated with
a third run only when end-to-end throughput spread exceeds ten percent or a
run has failures or incomplete delivery. P99 is recorded in development but
does not trigger a third run because short-run tail noise is too high.

Universal MQTTnet superiority across every benchmark cell remains outside the
ordinary release gate. Performance claims are limited to measured cells and
named metrics.
