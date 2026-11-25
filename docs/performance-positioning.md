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

The current Egress Actor does not yet own the complete Session-to-PipeWriter
send loop. The approved next experiment is a single, time-boxed connection
Egress closure. It must improve the primary QoS 1 / 100-subscriber / 64 KiB
cell without regressing critical cells by more than five percent. If it does
not meet that gate, the experiment is removed and the strict performance matrix
remains a long-term research target.
