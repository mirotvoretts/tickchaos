# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-08-10

First release. Protocol-aware UDP degradation proxy for market-data feed handlers.

### Added

- Deterministic fault engine driven by a seeded PRNG. One seed reproduces a run
  bit-for-bit; the seed is logged on every run and printed in the shutdown report.
- Toxics: `drop`, `duplicate`, `jitter`, `reorder`, `rate_limit` (token bucket),
  `drop_seq`, `gap_burst`. Operators are applied in scenario order and the first one
  that does not forward wins.
- Protocol awareness: MoldUDP64 sequence extraction (NASDAQ ITCH transport), enabling
  `drop_seq` and `gap_burst` to target exact sequence numbers. Packets whose sequence
  number cannot be parsed are always forwarded untouched.
- Real UDP transport built on `socket2`: multicast join, `SO_REUSEADDR`, configurable
  `SO_RCVBUF`.
- Time-shifted effects (`jitter`, `reorder`, `duplicate`) delivered through a
  `DelayQueue` on real timers, capped by `max_in_flight` with a `queue_overflows`
  counter so a burst cannot turn into an outage of the proxy's own making.
- TOML scenario configuration.
- HTTP control plane behind `--control-addr`: `GET /metrics` for a counter snapshot and
  `POST /reload` for hot-swapping the running toxics through a lock-free atomic swap.
- Exact, test-verified counters: `forwarded`, `dropped`, `delayed`, `duplicated`,
  `send_errors`, `queue_overflows`.
- Criterion microbenchmarks for the operator decision path, and a
  `cargo run --release --example latency` harness that measures added latency against a
  direct socket plus saturation throughput.
- FIX groundwork: `FixFramer` with body-length and tail validation capped at a 16 KiB
  maximum message length, and a `FixConnection` TCP transport that frames messages off a
  stream with Nagle disabled and explicit socket buffers on both ends. The session-fault
  operators and the connection-relay proxy on top of it are not part of this release.

### Notes

- `unsafe_code = "forbid"`; miri runs in the standard verification cycle.
- The hot path does not allocate per packet, does not log, does not take locks, and does
  not panic - a bad packet is counted into a metric and swallowed.
- MSRV 1.96, verified in CI.

[0.1.0]: https://github.com/mirotvoretts/tickchaos/releases/tag/v0.1.0
