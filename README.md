# TickChaos :: Chaos Proxy

**A protocol-aware UDP degradation proxy for testing market-data feed handlers against real network chaos - packet loss, reorder, duplication, jitter, and sequence-number gaps.**

Point your feed handler at the proxy instead of the exchange. Your code doesn't change; the packets do.

![tickchaos](assets/banner.png)

> **Demo:** _TODO - GIF of live sequence-gap detection (feed handler recovering from a dropped seqnum through the proxy)._

---

## Why not `turmoil` / `toxiproxy` / ...?

| | Real sockets<br>(no simulated runtime) | Zero code rewrite | Protocol-aware<br>sequence gaps | UDP-native |
|---|:---:|:---:|:---:|:---:|
| **tickchaos** | Yes | Yes | Yes | Yes |
| toxiproxy | Yes | Yes | No | No (TCP/HTTP) |
| tokio-rs/turmoil | No (sim runtime) | No | No | No (TCP sim) |
| madsim | No (sim runtime) | No | No | partial |
| Trixter | Yes | Yes | No | generic shaping |

- **toxiproxy** speaks TCP/HTTP and has no concept of UDP or sequence numbers.
- **turmoil / madsim** require rewriting your code against a *simulated* runtime. tickchaos drives **real UDP sockets** - the code under test runs unmodified.
- **Trixter** and netem-style shapers degrade traffic at the link level but are not protocol-aware: they can't drop *a specific seqnum*.

tickchaos's niche: **real sockets + protocol-aware faults on live trading infrastructure.** FIX support is on the roadmap.

---

## Why I built this

In real market-data pipelines, the gap-recovery path - detecting a missing sequence number and firing a retransmit request - is the code most likely to be wrong and the code you can never exercise against a live exchange (you can't ask NASDAQ to drop packet #48213 for you). Generic network shapers can't drop *a specific seqnum*, and simulation frameworks force you to rewrite the very code you're trying to trust. Nothing existed for this niche, so tickchaos does exactly one thing: deterministic, protocol-aware degradation on real UDP sockets.

---

## Quick start

```bash
# 1. Build (release profile: LTO, single codegen unit, stripped)
cargo build --release

# 2. Write a scenario (see below), e.g. scenario.toml

# 3. Run the proxy
./target/release/tickchaos --scenario scenario.toml

# 4. Point your feed handler at `listen` instead of the exchange.
#    The proxy forwards to `upstream`, degrading packets per the scenario.
```

Every run logs its `seed` - one seed reproduces a run bit-for-bit.

### Try it with `nc -u`

Three terminals, using the bundled `scenarios/market-open-burst.toml`
(`listen = 127.0.0.1:9000`, `upstream = 127.0.0.1:9001`):

```bash
# terminal 1 - the "exchange": listen where the proxy forwards to
nc -u -l 127.0.0.1 9001

# terminal 2 - the proxy
cargo run --release -- --scenario scenarios/market-open-burst.toml

# terminal 3 - the "feed handler": send to the proxy, not the exchange
nc -u 127.0.0.1 9000
```

Type lines into terminal 3; they arrive in terminal 1 degraded per the
scenario (2% drop, 0-3ms jitter, 1% reorder with a 5ms hold).

### Seeing a protocol-aware gap

`scenarios/gap-seq.toml` drops MoldUDP64 sequence 5 and nothing else - but you can't type a
binary MoldUDP64 header into `nc`. The runnable demonstration is the end-to-end test, which
pushes sequences 1..=10 through a real socket and asserts that exactly one of them vanishes:

```bash
cargo test --test e2e_protocol -- --nocapture
```

---

## Scenario config

A scenario is TOML. Top-level fields plus an ordered list of `operators` (toxics):

```toml
seed          = 42                      # deterministic PRNG seed (logged every run)
listen        = "127.0.0.1:9000"        # your feed handler connects here
upstream      = "203.0.113.10:4000"     # real exchange / feed source
protocol      = "moldudp64"             # optional: enables seqnum-aware toxics
# multicast_group = "233.252.0.1:4000"  # optional: multicast join
# recv_buf_bytes  = 4194304             # optional: SO_RCVBUF (default 4 MiB)
# max_in_flight   = 65536               # optional: delay-queue cap (default 65536)

[[operators]]
type        = "drop"
probability = 0.01                      # drop 1% of packets

[[operators]]
type    = "jitter"
min_ms  = 0
max_ms  = 5                             # uniform delay in [0, 5] ms

[[operators]]
type        = "duplicate"
probability = 0.005

[[operators]]
type        = "reorder"
probability = 0.02
hold_ms     = 3                         # hold packet, let later ones pass first

[[operators]]
type            = "rate_limit"
packets_per_sec = 100000

[[operators]]
type    = "drop_seq"
seqnums = [48213, 48214]                # drop these exact sequence numbers

[[operators]]
type  = "gap_burst"
start = 1000
len   = 50                              # drop the run [1000, 1050)
```

Operators are applied in list order; the first one that does not forward wins.

### Toxics

| `type` | Fields | Effect | Needs `protocol` |
|---|---|---|:---:|
| `drop` | `probability: f64` | Drops packets with the given probability | no |
| `duplicate` | `probability: f64` | Re-emits a copy of the packet | no |
| `jitter` | `min_ms: u64`, `max_ms: u64` | Uniform random delay in `[min, max]` | no |
| `reorder` | `probability: f64`, `hold_ms: u64` | Holds a packet so later ones overtake it | no |
| `rate_limit` | `packets_per_sec: u32` | Token-bucket cap; drops on empty bucket | no |
| `drop_seq` | `seqnums: [u64]` | Drops exactly those sequence numbers - no PRNG involved | yes |
| `gap_burst` | `start: u64`, `len: u32` | Drops the contiguous run `[start, start + len)` | yes |

`drop_seq` and `gap_burst` need a sequence number, which comes from the `protocol` field:

| `protocol` | Sequence source |
|---|---|
| unset / `"none"` | none - seqnum-aware toxics forward everything |
| `"moldudp64"` | MoldUDP64 header, big-endian `u64` at offset 10 (NASDAQ ITCH transport) |

A packet whose sequence number could not be parsed is always forwarded untouched: a
scenario that targets seqnums must not disturb unrelated traffic on the same socket.
Ready-made examples live in `scenarios/` (`gap-seq.toml`, `gap-burst.toml`,
`market-open-burst.toml`).

---

## Determinism & low-latency guarantees

- **Deterministic.** All fault injection goes through a seeded PRNG. One seed produces an identical run. The seed is logged every run.
- **Zero-copy hot path.** Reused `bytes` buffers, no per-packet allocation.
- **No locks / no logging on the data plane.** Metrics are atomics; `tracing` is control-plane only.
- **Never panics on the hot path.** A bad packet is counted and swallowed - the proxy stays up.
- **Bounded memory.** Delayed and duplicated packets wait in a capped queue (`max_in_flight`);
  past the cap they are counted as `queue_overflows` and dropped, so the proxy never turns a
  burst into an outage of its own.
- **No `unsafe`.** Enforced by `unsafe_code = "forbid"`; miri runs on every cycle regardless.

---

## Benchmarks

`cargo bench` measures the decision path - one `decide()` per prepared packet. Typical
figures on a desktop x86-64 (criterion, median of 100 samples):

| Bench | Time |
|---|---|
| `gap_burst` | 5.1 ns |
| `rate_limiter` | 7.2 ns |
| `dropper` / `duplicate` / `reorderer` | ~7.5 ns |
| `jitter` | 8.1 ns |
| `drop_seq` (hit or miss, 1000 targets) | 8.1 ns |
| `flow/three_operators` | 18.0 ns |

The domain layer is nanoseconds - `drop_seq` costs the same as a probabilistic `drop`
because the hash lookup disappears next to the PRNG call, and `gap_burst` is cheapest
precisely because it touches no PRNG at all. The latency budget is spent on sockets and
timers, not on operators.

End-to-end overhead is the number that matters, and the target - baseline passthrough
**< 50 us p99** - is not yet a hard gate: the CI bench job is informational
(`continue-on-error`), because a shared runner is too noisy to fail a build on. The
end-to-end smoke check lives in `tests/e2e_udp.rs::passthrough_adds_negligible_latency`
and is `#[ignore]`d for the same reason - run it locally with
`cargo test -- --ignored`.

---

## Metrics

Exact, test-verified counters, all `AtomicU64` and readable via `Stats::snapshot()`:

| Counter | Meaning |
|---|---|
| `forwarded` | Packets sent upstream, including delayed and duplicated copies |
| `dropped` | Packets a toxic removed |
| `delayed` | Packets parked in the delay queue (`jitter`, `reorder`) |
| `duplicated` | Extra copies scheduled by `duplicate` |
| `send_errors` | Failed sends - counted and swallowed, the loop keeps running |
| `queue_overflows` | Time-shifted packets dropped because `max_in_flight` was reached |

> _TODO - expose these over HTTP once the control plane lands (`GET /metrics`)._

---

## Stack

| Crate | Role |
|---|---|
| `tokio` | Async UDP data plane. The hot path sits behind a `PacketTransport` trait, so `mio` / raw sockets can replace it without touching the domain. |
| `socket2` | Socket construction: multicast join, `SO_REUSEADDR`, `SO_RCVBUF` sizing (undersized receive buffers drop packets on bursts). Wrapped into a tokio socket. |
| `bytes` | Zero-copy buffers on the hot path - no per-packet `Vec`. |
| `tokio-util` (`time`) + `futures` | `DelayQueue` driving delayed and duplicated packets on real timers, not per-packet `sleep().await`. |
| `rand` (`StdRng` + `seed_from_u64`) | Deterministic fault injection. `OsRng` is deliberately *not* used - reproducibility is the product. |
| `clap` (derive) | CLI. |
| `serde` + `toml` | Scenario (toxic) config. |
| `thiserror` / `anyhow` | Typed errors in the library; `anyhow` only in the CLI shell. |
| `tracing` + `tracing-subscriber` | Structured logs, **control plane only** - a per-packet span would eat the latency budget. Hot-path counters are `AtomicU64`. |
| `criterion` *(dev)* | Proxy overhead benchmarks. |
| `proptest` *(dev)* | Property tests for delivery invariants. |

Planned: `axum` + `arc-swap` for an HTTP control plane (live metrics, scenario hot-reload
via a lock-free flow swap); `quanta` / `minstant` for sub-millisecond jitter timestamps
(`tokio::time` quantizes to roughly a millisecond).

Rust edition 2021, MSRV 1.96 (checked in CI). Release profile: `lto = true`,
`codegen-units = 1`, `strip = true`, `overflow-checks = false` (the hot path must not panic;
overflow is handled explicitly with `checked_*` / `wrapping_*`). A `profiling` profile
inherits from release but keeps debug symbols, for `perf` / flamegraphs.

---

## Development

```bash
just verify     # fmt-check + lint + test + doc + miri
```

Or individually:

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
cargo +nightly miri test --lib     # miri needs the nightly toolchain
cargo bench                        # criterion, informational
```

`just setup-hooks` points `core.hooksPath` at `githooks/`, which runs `just quick`
(fmt + clippy) before every commit. CI additionally runs the MSRV check and
`cargo deny check advisories licenses bans sources`.

## License

MIT - see [LICENSE](LICENSE).
