# TickChaos :: Chaos Proxy

**A protocol-aware UDP degradation proxy for testing market-data feed handlers against real network chaos - packet loss, reorder, duplication, jitter, and sequence-number gaps.**

Point your feed handler at the proxy instead of the exchange. Your code doesn't change; the packets do.

![tickchaos](assets/banner.png)

> **Demo:** _TODO - GIF of live sequence-gap detection (feed handler recovering from a dropped seqnum through the proxy)._

---

## Why not `turmoil` / `toxiproxy` / ...?

| | Real sockets<br>(no simulated runtime) | Zero code rewrite | Protocol-aware<br>sequence gaps | UDP-native |
|---|:---:|:---:|:---:|:---:|
| **tickchaos** | Yes | Yes | Yes *(planned)* | Yes |
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

---

## Scenario config

A scenario is TOML. Top-level fields plus an ordered list of `operators` (toxics):

```toml
seed          = 42                      # deterministic PRNG seed (logged every run)
listen        = "127.0.0.1:9000"        # your feed handler connects here
upstream      = "203.0.113.10:4000"     # real exchange / feed source
# multicast_group = "233.252.0.1:4000"  # optional: multicast join
# recv_buf_bytes  = 4194304             # optional: SO_RCVBUF (default 4 MiB)

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
```

Operators are applied in list order.

### Toxics

| `type` | Fields | Effect | Status |
|---|---|---|---|
| `drop` | `probability: f64` | Drops packets with the given probability | implemented |
| `duplicate` | `probability: f64` | Re-emits a copy of the packet | TODO |
| `jitter` | `min_ms: u64`, `max_ms: u64` | Uniform random delay in `[min, max]` | TODO |
| `reorder` | `probability: f64`, `hold_ms: u64` | Holds a packet so later ones overtake it | TODO |
| `rate_limit` | `packets_per_sec: u32` | Token-bucket cap; drops on empty bucket | TODO |

> **Protocol-aware seqnum gaps** (drop / reorder by exact sequence number, ITCH & crypto feeds) - TODO / on the roadmap.

---

## Determinism & low-latency guarantees

- **Deterministic.** All fault injection goes through a seeded PRNG. One seed produces an identical run. The seed is logged every run.
- **Zero-copy hot path.** Reused `bytes` buffers, no per-packet allocation.
- **No locks / no logging on the data plane.** Metrics are atomics; `tracing` is control-plane only.
- **Never panics on the hot path.** A bad packet is counted and swallowed - the proxy stays up.

---

## Benchmarks (proxy overhead)

Target: baseline passthrough (proxy with no toxics) overhead **< 50 us p99**, verified in CI against the previous run.

> _TODO - publish criterion results (p50 / p99 overhead vs. direct socket)._

---

## Metrics

Exact, test-verified counters: dropped, reordered, duplicated, delayed. The proxy provably does what the scenario claims.

> _TODO - document the metrics/observability surface once the control plane stabilizes._

---

## Development

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo miri test
```

## License

_TODO._
