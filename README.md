# TickChaos :: Chaos Proxy

**A protocol-aware UDP degradation proxy for testing market-data feed handlers against real network chaos - packet loss, reorder, duplication, jitter, and sequence-number gaps.**

Point your feed handler at the proxy instead of the exchange. Your code doesn't change; the packets do.

[![crates.io](https://img.shields.io/crates/v/tickchaos.svg)](https://crates.io/crates/tickchaos)
[![docs.rs](https://docs.rs/tickchaos/badge.svg)](https://docs.rs/tickchaos)
[![license](https://img.shields.io/crates/l/tickchaos.svg)](LICENSE)

![tickchaos](https://raw.githubusercontent.com/mirotvoretts/tickchaos/main/assets/banner.png)

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

## Install

```bash
cargo install tickchaos
```

Or build from source (release profile: LTO, single codegen unit, stripped):

```bash
git clone https://github.com/mirotvoretts/tickchaos
cd tickchaos
cargo build --release
```

## Quick start

```bash
# 1. Write a scenario (see below), e.g. scenario.toml

# 2. Run the proxy
tickchaos --scenario scenario.toml

# 3. Point your feed handler at `listen` instead of the exchange.
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
tickchaos --scenario scenarios/market-open-burst.toml

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

## Performance

### End-to-end overhead

What actually matters is how much latency the proxy adds over a direct socket. The
harness measures both paths in one process off the same monotonic clock, at a paced
10,000 pps with 64-byte payloads over loopback, 20,000 packets with the first 2,000
discarded as warmup:

```bash
cargo run --release --example latency
```

Desktop x86-64 (Linux 7.1, unpinned, no isolated cores), median of 7 runs:

| Path | p50 | p99 |
|---|---|---|
| direct UDP socket | 10.6 us | 22-45 us |
| through tickchaos | 20.2 us | 51-94 us |
| **added by the proxy** | **~10 us** | **15-50 us** |

The added p50 of roughly 10 microseconds is stable to a few tenths across runs. The p99
delta is not: on an unpinned desktop it moves between 15 and 50 us run to run, and past
p99 the difference between the two paths is smaller than the scheduler noise in either
of them - the direct socket occasionally shows a *worse* p99.9 than the proxied one.
Treat p50 as the real figure and the tail as an upper bound that needs a tuned box
(pinned cores, `isolcpus`, busy-polling sockets) to measure honestly.

Saturation on the same loopback setup: with an unpaced sender offering about 210,000 pps
the proxy forwards roughly 165,000 pps, and the shortfall is dropped by the kernel in the
receive buffer before the proxy ever sees it. The receiver observes essentially every
packet the proxy forwards, so the ceiling is socket ingress, not the operator pipeline.

The `< 50 us p99` target from the project invariants is therefore met at the median run
but is not yet a hard CI gate: a shared GitHub runner is far too noisy to fail a build
on. The CI bench job is informational (`continue-on-error`).

### Why there is no head-to-head table here

There is no honest latency comparison against the alternatives, because none of them do
the same job on the same transport:

- **toxiproxy** cannot proxy UDP at all, so there is nothing to compare on the workload
  tickchaos exists for. A TCP-to-TCP comparison would be possible once the FIX track
  lands a TCP transport, and it is deliberately left out until then.
- **turmoil / madsim** run on a simulated clock. There is no wall-clock latency to
  measure; the numbers would be meaningless rather than merely unfair.
- **netem / tc** shape traffic in the kernel and will beat any userspace proxy on
  overhead. That is the correct trade to state plainly: tickchaos costs about 10 us more
  than a bare socket and, in exchange, can drop sequence number 48213 specifically -
  which netem cannot do at any latency.

Numbers above are reproducible on your own hardware with the command shown; please do
not trust a benchmark table you cannot re-run.

### Operator microbenchmarks

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

A coarse end-to-end regression check also lives in
`tests/e2e_udp.rs::passthrough_adds_negligible_latency`, `#[ignore]`d for the same noise
reason - run it locally with `cargo test -- --ignored`.

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

The same counters are printed as a TOML run report on shutdown, together with the seed
and uptime.

### Control plane

Passing `--control-addr` starts an HTTP control plane alongside the data plane. Omit the
flag and no socket is opened.

```bash
tickchaos --scenario scenario.toml --control-addr 127.0.0.1:9090
```

| Endpoint | Purpose |
|---|---|
| `GET /metrics` | Current counter snapshot as JSON |
| `POST /reload` | Body is a scenario TOML; swaps the running toxics without a restart |

```bash
curl -s http://127.0.0.1:9090/metrics
curl -s -X POST --data-binary @scenarios/gap-seq.toml http://127.0.0.1:9090/reload
```

A rejected scenario returns `400` with the parse error and leaves the running flow
untouched. On success the new flow is published through a lock-free atomic swap, which
the proxy picks up at the top of its next loop iteration - so on a completely idle proxy
the swap lands when the next packet or timer arrives, not at the instant `/reload`
returns.

---

## Contributing

Bug reports, scenarios, and code are welcome. Read
[CONTRIBUTING.md](CONTRIBUTING.md) first: it covers the development setup, the quality
gates a change has to pass (`just verify`), the hot-path invariants, and the pull
request process.

A bug report is far more useful with the `seed` from the run that produced it - one
seed reproduces a run bit-for-bit.

## License

MIT - see [LICENSE](LICENSE).
