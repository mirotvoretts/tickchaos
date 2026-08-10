# Contributing to tickchaos

Contributions are welcome - bug reports, scenarios, docs, and code. This document
describes how to get a change accepted. If anything here is unclear or out of date,
open an issue: that is a valid contribution too.

## Table of contents

- [Ways to contribute](#ways-to-contribute)
- [Reporting a bug](#reporting-a-bug)
- [Proposing a feature](#proposing-a-feature)
- [Development setup](#development-setup)
- [Quality gates](#quality-gates)
- [Coding guidelines](#coding-guidelines)
- [Commit messages](#commit-messages)
- [Pull request process](#pull-request-process)
- [Reporting a security issue](#reporting-a-security-issue)
- [Code of conduct](#code-of-conduct)
- [License](#license)

## Ways to contribute

- Report a bug or a surprising behaviour.
- Add a scenario to `scenarios/` demonstrating a real degradation pattern.
- Add or improve a toxic (operator) or a protocol extractor.
- Improve documentation, benchmarks, or test coverage.
- Review an open pull request.

If you are looking for a place to start, `docs/DEVELOPMENT_PLAN.md` lists the open
tasks with a full specification for each one.

## Reporting a bug

Open an issue including:

- What you expected to happen and what happened instead.
- The scenario TOML you ran (redact addresses if needed).
- The `seed` printed by the run. A seed reproduces a run bit-for-bit, so a report
  with a seed is usually reproducible on the maintainer's machine and one without
  usually is not.
- `tickchaos --version`, OS, and Rust toolchain version.
- The run report printed on shutdown (counters), if the issue is about delivery.

## Proposing a feature

Open an issue describing the failure mode you need to reproduce before writing code.
Scope matters here: tickchaos degrades real sockets in a deterministic, protocol-aware
way. Features that require a simulated runtime, that make runs non-reproducible, or
that add work to the packet hot path are likely to be declined regardless of the
implementation quality.

For anything larger than a single operator, agree on the design in the issue first.

## Development setup

Requirements:

- A stable Rust toolchain (edition 2021, MSRV 1.96, verified in CI).
- A nightly toolchain with the `miri` component, for undefined-behaviour checks:
  `rustup toolchain install nightly` and `rustup +nightly component add miri`.
- [`just`](https://github.com/casey/just) as the command runner (optional but assumed
  by the commands below).

```bash
git clone https://github.com/<your-fork>/tickchaos
cd tickchaos
just setup-hooks # points core.hooksPath at githooks/, runs fmt + clippy pre-commit
just verify # full local gate
```

Running the proxy against `nc -u` is described in the README quick start.

## Quality gates

A change is not valid until every gate is green. Run them locally before opening a
pull request; CI runs the same set.

```bash
just verify
```

which is:

```bash
cargo fmt --all --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features --locked
cargo +nightly miri test --lib --locked
```

Any clippy warning is a failure. CI additionally runs the MSRV check,
`cargo deny check advisories licenses bans sources`, and an informational
`cargo bench` job.

Benchmarks are informational, not a gate, because shared CI runners are too noisy to
fail a build on. If your change touches the hot path, run `cargo bench` locally and
include the before/after numbers in the pull request.

## Coding guidelines

Tests first. Write a failing test, make it pass, then refactor. Every behavioural
change needs a test that fails without it.

Style:

- No comments. Names and structure carry the meaning. A comment is acceptable only
  where the code cannot be made clear on its own: a non-obvious enum variant, or a
  safety argument. Try rewriting first.
- Document public API with rustdoc, but only where it genuinely helps. Do not
  document the obvious.
- Newtypes for domain primitives (`SeqNum`, `FeedId`, `Seed`). Make invalid
  combinations fail to compile rather than documenting them.
- No `unwrap`, `expect`, or `panic!` in library or binary code. Use `?` and typed
  errors: `thiserror` in library code, `anyhow` only in the CLI shell. Panicking is
  allowed in tests.
- Explicit arithmetic: `checked_*` where overflow is an error, `wrapping_*` where
  wrapping is the intended semantics (sequence numbers). Never rely on the global
  overflow-check flag.
- Do not hold a lock across `.await`. Keep async functions cancellation-safe by
  mutating state only after the await completes.
- Validate input at the boundary: addresses, ports, probabilities, durations.

Hot-path invariants (the packet data plane). A change that breaks one of these will
not be merged:

- Deterministic: all fault injection goes through the seeded PRNG. `OsRng` is
  forbidden here on purpose - reproducibility is the product.
- No allocation and no copies per packet. Reuse buffers, use `bytes::Bytes`.
- No logging and no locks. Metrics are atomics; `tracing` is control plane only.
- No panics. A bad packet is counted into a metric and swallowed.
- Delays are modelled on timers or a timing wheel, not a per-packet `sleep().await`.

Layering (see `CLAUDE.md` for the full model): `flows` depend on `scripts`, never the
reverse; `backgrounds` is the runtime that drives the active flow; sockets are
infrastructure. Keep the domain layer free of transport concerns.

## Commit messages

[Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/):

```
type(scope): description
```

Types in use: `feat`, `fix`, `refactor`, `test`, `docs`, `perf`, `build`, `ci`,
`chore`. Scope is the layer or module, for example `scripts`, `backgrounds`,
`transport`, `protocols`, `cli`.

```
feat(scripts): add protocol-aware sequence drop operator
fix(transport): report bind address on failure
```

Prefer small, logical commits over one large one. A commit should leave the tree
green.

## Pull request process

1. Fork and create a branch off `main`.
2. Make the change, with tests, in small commits.
3. Run `just verify` until everything is green.
4. Open the pull request with:
   - what changed and why,
   - the failure mode it addresses (link the issue),
   - benchmark numbers if the hot path is involved.
5. Expect a review within about a week. Changes touching the transport layer, byte
   parsing, or input validation get an extra security review pass and may take longer.
6. Address review comments as additional commits; do not force-push mid-review, it
   makes the diff hard to follow. Squashing happens on merge.

Pull requests that fail the gates, remove tests, or add hot-path allocations will be
sent back with a note on what to change.

## Reporting a security issue

Do not open a public issue for a vulnerability. Email the maintainer at
panoffilya@gmail.com (or telegram @illidvn) with a description and, if possible, a reproducing scenario and seed. You will get an acknowledgement within a few days.

Note that tickchaos is a testing tool that degrades traffic on purpose. Reports of
"the proxy dropped my packets" are not vulnerabilities. Memory unsafety, panics
reachable from network input, and parser crashes are.

## Code of conduct

Be direct, be civil, argue about the code and not the person. Harassment, personal
attacks, and off-topic hostility are not tolerated, and the maintainer may close or
block without further discussion. Assume good faith from others and give it back.

## License

By contributing you agree that your contribution is licensed under the MIT License,
the same terms as the rest of the project. See [LICENSE](LICENSE).
