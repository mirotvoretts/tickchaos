default: verify

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all --check

lint:
    cargo clippy --all-targets --all-features --locked -- -D warnings

test:
    cargo test --all-features --locked

miri:
    MIRIFLAGS=-Zmiri-disable-isolation cargo +nightly miri test --lib --locked

quick: fmt-check lint

verify: fmt-check lint test miri

setup-hooks:
    git config core.hooksPath githooks
