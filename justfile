# Build

clippy:
    export RUST_BACKTRACE=1
    cargo clippy --all-features --all-targets -- -D warnings

watch_clippy:
    export RUST_BACKTRACE=1
    cargo watch -x "clippy --all-features --all-targets -- -D warnings"

test:
    export RUST_BACKTRACE=1
    cargo test --all-features

check_all:
    export RUST_BACKTRACE=1

    just stable
    cargo clippy --all-features --all-targets -- -D warnings
    cargo build --all-features
    cargo test --all-features

    just msrv
    cargo build --all-features
    cargo test --all-features

    just nightly
    cargo build --all-features
    cargo test --all-features

    just stable

# Toolchain management

stable:
    ./scripts/switch_rust_toolchain.sh -c

nightly:
    ./scripts/switch_rust_toolchain.sh -n

msrv:
    ./scripts/switch_rust_toolchain.sh -m

# Formatting

fmt:
    cargo fmt

fmt_nightly:
    just nightly
    cargo fmt

