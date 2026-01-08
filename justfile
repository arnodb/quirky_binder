export RUST_BACKTRACE := "1"

# Build

build:
    cargo build --all-features --all-targets

watch_build:
    cargo watch -x "build --all-features --all-targets"

clippy:
    cargo clippy --all-features --all-targets -- -D warnings

watch_clippy:
    cargo watch -x "clippy --all-features --all-targets -- -D warnings"

test *args:
    cargo test --all-features {{args}}

check_all:
    just stable
    cargo clippy --all-features --all-targets -- -D warnings
    cargo build --all-features
    cargo test --all-features
    RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps

    just msrv
    cargo build --all-features
    cargo test --all-features

    just nightly
    cargo build --all-features
    cargo test --all-features

    just stable
    just recipes
    just wasm

doc:
    RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps

[working-directory: 'recipes']
recipes:
    cargo build --all-features

[working-directory: 'codegen/quirky_binder_codegen_wasm']
wasm:
    wasm-pack build

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

# Examples

run_example example *args:
    cargo run -p $(basename {{example}}) {{args}}
