#!/bin/sh

set -eu

set -x
cargo update -p home --precise 0.5.5
cargo update -p relative-path --precise 1.9.0
cargo update -p unicode-width --precise 0.1.13

cd codegen/quirky_binder_codegen_wasm
cargo update -p bumpalo --precise 3.14.0
cargo update -p unicode-width --precise 0.1.13

