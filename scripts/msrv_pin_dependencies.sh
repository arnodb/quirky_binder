#!/bin/sh

set -eu

set -x

#cargo update -p abc --precise x.y.z
cargo update -p home --precise 0.5.11

cd codegen/quirky_binder_codegen_wasm
#cargo update -p abc --precise x.y.z
cd -

cd recipes
#cargo update -p abc --precise x.y.z
cargo update -p home --precise 0.5.11
cd -
