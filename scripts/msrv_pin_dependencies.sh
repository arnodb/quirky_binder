#!/bin/sh

set -eu

set -x
cargo update -p home --precise 0.5.5
cargo update -p relative-path --precise 1.9.0

