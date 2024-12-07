#!/bin/sh

set -eu

SHORT_NAME="$1"

mkdir "$SHORT_NAME"
cd "$SHORT_NAME"
cargo generate --init --name "${SHORT_NAME}" --path ../template

