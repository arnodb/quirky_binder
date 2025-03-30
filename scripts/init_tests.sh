#!/bin/sh

set -eu

ROOT=$(cd "$(dirname "$0")/.." && pwd)

REPO="git2_repo"

cd "$ROOT/tests/quirky_binder_tests/input"

rm -Rf "$REPO"
mkdir "$REPO"
cd "$REPO"

set -x

git init

git config user.email "you@example.com"
git config user.name "Your Name"

for i in $(seq 1 12)
do
    echo "Content $i" >| "file_$i.txt"
    git add "file_$i.txt"
    git commit -m "Add file_$i.txt"
done

