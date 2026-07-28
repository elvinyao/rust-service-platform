#!/bin/sh
set -eu

minimum="${COVERAGE_MIN:-90}"

if ! cargo llvm-cov --version >/dev/null 2>&1; then
  printf '%s\n' \
    'cargo-llvm-cov is required for coverage.' \
    'Install it with: cargo +stable install cargo-llvm-cov --version 0.8.7 --locked' >&2
  exit 1
fi

cargo llvm-cov \
  --locked \
  --workspace \
  --all-features \
  --fail-under-lines "${minimum}" \
  --summary-only
