#!/usr/bin/env bash

set -euo pipefail
set -m

log_file="$(mktemp "${TMPDIR:-/tmp}/service-platform-dev.XXXXXX")"
dev_pid=""

cleanup() {
  if [[ -n "${dev_pid}" ]] && kill -0 "${dev_pid}" 2>/dev/null; then
    kill -TERM -- "-${dev_pid}" 2>/dev/null || kill -TERM "${dev_pid}" 2>/dev/null || true
    for _ in {1..50}; do
      if ! kill -0 "${dev_pid}" 2>/dev/null; then
        break
      fi
      sleep 0.1
    done
    if kill -0 "${dev_pid}" 2>/dev/null; then
      kill -KILL -- "-${dev_pid}" 2>/dev/null || true
    fi
  fi
  if [[ -n "${dev_pid}" ]]; then
    wait "${dev_pid}" 2>/dev/null || true
  fi
  rm -f "${log_file}"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

fail() {
  printf '%s\n' "$1" >&2
  printf '%s\n' '--- make dev output ---' >&2
  cat "${log_file}" >&2
  exit 1
}

wait_for_url() {
  local url="$1"
  for _ in {1..300}; do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "${dev_pid}" 2>/dev/null; then
      return 1
    fi
    sleep 0.1
  done
  return 1
}

make dev >"${log_file}" 2>&1 &
dev_pid=$!

wait_for_url "http://127.0.0.1:18080/health/liveness" ||
  fail "Reference runtime did not become live"

connected=false
for _ in {1..100}; do
  health="$(curl -fsS "http://127.0.0.1:8093/health" 2>/dev/null || true)"
  if [[ "${health}" == *'"clients":1'* ]]; then
    connected=true
    break
  fi
  sleep 0.1
done
[[ "${connected}" == true ]] || fail "WebSocket input did not connect to fake-server"

curl -fsS -X POST "http://127.0.0.1:8093/api/send" \
  -H "Content-Type: application/json" \
  -d '{"id":"dev-smoke-1","type":"AAA","content":"dev-smoke-event","user_id":"smoke-user"}' \
  >/dev/null

delivered=false
for _ in {1..100}; do
  posts="$(curl -fsS "http://127.0.0.1:8091/api/v4/posts" \
    -H "Authorization: Bearer test-token-123" 2>/dev/null || true)"
  if [[ "${posts}" == *'dev-smoke-event'* ]]; then
    delivered=true
    break
  fi
  sleep 0.1
done
[[ "${delivered}" == true ]] || fail "AAA event did not reach fake Mattermost"

printf '%s\n' 'make dev smoke test passed'
