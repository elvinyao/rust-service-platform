#!/usr/bin/env bash

set -euo pipefail

started=false

cleanup() {
  if [[ "${started}" == true ]]; then
    docker compose down --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

fail() {
  printf '%s\n' "$1" >&2
  docker compose logs >&2 || true
  exit 1
}

wait_for_url() {
  local url="$1"
  for _ in {1..300}; do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

docker compose config >/dev/null
if [[ -n "$(docker compose ps --status running --quiet)" ]]; then
  printf '%s\n' 'Docker Compose services are already running; stop them before verification' >&2
  exit 1
fi
started=true
docker compose up --build --detach

wait_for_url "http://127.0.0.1:18080/health/liveness" ||
  fail "Container runtime did not become live"

connected=false
for _ in {1..100}; do
  health="$(curl -fsS "http://127.0.0.1:8093/health" 2>/dev/null || true)"
  if [[ "${health}" == *'"clients":1'* ]]; then
    connected=true
    break
  fi
  sleep 0.1
done
[[ "${connected}" == true ]] || fail "Container WebSocket input did not connect"

curl -fsS -X POST "http://127.0.0.1:8093/api/send" \
  -H "Content-Type: application/json" \
  -d '{"id":"compose-smoke-1","type":"AAA","content":"compose-smoke-event","user_id":"smoke-user"}' \
  >/dev/null

delivered=false
for _ in {1..100}; do
  posts="$(curl -fsS "http://127.0.0.1:8091/api/v4/posts" \
    -H "Authorization: Bearer test-token-123" 2>/dev/null || true)"
  if [[ "${posts}" == *'compose-smoke-event'* ]]; then
    delivered=true
    break
  fi
  sleep 0.1
done
[[ "${delivered}" == true ]] || fail "Container AAA event did not reach fake Mattermost"

printf '%s\n' 'Docker Compose smoke test passed'
