# Fake Server

`fake-server` provides local APIs used by the demo profile and integration tests.

## Included APIs

- Confluence-like content and space endpoints
- Mattermost-like login, posts, channels, users, ping, and WebSocket endpoints
- generic WebSocket input with an HTTP event publisher

## Run

From the repository root:

```bash
cargo run -p fake-server --bin fake-server
```

Default ports:

```text
Confluence HTTP:     8090
Mattermost HTTP:     8091
Mattermost WebSocket:8092
Generic WebSocket:   8093
```

## Flags

```bash
cargo run -p fake-server --bin fake-server -- \
  --confluence-port 8090 \
  --mm-http-port 8091 \
  --mm-ws-port 8092 \
  --ws-port 8093
```

Readiness probe:

```bash
cargo run -p fake-server --bin fake-server -- \
  --healthcheck --ws-port 8093
```

## Send and Inspect Events

```bash
curl -s -X POST http://127.0.0.1:8093/api/send \
  -H 'Content-Type: application/json' \
  -d '{"id":"fake-1","type":"AAA","content":"hello","user_id":"local-user"}'

curl -s http://127.0.0.1:8093/api/messages
```

Inspect fake Mattermost posts:

```bash
curl -s http://127.0.0.1:8091/api/v4/posts \
  -H 'Authorization: Bearer test-token-123'
```

## Test

```bash
cargo test -p fake-server
```

The reference runtime integration tests start `FakeApiManager` on ephemeral ports and do not require a separately running process.
