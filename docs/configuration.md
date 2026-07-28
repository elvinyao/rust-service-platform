# Configuration

The reference runtime uses three independent strict YAML documents plus environment overrides.

## Configuration Layers

```text
runtime YAML
  -> operational environment overrides
  -> validation

rule-engine YAML
  -> normalization
  -> validation

rules YAML
  -> provider defaults
  -> rule validation
  -> provider/workflow validation
  -> executor preflight
```

The files are not deep-merged. `RUNTIME_CONFIG` and `RULE_ENGINE_CONFIG` select complete documents.

Default paths:

```text
RUNTIME_CONFIG=config/runtime.yaml
RULE_ENGINE_CONFIG=config/rule-engine.yaml
WORKFLOW_RULES=config/workflow-rules.yaml
```

## Base and Demo Profiles

The base composition is YAML-first:

```text
config/runtime.yaml
config/rule-engine.yaml
config/workflow-rules.yaml
```

Confluence, Mattermost, and BadgeDB are disabled. The WebSocket input remains enabled so the reference runtime demonstrates a real input; start `fake-server` or override/disable that input in a custom profile.

The demo profile enables all adapters:

```text
config/profiles/demo/runtime.yaml
config/profiles/demo/rule-engine.yaml
config/workflow-rules.yaml
```

Run it through `make dev`, which also manages the fake dependencies.

## Runtime Configuration

Complete shape:

```yaml
admin:
  address: "127.0.0.1:18080"
  read_header_timeout: "5s"
  read_timeout: "10s"
  write_timeout: "10s"
  idle_timeout: "120s"

inputs:
  websocket:
    enabled: true
    server_url: "ws://localhost:8093"
    path: "/ws"
    reconnect_interval: "5s"

adapters:
  confluence:
    enabled: false
    api_endpoint: "http://localhost:8090"
    settings_page_id: "settings-page-1"
    refresh_interval: "5m"
  mattermost:
    enabled: false
    server_url: "http://localhost:8091"
    websocket_url: "ws://localhost:8092"
    api_token: "test-token-123"
    channel: "test-channel-1"
  badgedb:
    enabled: false
    path: "/tmp/service-workflow-badges.db"
```

Unknown fields and additional YAML documents are rejected.

### Admin Fields

| Field | Meaning | Validation |
| --- | --- | --- |
| `address` | TCP listener address | Valid host and `u16` port |
| `read_header_timeout` | Maximum header-read time | Greater than zero |
| `read_timeout` | Maximum request-body read time | Greater than zero |
| `write_timeout` | Maximum handler/response write time | Greater than zero |
| `idle_timeout` | Maximum idle connection time | Greater than zero |

The admin listener binds before `App::start` succeeds. A bind failure rolls back already-started components.

Use `0.0.0.0:<port>` inside containers. Local profiles intentionally use loopback because the admin API has no authentication layer.

### WebSocket Input Fields

| Field | Meaning | Validation when enabled |
| --- | --- | --- |
| `enabled` | Register and start the input | Boolean |
| `server_url` | WebSocket authority | Absolute `ws` or `wss` URL |
| `path` | Path appended to `server_url` | Starts with `/` |
| `reconnect_interval` | Delay after disconnect or connect failure | Greater than zero |

The service reconnects until shutdown. The admin metrics expose connection count, URL, path, reconnect interval, and last-message age.

### Confluence-Like Adapter Fields

| Field | Meaning | Validation when enabled |
| --- | --- | --- |
| `enabled` | Register the settings service/provider | Boolean |
| `api_endpoint` | API base URL | Absolute `http` or `https` URL |
| `settings_page_id` | Page containing JSON rule settings | Non-empty |
| `refresh_interval` | Background refresh delay | Greater than zero |

An initial fetch failure is logged and the service continues with an empty/last-good snapshot. Invalid published rules still fail workflow-engine preflight.

### Mattermost-Like Adapter Fields

| Field | Meaning | Validation when enabled |
| --- | --- | --- |
| `enabled` | Register the service and executor | Boolean |
| `server_url` | HTTP API base URL | Absolute `http` or `https` URL |
| `websocket_url` | Optional WebSocket base or complete endpoint | Empty or absolute `ws`/`wss` URL |
| `api_token` | Bearer token | May be empty only when both login credentials are provided |
| `channel` | Default post channel | Non-empty |

When `api_token` is empty, both `MATTERMOST_USERNAME` and `MATTERMOST_PASSWORD` are required.
The service logs in during startup and requires a successful response with a non-empty `Token`
header; login failure fails startup and triggers normal rollback.

Do not commit production credentials. The token in the demo profile is fake local data.

### BadgeDB Adapter Fields

| Field | Meaning | Validation when enabled |
| --- | --- | --- |
| `enabled` | Register the in-memory demo store and `db` executor | Boolean |
| `path` | Diagnostic/backup path | Non-empty |

BadgeDB demonstrates service-owned state, executor lookup, health, and concurrent access. It is not a production database.

## Runtime Environment Overrides

The binary chooses config files with:

| Variable | Default |
| --- | --- |
| `RUNTIME_CONFIG` | `config/runtime.yaml` |
| `RULE_ENGINE_CONFIG` | `config/rule-engine.yaml` |
| `WORKFLOW_RULES` | `config/workflow-rules.yaml` |

`RuntimeConfig::apply_env` supports:

| Variable | Target |
| --- | --- |
| `ADMIN_ADDR` | `admin.address` |
| `ADMIN_READ_HEADER_TIMEOUT` | `admin.read_header_timeout` |
| `ADMIN_READ_TIMEOUT` | `admin.read_timeout` |
| `ADMIN_WRITE_TIMEOUT` | `admin.write_timeout` |
| `ADMIN_IDLE_TIMEOUT` | `admin.idle_timeout` |
| `WEBSOCKET_SERVER_URL` | `inputs.websocket.server_url` |
| `WEBSOCKET_PATH` | `inputs.websocket.path` |
| `CONFLUENCE_API_ENDPOINT` | `adapters.confluence.api_endpoint` |
| `CONFLUENCE_SETTINGS_PAGE_ID` | `adapters.confluence.settings_page_id` |
| `MATTERMOST_SERVER_URL` | `adapters.mattermost.server_url` |
| `MATTERMOST_WS_URL` | `adapters.mattermost.websocket_url` |
| `MATTERMOST_API_TOKEN` | `adapters.mattermost.api_token` |
| `MATTERMOST_CHANNEL` | `adapters.mattermost.channel` |
| `MATTERMOST_USERNAME` | env-only login field |
| `MATTERMOST_PASSWORD` | env-only login field |
| `BADGEDB_PATH` | `adapters.badgedb.path` |

Empty environment values are ignored. Duration overrides use the same duration syntax as YAML.

Enabled flags, WebSocket reconnect interval, and Confluence refresh interval are intentionally profile-owned and have no environment override.

## Logging Environment

Logging is operational configuration rather than runtime YAML:

| Variable | Values |
| --- | --- |
| `LOG_LEVEL` | A tracing filter such as `info`, `debug`, or `service_runtime=debug` |
| `LOG_FORMAT` | `json`, `text`, or `pretty` |
| `LOG_TIME_FORMAT` | A Chrono/strftime format |
| `LOG_CALLER_INFO` | Boolean |
| `LOG_ANSI` | Boolean |
| `LOG_OUTPUT` | `stdout`, `stderr`, or a file path |
| `LOG_FILE_MAX_SIZE_MB` | Positive integer |
| `LOG_FILE_ROTATE_HOURS` | Positive integer |

Boolean values accept `1/0`, `true/false`, `yes/no`, and `on/off`.

## Duration Syntax

Durations follow Go-compatible units because existing profile files use that syntax:

```text
ns us µs μs ms s m h
```

Examples:

```yaml
read_timeout: "10s"
refresh_interval: "5m"
idle_timeout: "1h30m"
```

Negative durations and unitless non-zero values are rejected.

## Rule Engine Configuration

```yaml
workflows:
  - name: WorkflowEngine
    providers:
      - yaml
    mode: single
    pipeline_order:
      - yaml
    action_merge:
      dedup: true
      order: priority

action_merge:
  dedup: true
  order: priority
```

Workflow fields:

| Field | Meaning |
| --- | --- |
| `name` | Exact workflow name used during processing |
| `providers` | Unique registered provider names |
| `mode` | `single`, `or`, `and`, or `pipeline` |
| `pipeline_order` | Required full provider order for `pipeline` |
| `action_merge.dedup` | Remove duplicate actions |
| `action_merge.order` | Currently only `priority` |

Validation rejects:

- an empty workflow list
- duplicate workflow names
- empty or duplicate provider names
- a `single` policy with anything other than one provider
- a pipeline order that does not contain every provider exactly once
- unknown modes, fields, or action-order values

The reference runtime removes disabled demo providers from policies. It fails if no provider remains or if a configured Confluence provider lacks the required Mattermost executor.

## YAML Rules

```yaml
version: "1"
rules:
  - id: log_aaa
    workflow: WorkflowEngine
    enabled: true
    priority: 100
    conditions:
      - field: type
        op: eq
        value: AAA
    actions:
      - id: write_log
        executor: log
        priority: 100
        params:
          level: info
          template: "event={{.ID}} type={{.Type}}"
```

Defaults:

- `version`: `"1"`
- `workflow`: provider default workflow
- `enabled`: `true`
- priorities: `0`
- conditions: empty, which means match all messages
- params: empty map

Rules require:

- a unique non-empty `id`
- at least one action
- valid condition fields and operators
- compilable regular expressions
- a non-empty executor name for every action
- a workflow policy that permits the publishing provider

## Strict Validation Examples

Unknown runtime field:

```yaml
admin:
  adress: "127.0.0.1:18080"
```

Invalid URL scheme:

```yaml
inputs:
  websocket:
    enabled: true
    server_url: "http://localhost:8093"
```

Invalid provider policy:

```yaml
workflows:
  - name: WorkflowEngine
    providers: [yaml, remote]
    mode: single
```

Invalid rule:

```yaml
rules:
  - id: broken
    conditions:
      - field: metadata.severity
        op: regex
        value: "["
    actions:
      - executor: log
```

Each example fails before the admin listener reports a successful application start.
