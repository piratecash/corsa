# RPC Layer

## English

### Overview

The RPC layer provides command dispatch for managing a CORSA node. All commands are registered in a `CommandTable` — a pure in-process function dispatch table with no HTTP dependency.

The desktop UI calls `CommandTable` directly (no HTTP round-trip). External clients (`corsa-cli`, third-party tools) access commands through a thin Fiber v3 HTTP wrapper that delegates to the same `CommandTable`. This ensures identical behavior for all callers.

The HTTP RPC server listens on `127.0.0.1:46464` by default. If username and password are not configured, access is granted without authentication.

### Interaction Diagram

#### Overall Architecture

```mermaid
graph TB
    subgraph UI_Layer["UI (optional, desktop only)"]
        UI["Desktop UI<br/>(Gio)"]
    end

    subgraph External["External Clients"]
        CLI["corsa-cli<br/>(HTTP POST)"]
        EXT["Third-party Tools<br/>(curl, scripts)"]
    end

    subgraph Node_Layer["Core Node (always present)"]
        CT["CommandTable<br/>(in-process dispatch)"]

        subgraph HTTP["HTTP Wrapper — Fiber v3<br/>127.0.0.1:46464"]
            AUTH["Auth Middleware<br/>HTTP Basic Auth (optional)"]
            EXEC["POST /rpc/v1/exec<br/>(universal dispatch)"]
            FRAME["POST /rpc/v1/frame<br/>(frame dispatch + fallback)"]
            LEGACY["Legacy Routes<br/>/system/* /network/* ..."]
        end

        NP["NodeProvider<br/>HandleLocalFrame() · Address() · ClientVersion()"]
        NODE["node.Service<br/>(network layer)"]
        MP["MetricsProvider<br/>TrafficSnapshot()"]
        MC["metrics.Collector<br/>(traffic sampling, ring buffer)"]
    end

    subgraph Desktop_Providers["Desktop Providers (optional, nil on standalone node)"]
        CP["ChatlogProvider<br/>FetchChatlog() · FetchChatlogPreviews() · FetchConversations()"]
        DP["DMRouterProvider<br/>Snapshot() · SendMessage()"]
        DC["DesktopClient<br/>(chatlog.Store / SQLite)"]
        DMR["DMRouter<br/>(UI state, conversations,<br/>message routing)"]
    end

    MESH["TCP Mesh Network<br/>(peer-to-peer)"]

    UI -->|"direct call<br/>(no HTTP)"| CT

    CLI --> AUTH
    EXT --> AUTH
    AUTH --> EXEC
    AUTH --> FRAME
    AUTH --> LEGACY
    EXEC --> CT
    FRAME -->|"CommandTable first,<br/>fallback HandleLocalFrame"| CT
    LEGACY --> CT

    CT --> NP
    CT --> MP
    CT -.->|"nil on standalone node"| CP
    CT -.->|"nil on standalone node"| DP

    NP --> NODE
    MP --> MC
    MC -->|"fetch_network_stats"| NODE
    CP --> DC
    DP --> DMR
    DMR --> NODE
    DMR --> DC
    NODE --> MESH
```
*Diagram 1 — Overall architecture.*

Core Node layer is always present and includes CommandTable, NodeProvider, MetricsProvider, and HTTP wrapper. Desktop Providers (ChatlogProvider, DMRouterProvider) are optional — on standalone node they are nil, their commands return 503 and are hidden from help. MetricsProvider (metrics.Collector) collects traffic samples from node.Service via `fetch_network_stats` and stores them in a ring buffer for `fetch_traffic_history`.

#### Request Processing Flow

```mermaid
sequenceDiagram
    participant UI as Desktop UI
    participant CT as CommandTable
    participant N as NodeProvider
    participant NS as node.Service

    Note over UI,CT: Desktop UI — direct call (no HTTP)
    UI->>CT: Execute({name: "get_peers"})
    CT->>N: HandleLocalFrame({type: "get_peers"})
    N->>NS: HandleLocalFrame(frame)
    NS-->>N: Frame{type: "peers", peers: [...]}
    N-->>CT: CommandResponse{Data: json}
    CT-->>UI: CommandResponse

    Note over UI,CT: External client — HTTP wrapper
    participant C as corsa-cli
    participant S as Fiber HTTP Server
    participant A as Auth Middleware

    C->>S: POST /rpc/v1/exec {command: "get_peers"}
    S->>A: Auth check
    A->>CT: Execute({name: "get_peers"})
    CT->>N: HandleLocalFrame({type: "get_peers"})
    N-->>CT: CommandResponse
    CT-->>A: CommandResponse
    A-->>C: 200 OK + JSON
```
*Diagram 2 — Request processing flow.*

Desktop UI calls CommandTable directly without HTTP. External clients (corsa-cli, third-party tools) go through Fiber HTTP server with optional Basic Auth, then delegate to the same CommandTable.

#### Operating Modes: Desktop and Node

```mermaid
graph LR
    subgraph Desktop["Desktop Mode"]
        direction TB
        CT_D["CommandTable"]
        HTTP_D["Fiber HTTP<br/>(external access)"]
        NODE_D["node.Service"]
        MC_D["metrics.Collector"]
        DC_D["DesktopClient"]
        DMR_D["DMRouter"]
        CL_D["chatlog.Store<br/>(SQLite)"]
        WIN["Window<br/>(direct calls)"]

        WIN -->|"Execute()"| CT_D
        HTTP_D -->|"Execute()"| CT_D
        CT_D -->|"NodeProvider"| NODE_D
        CT_D -->|"MetricsProvider"| MC_D
        CT_D -->|"ChatlogProvider"| DC_D
        CT_D -->|"DMRouterProvider"| DMR_D
        MC_D --> NODE_D
        DMR_D --> NODE_D
        DMR_D --> DC_D
        DC_D --> CL_D
    end

    subgraph Node["Standalone Node Mode"]
        direction TB
        CT_N["CommandTable"]
        HTTP_N["Fiber HTTP<br/>(external access)"]
        NODE_N["node.Service"]
        MC_N["metrics.Collector"]
        NIL_C["chatlog commands<br/>unavailable (503)"]
        NIL_D["DM commands<br/>unavailable (503)"]

        HTTP_N -->|"Execute()"| CT_N
        CT_N -->|"NodeProvider"| NODE_N
        CT_N -->|"MetricsProvider"| MC_N
        MC_N --> NODE_N
        CT_N -.-> NIL_C
        CT_N -.-> NIL_D
    end
```
*Diagram 3 — Operating modes.*

In Desktop mode, all providers are available: NodeProvider, MetricsProvider, ChatlogProvider, DMRouterProvider. In Standalone Node mode, MetricsProvider and NodeProvider are active, while chatlog and DM commands return 503.

#### Command Registration

```mermaid
graph TD
    TABLE["CommandTable<br/>(single source of truth)"]

    REG_SYS["RegisterSystemCommands<br/>help · ping · hello · version"]
    REG_NET["RegisterNetworkCommands<br/>get_peers · fetch_peer_health · fetch_network_stats · add_peer"]
    REG_IDN["RegisterIdentityCommands<br/>fetch_identities · fetch_contacts · fetch_trusted_contacts"]
    REG_MSG["RegisterMessageCommands<br/>fetch_messages · fetch_message_ids · fetch_message<br/>fetch_inbox · fetch_pending_messages · fetch_delivery_receipts<br/>fetch_dm_headers · send_dm*"]
    REG_CHT["RegisterChatlogCommands<br/>fetch_chatlog · fetch_chatlog_previews · fetch_conversations*"]
    REG_NTC["RegisterNoticeCommands<br/>fetch_notices"]
    REG_MET["RegisterMetricsCommands<br/>fetch_traffic_history"]

    REG_SYS -->|"Register()"| TABLE
    REG_NET -->|"Register()"| TABLE
    REG_IDN -->|"Register()"| TABLE
    REG_MSG -->|"Register()"| TABLE
    REG_CHT -->|"Register()"| TABLE
    REG_NTC -->|"Register()"| TABLE
    REG_MET -->|"Register()"| TABLE

    TABLE --> SERVER["Fiber HTTP Server<br/>(wraps CommandTable)"]
    TABLE --> WINDOW["Desktop Window<br/>(direct access)"]

    style REG_CHT fill:#555,stroke:#888
    style REG_MSG fill:#555,stroke:#888
```
*Diagram 4 — Command registration.*

Commands marked with `*` are mode-gated: when their provider is nil (standalone node), they are registered as unavailable via `RegisterUnavailable()` — returning 503 and hidden from help. `send_dm` requires DMRouterProvider; chatlog commands require ChatlogProvider; `fetch_traffic_history` requires MetricsProvider.

### Configuration

#### Environment Variables

| Variable | Default | Description |
|---|---|---|
| `CORSA_RPC_HOST` | `127.0.0.1` | RPC server listen address |
| `CORSA_RPC_PORT` | `46464` | RPC server port |
| `CORSA_RPC_USERNAME` | _(empty)_ | HTTP Basic Auth username |
| `CORSA_RPC_PASSWORD` | _(empty)_ | HTTP Basic Auth password |

If `CORSA_RPC_USERNAME` and `CORSA_RPC_PASSWORD` are not set — access without authentication. Setting only one of them is a configuration error — `NewServer` returns an error before Fiber is created, so no port is ever bound. When auth is enabled, all 401 responses include a `WWW-Authenticate: Basic realm="corsa-rpc"` header per RFC 7235, so clients can discover the required auth scheme programmatically.

### Architecture

#### Two-Layer Design

The architecture separates command execution from transport:

**Layer 1 — CommandTable (in-process):** Pure function dispatch. Each command is a `CommandHandler func(req CommandRequest) CommandResponse`. No HTTP, no network — just a map of command names to handler functions. Desktop UI calls this directly.

**Layer 2 — Fiber HTTP Server (external):** Thin HTTP wrapper around `CommandTable`. Adds auth middleware, URL routing, JSON serialization. Only used by external clients (`corsa-cli`, `curl`).

```go
// CommandTable — single source of truth for all commands.
// RegisterAllCommands is the single registration point — both bootstrap and tests use it.
table := rpc.NewCommandTable()
rpc.RegisterAllCommands(table, nodeService, chatlogProvider, dmRouter, metricsCollector)

// Desktop: replace base ping/get_peers with diagnostic-enriched versions,
// and hello with desktop identity (Client: "desktop").
// Accepts DiagnosticProvider + NodeProvider; nil diag is a safe no-op.
rpc.RegisterDesktopOverrides(table, desktopClient, nodeService)

// Desktop UI calls directly — no HTTP
resp := table.Execute(rpc.CommandRequest{Name: "ping"})

// HTTP server wraps the same table for external access.
// Pass nodeService to enable /rpc/v1/frame (frame dispatch + fallback).
server, _ := rpc.NewServer(cfg, table, nodeService)
```

#### Command Categories

| Category | Commands | Condition |
|---|---|---|
| **system** | help, ping, hello, version | Always registered |
| **network** | get_peers, fetch_peer_health, fetch_network_stats, add_peer | Always registered |
| **identity** | fetch_identities, fetch_contacts, fetch_trusted_contacts | Always registered |
| **message** | fetch_messages, fetch_message_ids, fetch_message, fetch_inbox, fetch_pending_messages, fetch_delivery_receipts, fetch_dm_headers | Always registered |
| **message** | send_dm | Always registered; unavailable (503, hidden from help) when DMRouter is nil |
| **chatlog** | fetch_chatlog, fetch_chatlog_previews, fetch_conversations | Always registered; unavailable (503, hidden from help) when ChatlogProvider is nil |
| **notice** | fetch_notices | Always registered |
| **metrics** | fetch_traffic_history | Always registered; unavailable (503, hidden from help) when MetricsProvider is nil |

#### Dependency Injection

Commands receive dependencies through provider interfaces:

```go
// NodeProvider — implemented by node.Service
type NodeProvider interface {
    HandleLocalFrame(frame protocol.Frame) protocol.Frame
    Address() string
    ClientVersion() string
}

// ChatlogProvider — implemented by DesktopClient (nil for standalone node)
type ChatlogProvider interface {
    FetchChatlog(topic, peerAddress string) (string, error)
    FetchChatlogPreviews() (string, error)
    FetchConversations() (string, error)
}
```

#### Error Model

`CommandResponse` carries a typed `ErrorKind` so the HTTP layer can map errors to correct status codes without inspecting error messages.

| ErrorKind | HTTP Status | When |
|---|---|---|
| `ErrValidation` | 400 Bad Request | Missing or invalid arguments from caller |
| `ErrNotFound` | 404 Not Found | Truly unknown command (not in CommandTable at all) |
| `ErrUnavailable` | 503 Service Unavailable | Command exists but not available in this mode (e.g. chatlog on standalone node) |
| `ErrInternal` | 500 Internal Server Error | Provider failure, serialization error |

Mode-gated commands are registered via `RegisterUnavailable()` — they exist in the table (so `Has()` returns true and `/exec` returns 503 instead of 404), but don't appear in `Commands()` (help, autocomplete). This ensures `/rpc/v1/exec` and legacy routes return identical status codes for the same command.

Command handlers use `validationError()` for input problems and `internalError()` for provider/system failures. The HTTP layer calls `resp.ErrorKind.HTTPStatus()` uniformly across all dispatch paths.

Malformed JSON body on legacy arg routes returns 400 immediately, before command dispatch. Empty body is accepted — commands with optional arguments use defaults.

### API Reference

#### Universal Dispatch

**POST /rpc/v1/exec** — execute any registered command.

Request:
```json
{"command": "ping", "args": {}}
```

Response: command-specific JSON.

#### Raw Frame Dispatch

**POST /rpc/v1/frame** — accept a raw JSON protocol frame and dispatch it through `CommandTable`, falling back to `HandleLocalFrame` for unregistered frame types. This matches the desktop console's `ExecuteConsoleCommand` behavior.

Request: a raw protocol frame JSON object with a `type` field:
```json
{"type": "fetch_chatlog", "topic": "dm", "address": "peer-addr-123"}
```

Response: the command's response (JSON).

Dispatch logic:
1. The frame's `type` is extracted as the command name, wire field names are normalized to RPC arg names via `normalizeFrameArgs` (e.g. `peers` → `address`, `address` → `peer_address` for chatlog).
2. If the command is registered in `CommandTable`, it is dispatched there. Registered handlers may rebuild the frame from scratch (e.g. `hello` ignores caller-supplied `Client`/`ClientVersion` and uses its own values; desktop-enriched `ping` builds a per-peer diagnostic report instead of proxying the frame).
3. If the command is not found in `CommandTable`, the raw frame is forwarded to `HandleLocalFrame` with all caller-supplied wire fields preserved.

This endpoint is only available when the server is created with a `NodeProvider`.

The Go client (`rpc.Client.ExecuteCommand`) automatically detects raw JSON input and routes it to `/frame` instead of `/exec`.

#### Legacy Routes

All endpoints accept `POST` with `Content-Type: application/json`. These routes map to the same CommandTable commands.

##### System

**POST /rpc/v1/system/help** — list all available commands.

Response:
```json
{
  "version": "1.0",
  "commands": [
    {"name": "help", "description": "List all available RPC commands", "category": "system"},
    {"name": "get_peers", "description": "Get list of connected peers", "category": "network"}
  ]
}
```

`version` is the help response schema version (currently `"1.0"`). It tracks the structure of the help response itself — not the protocol or client version. Clients can use it to detect format changes (e.g. new fields in command metadata). Bump when the help response structure changes.

**POST /rpc/v1/system/ping** — send local ping, returns pong response. In desktop mode, replaced by `DiagnosticProvider` — pings every connected peer over TCP and reports per-peer status.

**POST /rpc/v1/system/hello** — send hello frame for identification. The handler populates `Version`, `MinimumProtocolVersion`, `Client`, `ClientVersion`, and `ClientBuild` before forwarding to the node — without these fields the handshake is rejected. The standalone node handler identifies as `Client: "rpc"`; in desktop mode, `RegisterDesktopOverrides` replaces it with `Client: "desktop"` and the desktop application version. `ClientBuild` is a monotonically increasing integer that peers compare to detect newer software releases.

**POST /rpc/v1/system/version** — client and protocol version.

Response:
```json
{
  "client_version": "0.21-alpha",
  "protocol_version": 3,
  "node_address": "a1b2c3d4..."
}
```

##### Network

**POST /rpc/v1/network/peers** — list all known peers. In desktop mode, returns enriched JSON with health data and per-peer `client_version` / `client_build`. Peers without a known build report `client_build: 0`.

Response (desktop mode):
```json
{
  "type": "peers",
  "count": 1,
  "total": 2,
  "connected": [
    {
      "address": "65.108.204.190:64646",
      "network": "ipv4",
      "client_version": "0.19-alpha",
      "client_build": 19,
      "state": "healthy",
      "connected": true,
      "bytes_sent": 1024,
      "bytes_received": 2048,
      "total_traffic": 3072
    }
  ],
  "pending": [],
  "known_only": ["192.0.2.1:64646"],
  "peers": [...]
}
```

**POST /rpc/v1/network/health** — peer health status.

**POST /rpc/v1/network/stats** — aggregated network traffic statistics.

Response includes per-peer bytes sent/received and total node traffic.

**POST /rpc/v1/network/add_peer** — add a peer.

Request: `{"address": "host:port"}`

##### Metrics

**POST /rpc/v1/metrics/traffic_history** — rolling traffic history (1 sample/sec, 1 hour window).

Response contains an array of per-second samples with bytes sent/received deltas and cumulative totals. The buffer holds up to 3600 entries (1 hour). Samples are ordered oldest-first.

##### Identity

**POST /rpc/v1/identity/identities** — all known identities.

**POST /rpc/v1/identity/contacts** — all contacts.

**POST /rpc/v1/identity/trusted_contacts** — trusted contacts (TOFU pinned).

##### Messages

**POST /rpc/v1/message/list** — request: `{"topic": "global"}`

**POST /rpc/v1/message/ids** — request: `{"topic": "global"}`

**POST /rpc/v1/message/get** — request: `{"topic": "dm", "id": "message-uuid"}`

**POST /rpc/v1/message/inbox** — request: `{"topic": "dm", "recipient": "address (optional, defaults to self)"}`

**POST /rpc/v1/message/pending** — request: `{"topic": "dm"}`

**POST /rpc/v1/message/receipts** — request: `{"recipient": "address (optional, defaults to self)"}`

**POST /rpc/v1/message/dm_headers** — fetch direct message headers.

**POST /rpc/v1/message/send_dm** — queue a direct message for delivery (desktop mode only, returns 503 on standalone node).

Request: `{"to": "address", "body": "message text"}`

Response: `{"status": "queued", "to": "address"}`. The message is accepted for async delivery via DMRouter. Actual delivery happens in a background goroutine — check delivery receipts for confirmation.

##### Chat History

Available only in desktop mode. In standalone node mode returns 503.

**POST /rpc/v1/chatlog/entries** — request: `{"topic": "dm", "peer_address": "address"}`

**POST /rpc/v1/chatlog/previews** — last message from each peer.

**POST /rpc/v1/chatlog/conversations** — metadata for all conversations.

##### Notices

**POST /rpc/v1/notice/list** — anonymous encrypted notices (Gazeta).

### corsa-cli

Thin console client for the RPC server. No local command table, no aliases — passes the command name and arguments directly to `POST /rpc/v1/exec`. The server is the single source of truth. `corsa-cli help` fetches the command list from the server.

#### Build

```bash
make build-cli-all
```

#### Usage

```bash
# List available commands (fetched from server)
corsa-cli help

# Simple commands (no arguments)
corsa-cli ping
corsa-cli version
corsa-cli get_peers
corsa-cli fetch_peer_health

# Positional arguments (matches help output syntax)
corsa-cli add_peer 1.2.3.4:8080
corsa-cli send_dm peer-addr hello world
corsa-cli fetch_chatlog dm abc123

# Named arguments (key=value)
corsa-cli add_peer address=1.2.3.4:8080
corsa-cli send_dm to=peer-addr body="hello world"
corsa-cli fetch_messages topic=dm
corsa-cli fetch_chatlog topic=dm peer_address=abc123

# JSON argument (single quoted JSON object)
corsa-cli add_peer '{"address": "1.2.3.4:8080"}'
corsa-cli send_dm '{"to": "peer-addr", "body": "hello world"}'

# With -named flag (explicit key=value mode)
corsa-cli -named fetch_inbox topic=dm recipient=peer-addr

# Authentication
corsa-cli --username admin --password secret get_peers

# Remote host
corsa-cli --host 192.168.1.100 --port 46464 get_peers
```

#### Flags

| Flag | Default | Description |
|---|---|---|
| `--host` | `127.0.0.1` | RPC server host |
| `--port` | `46464` | RPC server port |
| `--username` | _(empty)_ | Username |
| `--password` | _(empty)_ | Password |
| `--named` | `false` | Interpret arguments as key=value pairs |

### Integration

#### Registration

`RegisterAllCommands(table, node, chatlog, dmRouter, metricsProvider)` is the single registration point for all command groups. Both the application bootstrap and tests call this function to populate a `CommandTable`. Pass `nil` for `chatlog`, `dmRouter`, or `metricsProvider` to simulate standalone node mode — those commands are registered as unavailable (503).

#### Desktop Application

The desktop application creates a `CommandTable`, calls `RegisterAllCommands` with all providers, then calls `RegisterDesktopOverrides(table, client, client)` to replace base `ping`, `get_peers`, and `hello` handlers with desktop-enriched versions. The enriched `ping` opens TCP sessions to every connected peer and reports per-peer status. The enriched `get_peers` merges the raw peer list with health data and categorizes peers into connected/pending/known_only. The enriched `hello` identifies as `Client: "desktop"` with the desktop application version instead of the generic `Client: "rpc"`. `RegisterDesktopOverrides` accepts a `DiagnosticProvider` and a `NodeProvider` — passing `nil` diag is a no-op. The resulting table is passed to both the Fiber HTTP server (for external access) and the Window (for direct UI access). All 6 command categories are registered, including chatlog and DM commands.

#### Standalone Node (corsa-node)

The standalone node creates a `CommandTable` and calls `RegisterAllCommands` with nil providers for chatlog, DMRouter, and metricsProvider. Commands that require unavailable providers (`fetch_chatlog`, `fetch_chatlog_previews`, `fetch_conversations`, `send_dm`, `fetch_traffic_history`) are registered as unavailable — they return 503 via both `/rpc/v1/exec` and legacy endpoints, but do not appear in help output. Note: `fetch_dm_headers` is always registered because it uses only `NodeProvider`.

#### Go Client (rpc.Client)

`rpc.Client` is the exported HTTP client for the RPC server. `ExecuteCommand(input)` routes input based on format: raw JSON frames (starting with `{`) are sent to `POST /rpc/v1/frame` where the server applies CommandTable dispatch (registered commands may normalize or rebuild the frame; only unregistered types preserve all wire fields via HandleLocalFrame fallback); named commands are parsed via `ParseConsoleInput` into `{command, args}` and sent to `POST /rpc/v1/exec`. No legacy route logic — `ParseConsoleInput` is the single source of truth for positional-to-named arg mapping.

#### ParseConsoleInput

`ParseConsoleInput` accepts two input formats: named commands (`send_dm addr hello world`) and raw JSON frames (`{"type":"ping"}`). Command names are case-insensitive. For JSON input, the `type` field becomes the command name and all fields are passed as args. Used by both the UI console (in-process) and `rpc.Client` (over HTTP).

**Frame field normalization.** Protocol wire frames use different field names than RPC handlers. `normalizeFrameArgs` bridges the gap so pasting real wire frames into the console works. Aliases are applied only when the RPC-expected field is absent; if both are present, the RPC field wins.

| Command | Wire field | RPC field | Transformation |
|---|---|---|---|
| `add_peer` | `peers` (array) | `address` (string) | `peers[0]` → `address` |
| `send_dm` | `recipient` | `to` | rename |
| `fetch_chatlog` | `address` | `peer_address` | rename |
| *(pagination)* | `count` | `offset` | rename (all commands) |

#### UI Console

The UI console executes commands directly through `CommandTable` — no HTTP round-trip. Command suggestions are loaded synchronously from `CommandTable.Commands()` at initialization. Two special behaviors differ from the API path: `help` renders a human-readable categorized text (with defaults and self-address) instead of machine JSON, and unknown commands fall back to `ExecuteConsoleCommand` for raw protocol frame passthrough to `HandleLocalFrame`.

### Testing

```bash
# Run all RPC tests
go test ./internal/core/rpc/... -v

# Run CLI tests (parseArgs, execRPC)
go test ./cmd/corsa-cli/... -v

# Run tests for specific command categories
go test ./internal/core/rpc/... -run TestSystem -v
go test ./internal/core/rpc/... -run TestNetwork -v

# Run CommandTable unit tests
go test ./internal/core/rpc/... -run TestCommandTable -v

# Run console parser tests
go test ./internal/core/rpc/... -run TestParseConsole -v

# Run Go client tests
go test ./internal/core/rpc/... -run TestClient -v
```

Tests use Fiber `app.Test()` for HTTP-layer tests. CommandTable can be tested directly without HTTP. CLI tests cover `parseArgs` (JSON, key=value, positional) and `execRPC` (endpoint, auth). Client tests use `httptest.NewServer` to verify routing: named commands → `/rpc/v1/exec`, raw JSON frames → `/rpc/v1/frame`.

---

## Русский

### Обзор

RPC слой обеспечивает диспетчеризацию команд для управления нодой CORSA. Все команды регистрируются в `CommandTable` — чисто внутрипроцессной таблице диспетчеризации функций без зависимости от HTTP.

Desktop UI вызывает `CommandTable` напрямую (без HTTP round-trip). Внешние клиенты (`corsa-cli`, сторонние инструменты) обращаются к командам через тонкую HTTP-обёртку на Fiber v3, которая делегирует в тот же `CommandTable`. Это гарантирует идентичное поведение для всех вызывающих.

HTTP RPC сервер по умолчанию слушает `127.0.0.1:46464`. Если username и password не указаны — доступ без авторизации.

### Диаграмма взаимодействия

#### Общая архитектура

```mermaid
graph TB
    subgraph UI_Layer["UI (опционально, только desktop)"]
        UI["Desktop UI<br/>(Gio)"]
    end

    subgraph External["Внешние клиенты"]
        CLI["corsa-cli<br/>(HTTP POST)"]
        EXT["Сторонние инструменты<br/>(curl, скрипты)"]
    end

    subgraph Node_Layer["Ядро ноды (всегда присутствует)"]
        CT["CommandTable<br/>(внутрипроцессная диспетчеризация)"]

        subgraph HTTP["HTTP-обёртка — Fiber v3<br/>127.0.0.1:46464"]
            AUTH["Авторизация<br/>HTTP Basic Auth (опционально)"]
            EXEC["POST /rpc/v1/exec<br/>(универсальная диспетчеризация)"]
            FRAME["POST /rpc/v1/frame<br/>(диспетчеризация фреймов + fallback)"]
            LEGACY["Legacy-маршруты<br/>/system/* /network/* ..."]
        end

        NP["NodeProvider<br/>HandleLocalFrame() · Address() · ClientVersion()"]
        NODE["node.Service<br/>(сетевой уровень)"]
        MP["MetricsProvider<br/>TrafficSnapshot()"]
        MC["metrics.Collector<br/>(сбор трафика, кольцевой буфер)"]
    end

    subgraph Desktop_Providers["Desktop-провайдеры (опционально, nil на standalone-ноде)"]
        CP["ChatlogProvider<br/>FetchChatlog() · FetchChatlogPreviews() · FetchConversations()"]
        DP["DMRouterProvider<br/>Snapshot() · SendMessage()"]
        DC["DesktopClient<br/>(chatlog.Store / SQLite)"]
        DMR["DMRouter<br/>(состояние UI, разговоры,<br/>маршрутизация сообщений)"]
    end

    MESH["TCP Mesh-сеть<br/>(peer-to-peer)"]

    UI -->|"прямой вызов<br/>(без HTTP)"| CT

    CLI --> AUTH
    EXT --> AUTH
    AUTH --> EXEC
    AUTH --> FRAME
    AUTH --> LEGACY
    EXEC --> CT
    FRAME -->|"CommandTable первый,<br/>fallback HandleLocalFrame"| CT
    LEGACY --> CT

    CT --> NP
    CT --> MP
    CT -.->|"nil на standalone-ноде"| CP
    CT -.->|"nil на standalone-ноде"| DP

    NP --> NODE
    MP --> MC
    MC -->|"fetch_network_stats"| NODE
    CP --> DC
    DP --> DMR
    DMR --> NODE
    DMR --> DC
    NODE --> MESH
```
*Диаграмма 1 — Общая архитектура.*

Слой ядра ноды всегда присутствует и включает CommandTable, NodeProvider, MetricsProvider и HTTP-обёртку. Desktop-провайдеры (ChatlogProvider, DMRouterProvider) опциональны — на standalone-ноде они равны nil, их команды возвращают 503 и скрыты из help. MetricsProvider (metrics.Collector) собирает сэмплы трафика от node.Service через `fetch_network_stats` и хранит их в кольцевом буфере для `fetch_traffic_history`.

#### Поток обработки запроса

```mermaid
sequenceDiagram
    participant UI as Desktop UI
    participant CT as CommandTable
    participant N as NodeProvider
    participant NS as node.Service

    Note over UI,CT: Desktop UI — прямой вызов (без HTTP)
    UI->>CT: Execute({name: "get_peers"})
    CT->>N: HandleLocalFrame({type: "get_peers"})
    N->>NS: HandleLocalFrame(frame)
    NS-->>N: Frame{type: "peers", peers: [...]}
    N-->>CT: CommandResponse{Data: json}
    CT-->>UI: CommandResponse

    Note over UI,CT: Внешний клиент — через HTTP
    participant C as corsa-cli
    participant S as Fiber HTTP сервер
    participant A as Авторизация

    C->>S: POST /rpc/v1/exec {command: "get_peers"}
    S->>A: Проверка авторизации
    A->>CT: Execute({name: "get_peers"})
    CT->>N: HandleLocalFrame({type: "get_peers"})
    N-->>CT: CommandResponse
    CT-->>A: CommandResponse
    A-->>C: 200 OK + JSON
```
*Диаграмма 2 — Поток обработки запроса.*

Desktop UI вызывает CommandTable напрямую без HTTP. Внешние клиенты (corsa-cli, сторонние инструменты) проходят через Fiber HTTP сервер с опциональной Basic Auth, затем делегируют в тот же CommandTable.

#### Режимы работы: Desktop и Node

```mermaid
graph LR
    subgraph Desktop["Режим Desktop"]
        direction TB
        CT_D["CommandTable"]
        HTTP_D["Fiber HTTP<br/>(внешний доступ)"]
        NODE_D["node.Service"]
        MC_D["metrics.Collector"]
        DC_D["DesktopClient"]
        DMR_D["DMRouter"]
        CL_D["chatlog.Store<br/>(SQLite)"]
        WIN["Window<br/>(прямые вызовы)"]

        WIN -->|"Execute()"| CT_D
        HTTP_D -->|"Execute()"| CT_D
        CT_D -->|"NodeProvider"| NODE_D
        CT_D -->|"MetricsProvider"| MC_D
        CT_D -->|"ChatlogProvider"| DC_D
        CT_D -->|"DMRouterProvider"| DMR_D
        MC_D --> NODE_D
        DMR_D --> NODE_D
        DMR_D --> DC_D
        DC_D --> CL_D
    end

    subgraph Node["Режим Standalone Node"]
        direction TB
        CT_N["CommandTable"]
        HTTP_N["Fiber HTTP<br/>(внешний доступ)"]
        NODE_N["node.Service"]
        MC_N["metrics.Collector"]
        NIL_C["chatlog-команды<br/>недоступны (503)"]
        NIL_D["DM-команды<br/>недоступны (503)"]

        HTTP_N -->|"Execute()"| CT_N
        CT_N -->|"NodeProvider"| NODE_N
        CT_N -->|"MetricsProvider"| MC_N
        MC_N --> NODE_N
        CT_N -.-> NIL_C
        CT_N -.-> NIL_D
    end
```
*Диаграмма 3 — Режимы работы.*

В режиме Desktop доступны все провайдеры: NodeProvider, MetricsProvider, ChatlogProvider, DMRouterProvider. В режиме Standalone Node активны MetricsProvider и NodeProvider, а chatlog- и DM-команды возвращают 503.

#### Регистрация команд

```mermaid
graph TD
    TABLE["CommandTable<br/>(единственный источник истины)"]

    REG_SYS["RegisterSystemCommands<br/>help · ping · hello · version"]
    REG_NET["RegisterNetworkCommands<br/>get_peers · fetch_peer_health · fetch_network_stats · add_peer"]
    REG_IDN["RegisterIdentityCommands<br/>fetch_identities · fetch_contacts · fetch_trusted_contacts"]
    REG_MSG["RegisterMessageCommands<br/>fetch_messages · fetch_message_ids · fetch_message<br/>fetch_inbox · fetch_pending_messages · fetch_delivery_receipts<br/>fetch_dm_headers · send_dm*"]
    REG_CHT["RegisterChatlogCommands<br/>fetch_chatlog · fetch_chatlog_previews · fetch_conversations*"]
    REG_NTC["RegisterNoticeCommands<br/>fetch_notices"]
    REG_MET["RegisterMetricsCommands<br/>fetch_traffic_history"]

    REG_SYS -->|"Register()"| TABLE
    REG_NET -->|"Register()"| TABLE
    REG_IDN -->|"Register()"| TABLE
    REG_MSG -->|"Register()"| TABLE
    REG_CHT -->|"Register()"| TABLE
    REG_NTC -->|"Register()"| TABLE
    REG_MET -->|"Register()"| TABLE

    TABLE --> SERVER["Fiber HTTP сервер<br/>(обёртка над CommandTable)"]
    TABLE --> WINDOW["Desktop Window<br/>(прямой доступ)"]

    style REG_CHT fill:#555,stroke:#888
    style REG_MSG fill:#555,stroke:#888
```
*Диаграмма 4 — Регистрация команд.*

Команды с `*` являются mode-gated: при nil-провайдере (standalone нода) они регистрируются как недоступные через `RegisterUnavailable()` — возвращают 503 и скрыты из help. `send_dm` требует DMRouterProvider; chatlog-команды требуют ChatlogProvider; `fetch_traffic_history` требует MetricsProvider.

### Конфигурация

#### Переменные окружения

| Переменная | По умолчанию | Описание |
|---|---|---|
| `CORSA_RPC_HOST` | `127.0.0.1` | Адрес прослушивания RPC сервера |
| `CORSA_RPC_PORT` | `46464` | Порт RPC сервера |
| `CORSA_RPC_USERNAME` | _(пусто)_ | Имя пользователя для HTTP Basic Auth |
| `CORSA_RPC_PASSWORD` | _(пусто)_ | Пароль для HTTP Basic Auth |

Если `CORSA_RPC_USERNAME` и `CORSA_RPC_PASSWORD` не указаны — доступ без авторизации. Указание только одного из них — ошибка конфигурации: `NewServer` возвращает ошибку до создания Fiber, порт не занимается. Когда авторизация включена, все 401-ответы содержат заголовок `WWW-Authenticate: Basic realm="corsa-rpc"` по RFC 7235, позволяя клиентам программно определить требуемую схему авторизации.

### Архитектура

#### Двухслойный дизайн

Архитектура разделяет выполнение команд и транспорт:

**Слой 1 — CommandTable (внутрипроцессный):** Чистая диспетчеризация функций. Каждая команда — это `CommandHandler func(req CommandRequest) CommandResponse`. Без HTTP, без сети — просто map имён команд на функции-обработчики. Desktop UI вызывает напрямую.

**Слой 2 — Fiber HTTP сервер (внешний):** Тонкая HTTP-обёртка над `CommandTable`. Добавляет middleware авторизации, URL-маршрутизацию, JSON-сериализацию. Используется только внешними клиентами (`corsa-cli`, `curl`).

```go
// CommandTable — единственный источник истины для всех команд.
// RegisterAllCommands — единая точка регистрации, используемая и bootstrap, и тестами.
table := rpc.NewCommandTable()
rpc.RegisterAllCommands(table, nodeService, chatlogProvider, dmRouter, metricsCollector)

// Desktop: замена базовых ping/get_peers на диагностически обогащённые версии,
// а также hello на desktop-идентификацию (Client: "desktop").
// Принимает DiagnosticProvider + NodeProvider; nil diag — безопасный no-op.
rpc.RegisterDesktopOverrides(table, desktopClient, nodeService)

// Desktop UI вызывает напрямую — без HTTP
resp := table.Execute(rpc.CommandRequest{Name: "ping"})

// HTTP сервер оборачивает ту же таблицу для внешнего доступа.
// nodeService включает /rpc/v1/frame (диспетчеризация фреймов + fallback).
server, _ := rpc.NewServer(cfg, table, nodeService)
```

#### Категории команд

| Категория | Команды | Условие |
|---|---|---|
| **system** | help, ping, hello, version | Всегда зарегистрированы |
| **network** | get_peers, fetch_peer_health, fetch_network_stats, add_peer | Всегда зарегистрированы |
| **identity** | fetch_identities, fetch_contacts, fetch_trusted_contacts | Всегда зарегистрированы |
| **message** | fetch_messages, fetch_message_ids, fetch_message, fetch_inbox, fetch_pending_messages, fetch_delivery_receipts, fetch_dm_headers | Всегда зарегистрированы |
| **message** | send_dm | Всегда зарегистрирована; недоступна (503, скрыта из help) при DMRouter = nil |
| **chatlog** | fetch_chatlog, fetch_chatlog_previews, fetch_conversations | Всегда зарегистрированы; недоступны (503, скрыты из help) при ChatlogProvider = nil |
| **notice** | fetch_notices | Всегда зарегистрированы |
| **metrics** | fetch_traffic_history | Всегда зарегистрирована; недоступна (503, скрыта из help) при MetricsProvider = nil |

#### Внедрение зависимостей

Команды получают зависимости через интерфейсы провайдеров:

```go
// NodeProvider — реализуется node.Service
type NodeProvider interface {
    HandleLocalFrame(frame protocol.Frame) protocol.Frame
    Address() string
    ClientVersion() string
}

// ChatlogProvider — реализуется DesktopClient (nil для standalone ноды)
type ChatlogProvider interface {
    FetchChatlog(topic, peerAddress string) (string, error)
    FetchChatlogPreviews() (string, error)
    FetchConversations() (string, error)
}
```

#### Модель ошибок

`CommandResponse` содержит типизированный `ErrorKind`, чтобы HTTP-слой мог корректно маппить ошибки на статус-коды без анализа текста сообщений.

| ErrorKind | HTTP статус | Когда |
|---|---|---|
| `ErrValidation` | 400 Bad Request | Отсутствующие или невалидные аргументы от вызывающей стороны |
| `ErrNotFound` | 404 Not Found | Команда полностью неизвестна (не в CommandTable) |
| `ErrUnavailable` | 503 Service Unavailable | Команда существует, но недоступна в этом режиме (например chatlog на standalone-ноде) |
| `ErrInternal` | 500 Internal Server Error | Ошибка провайдера, ошибка сериализации |

Mode-gated команды регистрируются через `RegisterUnavailable()` — они присутствуют в таблице (поэтому `Has()` возвращает true, а `/exec` возвращает 503 вместо 404), но не появляются в `Commands()` (help, autocomplete). Это гарантирует, что `/rpc/v1/exec` и legacy routes возвращают одинаковые статус-коды для одной и той же команды.

Обработчики команд используют `validationError()` для ошибок ввода и `internalError()` для ошибок провайдеров/системы. HTTP-слой единообразно вызывает `resp.ErrorKind.HTTPStatus()` во всех путях диспетчеризации.

Некорректный JSON body на legacy arg routes возвращает 400 немедленно, до диспетчеризации команды. Пустое тело запроса допускается — команды с опциональными аргументами используют значения по умолчанию.

### Справочник API

#### Универсальная диспетчеризация

**POST /rpc/v1/exec** — выполнение любой зарегистрированной команды.

Запрос:
```json
{"command": "ping", "args": {}}
```

Ответ: JSON, специфичный для команды.

#### Диспетчеризация фреймов

**POST /rpc/v1/frame** — принимает сырой JSON-фрейм протокола и диспетчеризует его через `CommandTable`, с fallback на `HandleLocalFrame` для незарегистрированных типов фреймов. Повторяет поведение `ExecuteConsoleCommand` в desktop-консоли.

Запрос: сырой JSON-объект фрейма протокола с полем `type`:
```json
{"type": "fetch_chatlog", "topic": "dm", "address": "peer-addr-123"}
```

Ответ: JSON ответа команды.

Логика диспетчеризации:
1. Из фрейма извлекается `type` как имя команды, wire-имена полей нормализуются в RPC-аргументы через `normalizeFrameArgs` (например, `peers` → `address`, `address` → `peer_address` для chatlog).
2. Если команда зарегистрирована в `CommandTable`, она диспетчеризуется туда. Зарегистрированные обработчики могут пересобрать фрейм с нуля (например, `hello` игнорирует переданные `Client`/`ClientVersion` и использует собственные значения; desktop-обогащённый `ping` строит диагностический отчёт по каждому пиру вместо проксирования фрейма).
3. Если команда не найдена в `CommandTable`, сырой фрейм пересылается в `HandleLocalFrame` с сохранением всех wire-полей отправителя.

Эндпоинт доступен только когда сервер создан с `NodeProvider`.

Go-клиент (`rpc.Client.ExecuteCommand`) автоматически определяет сырой JSON на входе и направляет его на `/frame` вместо `/exec`.

#### Legacy-маршруты

Все эндпоинты принимают `POST` с `Content-Type: application/json`. Эти маршруты отображаются на те же команды CommandTable.

##### Системные

**POST /rpc/v1/system/help** — список всех доступных команд.

Ответ:
```json
{
  "version": "1.0",
  "commands": [
    {"name": "help", "description": "Список всех доступных RPC команд", "category": "system"},
    {"name": "get_peers", "description": "Получить список подключённых пиров", "category": "network"}
  ]
}
```

`version` — версия схемы ответа help (сейчас `"1.0"`). Отслеживает структуру самого ответа help, а не версию протокола или клиента. Клиенты могут использовать это поле для обнаружения изменений формата (например, новых полей в метаданных команд). Инкрементируется при изменении структуры ответа help.

**POST /rpc/v1/system/ping** — локальный пинг, возвращает pong-ответ. В desktop-режиме заменяется через `DiagnosticProvider` — пингует каждого подключённого пира по TCP и сообщает статус по каждому.

**POST /rpc/v1/system/hello** — отправка hello-фрейма для идентификации. Обработчик заполняет `Version`, `MinimumProtocolVersion`, `Client`, `ClientVersion` и `ClientBuild` перед отправкой ноде — без этих полей handshake отклоняется. Standalone-нода идентифицируется как `Client: "rpc"`; в desktop-режиме `RegisterDesktopOverrides` заменяет обработчик на `Client: "desktop"` с версией desktop-приложения. `ClientBuild` — монотонно возрастающий целочисленный номер сборки, по которому пиры определяют наличие новых версий ПО.

**POST /rpc/v1/system/version** — версия клиента и протокола.

Ответ:
```json
{
  "client_version": "0.21-alpha",
  "protocol_version": 3,
  "node_address": "a1b2c3d4..."
}
```

##### Сеть

**POST /rpc/v1/network/peers** — список всех известных пиров. В desktop-режиме возвращает обогащённый JSON с данными о здоровье и полями `client_version` / `client_build` для каждого пира. Пиры без известного билда возвращают `client_build: 0`.

Ответ (desktop-режим):
```json
{
  "type": "peers",
  "count": 1,
  "total": 2,
  "connected": [
    {
      "address": "65.108.204.190:64646",
      "network": "ipv4",
      "client_version": "0.19-alpha",
      "client_build": 19,
      "state": "healthy",
      "connected": true,
      "bytes_sent": 1024,
      "bytes_received": 2048,
      "total_traffic": 3072
    }
  ],
  "pending": [],
  "known_only": ["192.0.2.1:64646"],
  "peers": [...]
}
```

**POST /rpc/v1/network/health** — состояние здоровья пиров.

**POST /rpc/v1/network/stats** — агрегированная статистика сетевого трафика.

Ответ включает количество байт отправленных/полученных по каждому пиру и общий трафик ноды.

**POST /rpc/v1/network/add_peer** — добавление пира.

Запрос: `{"address": "host:port"}`

##### Метрики

**POST /rpc/v1/metrics/traffic_history** — история трафика (1 семпл/сек, окно 1 час).

Ответ содержит массив посекундных семплов с дельтами байт отправленных/полученных и кумулятивными итогами. Буфер вмещает до 3600 записей (1 час). Семплы отсортированы от старых к новым.

##### Идентификация

**POST /rpc/v1/identity/identities** — все известные идентификаторы.

**POST /rpc/v1/identity/contacts** — все контакты.

**POST /rpc/v1/identity/trusted_contacts** — доверенные контакты (закреплённые по TOFU).

##### Сообщения

**POST /rpc/v1/message/list** — запрос: `{"topic": "global"}`

**POST /rpc/v1/message/ids** — запрос: `{"topic": "global"}`

**POST /rpc/v1/message/get** — запрос: `{"topic": "dm", "id": "message-uuid"}`

**POST /rpc/v1/message/inbox** — запрос: `{"topic": "dm", "recipient": "address (опционально, по умолчанию — свой)"}`

**POST /rpc/v1/message/pending** — запрос: `{"topic": "dm"}`

**POST /rpc/v1/message/receipts** — запрос: `{"recipient": "address (опционально, по умолчанию — свой)"}`

**POST /rpc/v1/message/dm_headers** — получение заголовков прямых сообщений.

**POST /rpc/v1/message/send_dm** — постановка прямого сообщения в очередь доставки (только desktop-режим, возвращает 503 на standalone-ноде).

Запрос: `{"to": "address", "body": "текст сообщения"}`

Ответ: `{"status": "queued", "to": "address"}`. Сообщение принято для асинхронной доставки через DMRouter. Фактическая отправка происходит в фоновой goroutine — используйте delivery receipts для подтверждения.

##### История чатов

Доступно только в desktop-режиме. В режиме standalone-ноды возвращает 503.

**POST /rpc/v1/chatlog/entries** — запрос: `{"topic": "dm", "peer_address": "address"}`

**POST /rpc/v1/chatlog/previews** — последнее сообщение от каждого пира.

**POST /rpc/v1/chatlog/conversations** — метаданные всех разговоров.

##### Уведомления

**POST /rpc/v1/notice/list** — анонимные зашифрованные уведомления (Gazeta).

### corsa-cli

Тонкий консольный клиент RPC сервера. Без локальной таблицы команд, без алиасов — передаёт имя команды и аргументы напрямую в `POST /rpc/v1/exec`. Сервер является единственным источником истины. `corsa-cli help` получает список команд с сервера.

#### Сборка

```bash
make build-cli-all
```

#### Использование

```bash
# Список доступных команд (с сервера)
corsa-cli help

# Простые команды (без аргументов)
corsa-cli ping
corsa-cli version
corsa-cli get_peers
corsa-cli fetch_peer_health

# Позиционные аргументы (совпадают с синтаксисом help)
corsa-cli add_peer 1.2.3.4:8080
corsa-cli send_dm peer-addr hello world
corsa-cli fetch_chatlog dm abc123

# Именованные аргументы (key=value)
corsa-cli add_peer address=1.2.3.4:8080
corsa-cli send_dm to=peer-addr body="hello world"
corsa-cli fetch_messages topic=dm
corsa-cli fetch_chatlog topic=dm peer_address=abc123

# JSON аргумент (один JSON-объект в кавычках)
corsa-cli add_peer '{"address": "1.2.3.4:8080"}'
corsa-cli send_dm '{"to": "peer-addr", "body": "hello world"}'

# С флагом -named (явный режим key=value)
corsa-cli -named fetch_inbox topic=dm recipient=peer-addr

# Авторизация
corsa-cli --username admin --password secret get_peers

# Удалённый хост
corsa-cli --host 192.168.1.100 --port 46464 get_peers
```

#### Флаги

| Флаг | По умолчанию | Описание |
|---|---|---|
| `--host` | `127.0.0.1` | Хост RPC сервера |
| `--port` | `46464` | Порт RPC сервера |
| `--username` | _(пусто)_ | Имя пользователя |
| `--password` | _(пусто)_ | Пароль |
| `--named` | `false` | Интерпретировать аргументы как key=value пары |

### Интеграция

#### Регистрация

`RegisterAllCommands(table, node, chatlog, dmRouter, metricsProvider)` — единая точка регистрации всех групп команд. И bootstrap приложения, и тесты вызывают эту функцию для заполнения `CommandTable`. Передайте `nil` для `chatlog`, `dmRouter` или `metricsProvider` для режима standalone ноды — эти команды будут зарегистрированы как недоступные (503).

#### Desktop приложение

Desktop приложение создаёт `CommandTable`, вызывает `RegisterAllCommands` со всеми провайдерами, затем вызывает `RegisterDesktopOverrides(table, client, client)` для замены базовых обработчиков `ping`, `get_peers` и `hello` на desktop-обогащённые версии. Обогащённый `ping` открывает TCP-сессии ко всем подключённым пирам и сообщает статус по каждому. Обогащённый `get_peers` объединяет список пиров с данными о здоровье и категоризирует пиров на connected/pending/known_only. Обогащённый `hello` идентифицируется как `Client: "desktop"` с версией desktop-приложения вместо генерического `Client: "rpc"`. `RegisterDesktopOverrides` принимает `DiagnosticProvider` и `NodeProvider` — передача `nil` diag является no-op. Результирующая таблица передаётся и Fiber HTTP серверу (для внешнего доступа), и Window (для прямого доступа UI). Все 6 категорий команд регистрируются, включая chatlog и DM.

#### Standalone нода (corsa-node)

Standalone нода создаёт `CommandTable` и вызывает `RegisterAllCommands` с nil-провайдерами для chatlog, DMRouter и metricsProvider. Команды, требующие недоступных провайдеров (`fetch_chatlog`, `fetch_chatlog_previews`, `fetch_conversations`, `send_dm`, `fetch_traffic_history`), регистрируются как недоступные — возвращают 503 через `/rpc/v1/exec` и legacy эндпоинты, но не отображаются в help. Примечание: `fetch_dm_headers` всегда зарегистрирована, т.к. использует только `NodeProvider`.

#### Go-клиент (rpc.Client)

`rpc.Client` — экспортируемый HTTP-клиент RPC сервера. `ExecuteCommand(input)` маршрутизирует ввод по формату: сырые JSON-фреймы (начинающиеся с `{`) отправляются на `POST /rpc/v1/frame`, где сервер применяет dispatch через CommandTable (зарегистрированные команды могут нормализовать или пересобрать фрейм; только незарегистрированные типы сохраняют все wire-поля через fallback в HandleLocalFrame); именованные команды разбираются через `ParseConsoleInput` в `{command, args}` и отправляются на `POST /rpc/v1/exec`. Никакой логики legacy-маршрутов — `ParseConsoleInput` является единственным источником истины для маппинга позиционных аргументов в именованные.

#### ParseConsoleInput

`ParseConsoleInput` принимает два формата ввода: именованные команды (`send_dm addr hello world`) и сырые JSON-фреймы (`{"type":"ping"}`). Имена команд регистронезависимы. Для JSON-ввода поле `type` становится именем команды, все поля передаются как args. Используется и UI-консолью (in-process), и `rpc.Client` (через HTTP).

**Нормализация полей фрейма.** Проводные фреймы протокола используют другие имена полей, чем RPC-обработчики. `normalizeFrameArgs` устраняет разрыв, чтобы вставка реальных wire-фреймов в консоль работала. Алиасы применяются только если RPC-поле отсутствует; если присутствуют оба — RPC-поле имеет приоритет.

| Команда | Wire-поле | RPC-поле | Трансформация |
|---|---|---|---|
| `add_peer` | `peers` (массив) | `address` (строка) | `peers[0]` → `address` |
| `send_dm` | `recipient` | `to` | переименование |
| `fetch_chatlog` | `address` | `peer_address` | переименование |
| *(пагинация)* | `count` | `offset` | переименование (все команды) |

#### UI консоль

UI консоль выполняет команды напрямую через `CommandTable` — без HTTP round-trip. Подсказки команд загружаются синхронно из `CommandTable.Commands()` при инициализации. Два поведения отличаются от API-пути: `help` рендерит человекочитаемый категоризированный текст (с дефолтами и self-address) вместо машинного JSON, а неизвестные команды делают fallback на `ExecuteConsoleCommand` для passthrough сырых протокольных фреймов в `HandleLocalFrame`.

### Тестирование

```bash
# Запуск всех RPC тестов
go test ./internal/core/rpc/... -v

# Запуск тестов CLI (parseArgs, execRPC)
go test ./cmd/corsa-cli/... -v

# Запуск тестов конкретных категорий команд
go test ./internal/core/rpc/... -run TestSystem -v
go test ./internal/core/rpc/... -run TestNetwork -v

# Запуск unit-тестов CommandTable
go test ./internal/core/rpc/... -run TestCommandTable -v

# Запуск тестов парсера консоли
go test ./internal/core/rpc/... -run TestParseConsole -v

# Запуск тестов Go-клиента
go test ./internal/core/rpc/... -run TestClient -v
```

Тесты используют Fiber `app.Test()` для тестирования HTTP-слоя. CommandTable можно тестировать напрямую без HTTP. CLI-тесты покрывают `parseArgs` (JSON, key=value, positional) и `execRPC` (эндпоинт, авторизация). Тесты клиента используют `httptest.NewServer` для проверки маршрутизации: именованные команды → `/rpc/v1/exec`, сырые JSON-фреймы → `/rpc/v1/frame`.
