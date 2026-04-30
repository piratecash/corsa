# System Commands

## English

### POST /rpc/v1/system/help

List all available commands.

Response:
```json
{
  "version": "1.0",
  "commands": [
    {"name": "help", "description": "List all available RPC commands", "category": "system"},
    {"name": "getPeers", "description": "Get list of connected peers", "category": "network"}
  ]
}
```

`version` is the help response schema version (currently `"1.0"`). It tracks the structure of the help response itself — not the protocol or client version. Clients can use it to detect format changes (e.g. new fields in command metadata). Bump when the help response structure changes.

### POST /rpc/v1/system/ping

Send local ping, returns pong response. The same semantic result regardless of transport (HTTP RPC or in-process desktop).

### POST /rpc/v1/system/hello

Send hello frame for identification. The handler populates `Version`, `MinimumProtocolVersion`, `Client`, `ClientVersion`, and `ClientBuild` before forwarding to the node — without these fields the handshake is rejected. The standalone node handler identifies as `Client: "rpc"`; in desktop mode, `RegisterDesktopOverrides` registers a transport-level override that identifies as `Client: "desktop"` with the desktop application version. This is not a semantic fork — it changes only transport identity metadata. `ClientBuild` is a monotonically increasing integer that peers compare to detect newer software releases.

### POST /rpc/v1/system/version

Client and protocol version.

Response:
```json
{
  "client_version": "0.21-alpha",
  "protocol_version": 3,
  "node_address": "a1b2c3d4..."
}
```

### POST /rpc/v1/system/node_status

PIP-0001 integration surface — single authenticated probe consumed by
PirateCash Core at masternode startup (Stage 1) and during the v20
liveness checks (Stage 2). The `public_key` field is the long-term
identifier the v21 proof-of-service will sign challenges with.

Equivalent dispatch: `POST /rpc/v1/exec` with `{"command": "getNodeStatus"}`.
Snake_case aliases `node_status` and `get_node_status` resolve to the
same handler; case-insensitive matching additionally accepts spellings
like `nodestatus`. Route-based callers may use either
`/rpc/v1/system/node_status` or `/rpc/v1/system/nodestatus`.

Response:
```json
{
  "identity": "a1b2c3d4...",
  "address": "a1b2c3d4...",
  "public_key": "Q...=",
  "box_public_key": "B...=",
  "protocol_version": 17,
  "minimum_protocol_version": 12,
  "client_version": "0.21-alpha",
  "client_build": 121,
  "connected_peers": 4,
  "started_at": "2026-04-30T12:00:00Z",
  "uptime_seconds": 3742,
  "current_time": "2026-04-30T13:02:22Z"
}
```

Field reference:

- `identity` — ed25519 fingerprint, the node's stable routing identifier
  and the primary field PirateCash Core should consume.
- `address` — ed25519 fingerprint, the node's stable routing identifier.
- `public_key` — base64-encoded ed25519 public key behind `address`.
- `box_public_key` — base64-encoded curve25519 public key for E2E DM
  encryption.
- `protocol_version` / `minimum_protocol_version` — wire protocol the
  node currently speaks and the floor it accepts on inbound peers.
- `client_version` / `client_build` — implementation identifiers.
  `client_build` is a monotonically increasing integer suitable for
  policy gates.
- `connected_peers` — number of **distinct peer identities** the node
  currently has at least one live connection with, counting both
  outbound and inbound sessions. Multiple sockets from the same peer
  identity collapse to one (the field measures relay reach, not socket
  count; use `getActiveConnections` for per-connection detail). A
  masternode reporting zero usable peers is unlikely to be relaying
  messenger traffic (Stage 2 service-health hint).
- `started_at` / `uptime_seconds` / `current_time` — RFC3339Nano UTC
  timestamps plus a whole-seconds convenience field. PirateCash Core
  uses these to detect clock drift between its own host and the
  masternode.

Security guarantees enforced by tests:

- Only public key material crosses the wire. The endpoint MUST NOT
  carry private keys, seeds, RPC credentials, or other secrets.
- Authenticated localhost RPC only. Same auth gate as every other
  command in this document — see [../rpc.md](../rpc.md).
- No signature attached. PIP-0001 stages 1 and 2 do not require one;
  the v21 proof-of-service will be exposed as a separate signed
  artifact rather than retrofitted onto this snapshot, so health-check
  loops stay cheap.

---

## Русский

### POST /rpc/v1/system/help

Список всех доступных команд.

Ответ:
```json
{
  "version": "1.0",
  "commands": [
    {"name": "help", "description": "Список всех доступных RPC команд", "category": "system"},
    {"name": "getPeers", "description": "Получить список подключённых пиров", "category": "network"}
  ]
}
```

`version` — версия схемы ответа help (сейчас `"1.0"`). Отслеживает структуру самого ответа help, а не версию протокола или клиента. Клиенты могут использовать это поле для обнаружения изменений формата (например, новых полей в метаданных команд). Инкрементируется при изменении структуры ответа help.

### POST /rpc/v1/system/ping

Локальный пинг, возвращает pong-ответ. Одинаковый семантический результат вне зависимости от транспорта (HTTP RPC или in-process desktop).

### POST /rpc/v1/system/hello

Отправка hello-фрейма для идентификации. Обработчик заполняет `Version`, `MinimumProtocolVersion`, `Client`, `ClientVersion` и `ClientBuild` перед отправкой ноде — без этих полей handshake отклоняется. Standalone-нода идентифицируется как `Client: "rpc"`; в desktop-режиме `RegisterDesktopOverrides` регистрирует transport-level override, который идентифицируется как `Client: "desktop"` с версией desktop-приложения. Это не семантический форк — меняются только transport identity метаданные. `ClientBuild` — монотонно возрастающий целочисленный номер сборки, по которому пиры определяют наличие новых версий ПО.

### POST /rpc/v1/system/version

Версия клиента и протокола.

Ответ:
```json
{
  "client_version": "0.21-alpha",
  "protocol_version": 3,
  "node_address": "a1b2c3d4..."
}
```

### POST /rpc/v1/system/node_status

Точка интеграции из PIP-0001 — один аутентифицированный probe, который
PirateCash Core вызывает на старте мастерноды (Этап 1) и при проверках
доступности в v20 (Этап 2). Поле `public_key` — это долговременный
идентификатор, которым v21 proof-of-service будет подписывать
challenge-и.

Эквивалентный вызов: `POST /rpc/v1/exec` с
`{"command": "getNodeStatus"}`. Алиасы `node_status` и `get_node_status`
ведут к тому же обработчику; case-insensitive поиск дополнительно
принимает варианты вроде `nodestatus`. Для route-based вызова работают
оба пути: `/rpc/v1/system/node_status` и `/rpc/v1/system/nodestatus`.

Ответ:
```json
{
  "identity": "a1b2c3d4...",
  "address": "a1b2c3d4...",
  "public_key": "Q...=",
  "box_public_key": "B...=",
  "protocol_version": 17,
  "minimum_protocol_version": 12,
  "client_version": "0.21-alpha",
  "client_build": 121,
  "connected_peers": 4,
  "started_at": "2026-04-30T12:00:00Z",
  "uptime_seconds": 3742,
  "current_time": "2026-04-30T13:02:22Z"
}
```

Описание полей:

- `identity` — ed25519-fingerprint, стабильный идентификатор ноды
  в маршрутизации и основное поле для PirateCash Core.
- `address` — ed25519-fingerprint, стабильный идентификатор ноды
  в маршрутизации.
- `public_key` — base64-кодированный публичный ed25519-ключ,
  соответствующий `address`.
- `box_public_key` — base64-кодированный публичный curve25519-ключ для
  end-to-end шифрования DM.
- `protocol_version` / `minimum_protocol_version` — текущая версия
  wire-протокола ноды и минимальная допустимая для входящих пиров.
- `client_version` / `client_build` — идентификаторы реализации.
  `client_build` — монотонно возрастающее число, удобно для policy
  gate-ов.
- `connected_peers` — количество **различных peer identity**, с
  которыми у ноды сейчас есть хотя бы одно живое соединение, считая
  и outbound, и inbound сессии. Несколько сокетов от одного и того
  же peer identity схлопываются в один (поле описывает охват
  ретрансляции, а не число сокетов; для пер-коннекшен данных есть
  `getActiveConnections`). Мастернода с нулём полезных пиров вряд
  ли ретранслирует мессенджерный трафик (service-health сигнал
  Этапа 2).
- `started_at` / `uptime_seconds` / `current_time` — RFC3339Nano UTC
  timestamps плюс convenience-поле в целых секундах. PirateCash Core
  использует их для детекции дрейфа часов между своим хостом и
  мастернодой.

Гарантии безопасности, защищённые тестами:

- Через wire идёт только публичный ключевой материал. Эндпоинт НЕ
  должен возвращать приватные ключи, seed-ы, RPC-credentials и любые
  другие секреты.
- Только аутентифицированный localhost-RPC. Тот же gate, что и у всех
  остальных команд в этом документе — см. [../rpc.md](../rpc.md).
- Подпись не прикладывается. Этапы 1 и 2 PIP-0001 её не требуют;
  v21 proof-of-service будет отдельным подписанным артефактом, а не
  довеском к этому снапшоту, чтобы health-check loop оставался
  дешёвым.
