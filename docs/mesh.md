# Mesh Network — Node Layer

## English

Related documentation:

- [architecture.md](architecture.md) — project layout and runtime model
- [protocol.md](protocol.md) — frame format and protocol version
- [encryption.md](encryption.md) — cryptographic primitives
- [dm_router.md](dm_router.md) — service layer on top of the mesh

Source: `internal/core/node/service.go`, `internal/core/node/peer_state.go`,
`internal/core/node/trust.go`, `internal/core/node/netgroup.go`,
`internal/core/transport/peer.go`

### Overview

CORSA forms a peer-to-peer mesh where every node — whether a standalone relay
(`corsa-node`) or an embedded desktop instance (`corsa-desktop`) — maintains
direct TCP connections to a subset of peers and gossips messages across the
network. There is no central server; the bootstrap peer is simply the first
node a newcomer connects to for initial discovery.

### Mesh topology

```mermaid
graph TD
    subgraph "CORSA Mesh Network"
        BS["Bootstrap Node<br/>(full)"]
        N1["Node A<br/>(full, desktop)"]
        N2["Node B<br/>(full, desktop)"]
        N3["Node C<br/>(full, relay)"]
        N4["Node D<br/>(client, mobile)"]
    end

    N1 <-->|TCP session| BS
    N2 <-->|TCP session| BS
    N3 <-->|TCP session| BS
    N1 <-->|TCP session| N2
    N1 <-->|TCP session| N3
    N2 <-->|TCP session| N3
    N4 -->|TCP session| N2
    N4 -->|TCP session| N3

    style BS fill:#f9a825,stroke:#333
    style N4 fill:#81d4fa,stroke:#333
```
*Diagram 1 — Mesh topology with bootstrap and peer nodes*

### Peer discovery flow

```mermaid
sequenceDiagram
    participant New as New Node
    participant BS as Bootstrap Peer
    participant P1 as Peer 1
    participant P2 as Peer 2

    Note over New: Startup: reads bootstrap<br/>addresses from env

    loop bootstrapLoop() every 2s
        New->>BS: TCP dial → hello
        BS-->>New: welcome (challenge, observed IP)
        New->>BS: auth_session (signed challenge)
        New->>BS: get_peers
        BS-->>New: peers [P1, P2, ...]
        Note over New: addPeerAddress(P1)<br/>addPeerAddress(P2)
    end

    New->>P1: TCP dial → hello → auth
    P1-->>New: welcome
    New->>P1: get_peers
    P1-->>New: peers [BS, P2, P3, ...]
    Note over New: Merges new addresses<br/>into local pool

    New->>P2: TCP dial → hello → auth
    P2-->>New: welcome
    Note over New: Up to 8 outgoing<br/>sessions maintained
```
*Diagram 2 — Bootstrap and peer discovery sequence*

### Node roles

- **full** — relays DMs, delivery receipts, and Gazeta notices on behalf of
  the entire network. Both `corsa-node` and `corsa-desktop` default to this.
- **client** — syncs peers and contacts, stores only its own traffic, never
  forwards foreign messages. Future mobile/light clients will use this role.

### Peer discovery

Each node starts with a list of bootstrap addresses (from `CORSA_BOOTSTRAP_PEERS`
env var, default `65.108.204.190:64646`). On startup, `bootstrapLoop()` runs
every 2 seconds and:

1. **`ensurePeerSessions()`** — dials up to 8 outgoing peers, selected by
   score (highest first) from the known address pool.
2. **Peer exchange** — on connecting to any peer, the node sends `get_peers`
   and receives a `peers` frame. Discovered addresses are merged into the local
   pool via `addPeerAddress()`, making them available for future dials. This
   gives the network a gossip-style peer discovery mechanism.
3. **Stale peer eviction** — every 10 minutes, peers with score ≤ −20 and no
   successful connection in the last 24 hours are pruned. Bootstrap peers and
   currently connected peers are never evicted.
4. **Peer state persistence** — every 5 minutes, the address pool is saved to
   `peers-{port}.json`. On restart, persisted peers are merged with bootstrap
   peers so the node quickly reconnects to previously healthy peers.

### Peer scoring and dial priority

Each peer carries a reputation score in the range [−50, +100]:

- **+10** on successful TCP handshake (reset consecutive failures)
- **−5** on dial/protocol failure
- **−2** on clean disconnect

`peerDialCandidates()` sorts peers by score descending. If a peer fails once,
it is retried immediately. For ≥ 2 consecutive failures an exponential cooldown
applies: `min(30s × 2^(failures−2), 30 min)`. A successful connection resets
the counter.

Fallback port variants (e.g., `:64647` for `:64646`) inherit the primary
peer's reputation to prevent cooldown bypass.

### Connection lifecycle

**Outbound:**

1. `runPeerSession()` dials with 1.5 s timeout.
2. Client sends `hello` (identity, services, node type, reachable network
   groups, advertised listen address).
3. Server responds with `welcome` (protocol version, challenge nonce,
   observed address, server identity).
4. Client signs the challenge with `ed25519` and sends `auth_session`.
   Server verifies; invalid signatures accumulate ban points (1 000 points
   → 24-hour IP ban).
5. Session enters `servePeerSession()`: initial one-time sync
   (`get_peers`, `fetch_contacts`, flush pending frames), then
   steady-state with heartbeat pings every ~2 min and event-driven
   push frames only (no periodic polling).

**Inbound:**

1. `handleConn()` reads the peer's `hello` frame.
2. Sends `welcome` with a fresh challenge.
3. Waits for `auth_session` and verifies.
4. Commands are routed through `handlePeerSessionFrame()`.

### Connection handshake (detail)

```mermaid
sequenceDiagram
    participant C as Client (dialer)
    participant S as Server (listener)

    C->>S: TCP connect (1.5s timeout)
    C->>S: hello {identity, services,<br/>node_type, networks, listen_addr}
    S->>S: Validate hello
    S-->>C: welcome {proto_version, challenge,<br/>observed_addr, server_identity}
    C->>C: ed25519.Sign(challenge)
    C->>S: auth_session {signature}
    S->>S: Verify signature
    alt Valid
        S->>S: Enter servePeerSession()
        Note over C,S: Session active:<br/>heartbeat ~2 min,<br/>event-driven push only
    else Invalid
        S->>S: Ban points += 1000
        Note over S: 1000 pts → 24h IP ban
        S--xC: Close connection
    end
```
*Diagram 3 — TCP connection handshake and authentication*

### Peer session lifecycle (full flow)

```mermaid
sequenceDiagram
    participant A as Node A (initiator)
    participant B as Node B (responder)

    Note over A,B: Phase 1: Handshake

    A->>B: hello (version, address, client, box_key, pub_key, box_sig)
    B->>A: welcome (address, challenge, observed_address)

    Note over A,B: Phase 2: Authentication

    A->>B: auth_session (address, signature)
    B->>A: auth_ok

    Note over A,B: Phase 3: Subscription

    A->>B: subscribe_inbox (topic="dm", recipient=A.address)
    B->>A: subscribed

    Note over A,B: Phase 4: Initial Sync (one-time)

    A->>B: get_peers
    B->>A: peers [...]
    A->>B: fetch_contacts
    B->>A: contacts [...]

    A->>B: flush pending frames (push_message, push_delivery_receipt)

    Note over A,B: Phase 5: Steady-state

    rect rgb(240, 248, 255)
        Note over A,B: Heartbeat (every ~2 min)
        A->>B: ping
        B->>A: pong
    end

    rect rgb(255, 248, 240)
        Note over A,B: Message relay (event-driven)
        B-->>A: push_message (new message for A)
        A-->>B: ack_delete
    end

    rect rgb(255, 248, 240)
        Note over A,B: Delivery receipt relay (event-driven)
        B-->>A: push_delivery_receipt
        A-->>B: ack_delete
    end

    rect rgb(240, 255, 240)
        Note over A,B: Peer announcement (event-driven)
        B-->>A: announce_peer (new peer address)
        A-->>B: announce_peer_ack
        Note right of A: Non-recursive:<br/>not relayed further
    end

    rect rgb(255, 240, 240)
        Note over A,B: Stall detection
        Note right of A: If pong not received<br/>within 45s → disconnect
    end
```
*Diagram 4 — Peer session lifecycle: handshake → auth → sync → steady-state*

### Health monitoring

- **Heartbeat** — pings sent every ~2 min (with jitter up to 15 s). If
  no pong arrives within 45 s (`pongStallTimeout`), the peer is declared
  stalled and the session is torn down.
- **Initial sync** — on connect only: `get_peers`, `fetch_contacts`,
  flush pending frames. No periodic polling in steady-state.
- **Peer announcement** — when a new peer authenticates on an inbound
  connection, its advertised listen address is announced to all active
  outbound sessions via `announce_peer`. The frame includes the peer's
  `node_type` ("full" or "client") so recipients know what kind of peer
  is being offered. The announcement is non-recursive: recipients learn
  the address locally but do not relay it further.
  Local/private addresses are excluded. If the outbound send channel is
  full, the frame is queued via the pending mechanism and delivered after
  drain. If a frame with the same dedup key is already queued, its
  payload is **replaced** with the newer version so that metadata changes
  (e.g. `node_type`) are not lost while the queue is blocked.
  `node_type` is a **required** field: on the receiving side it is
  validated against known types (`isKnownNodeType`); frames with an
  unknown or empty `node_type` are silently ignored — the ack is still
  returned but the peer address is not learned. Local/private addresses
  (`NetGroupLocal`) are filtered in the `announce_peer` handler itself,
  but **not** in `addPeerAddress`/`promotePeerAddress` — those lower-level
  functions accept LAN peers so that dev/LAN meshes can discover 10.x /
  192.168.x nodes via direct hello/welcome and `get_peers`.
  Authenticated senders trigger promotion: the peer's cooldown is reset
  (via `resolveHealthAddress` so fallback-variant cooldown shares with
  the primary) and its `node_type` is updated, but it is **not** moved
  to the front
  of the dial list. Dial priority is managed locally — remote peers
  cannot influence it (trust-no-one policy). Only the local `add_peer`
  RPC command (explicit user action) prepends to the front.
  Unauthenticated senders can only add **new** peers; if the address is
  already known, its `node_type` is preserved to prevent an unauthenticated
  client from retagging a "full" peer as "client" and disrupting routing.
  Different ports on the same host are treated as distinct peers (multiple
  nodes may share an IP); the fallback-variant model handles same-host
  resolution at dial time, not at discovery time.
- **Stall detection** — based on heartbeat response:
  - no useful traffic for ≥ 2 min (`heartbeatInterval`): _degraded_
  - no useful traffic for ≥ 2 min 45 s (`heartbeatInterval` + `pongStallTimeout`): _stalled_

Peer states: `healthy` → `degraded` → `stalled` → `reconnecting`.

**Aggregate network status** is derived from the number of _usable_ peers
(healthy + degraded). Stalled peers have a live TCP session but are excluded
from message routing, so they do not count as usable. Per-peer details are
shown in the breakdown line.

- `offline` — no known peers
- `reconnecting` — all peers are reconnecting, none connected
- `limited` — zero or one usable peer (stalled-only counts here too)
- `warning` — usable peers exist but less than half of total
- `healthy` — at least half of known peers are usable (2+)

### Message routing

**Direct messages:**

1. Desktop encrypts the message with the recipient's X25519 box key and
   sends `send_message` to the embedded local node.
2. The node validates the ed25519 signature, deduplicates, and stores it.
3. If the message is local (sender or recipient is this node),
   `MessageStore.StoreMessage()` is called (desktop persists to chatlog).
4. `gossipMessage()` fans the message out to the best 3 scored peers via
   `routingTargetsForMessage()`:
   - broadcast (`recipient="*"`): all full nodes
   - addressed: all full nodes + the specific recipient if client role
5. If the target peer has no active session, the message is queued in
   `pending[address]` and flushed when the session reconnects.

```mermaid
flowchart LR
    subgraph "Sender (Desktop)"
        UI["UI: SendMessage()"]
        ENC["Encrypt with<br/>X25519 box key"]
    end

    subgraph "Local Node"
        VAL["Validate ed25519 sig<br/>+ deduplicate"]
        STORE["MessageStore.<br/>StoreMessage()"]
        GOSSIP["gossipMessage()<br/>→ best 3 peers"]
    end

    subgraph "Relay Nodes"
        R1["Full Node 1"]
        R2["Full Node 2"]
    end

    subgraph "Recipient Node"
        RCV["Receive + verify"]
        RSTORE["StoreMessage()"]
        RECEIPT["emitDeliveryReceipt()"]
    end

    UI --> ENC --> VAL --> STORE
    VAL --> GOSSIP
    GOSSIP --> R1 --> RCV
    GOSSIP --> R2 --> RCV
    RCV --> RSTORE --> RECEIPT

    style UI fill:#e1f5fe,stroke:#333
    style RECEIPT fill:#c8e6c9,stroke:#333
```
*Diagram 5 — Direct message routing and gossip*

**Delivery receipts:**

1. When a node receives a DM addressed to it, `emitDeliveryReceipt()` auto-
   generates a `"delivered"` receipt and stores it via `storeDeliveryReceipt()`.
2. The receipt is gossiped back toward the original sender.
3. `"seen"` receipts are sent when the active chat is on screen —
   both `SelectPeer` and `AutoSelectPeer` trigger `MarkConversationSeen()`.
4. Receipts persist to chatlog via `UpdateDeliveryStatus()` before the local
   change event is emitted, maintaining the "DB first, then event" invariant.

```mermaid
sequenceDiagram
    participant A as Sender Node
    participant Mesh as Mesh (gossip)
    participant B as Recipient Node

    A->>Mesh: send_message (encrypted DM)
    Mesh->>B: deliver message
    B->>B: StoreMessage() → chatlog.db
    B->>B: emitDeliveryReceipt("delivered")
    B-->>Mesh: delivery_receipt "delivered"
    Mesh-->>A: delivery_receipt "delivered"
    A->>A: UpdateDeliveryStatus("delivered")

    Note over B: User opens chat and clicks peer
    B->>B: MarkConversationSeen()
    B-->>Mesh: delivery_receipt "seen"
    Mesh-->>A: delivery_receipt "seen"
    A->>A: UpdateDeliveryStatus("seen")
```
*Diagram 6 — Delivery receipt flow and status updates*

**Transit relay:**

Messages where neither sender nor recipient is this node are relayed:

- Stored in `relayRetry` with a 3-minute TTL.
- Persisted across restarts in `queue-{port}.json`.
- Retried on reconnect via `retryRelayDeliveries()`.

### Pending frame queue

Outbound frames that cannot be sent immediately are queued per peer address in
`pending[address]`. Each entry records the frame, queue time, and retry count.
On reconnect, `flushPendingPeerFrames()` drains the queue. The queue is
persisted in `queue-{port}.json` and survives restarts.

Legacy entries keyed by fallback-port addresses are migrated on load; entries
that cannot be resolved to a primary address move to the `"orphaned"` section.

### Network groups and NAT

Peers are classified into reachable network groups by `ClassifyAddress()`:

- **IPv4**, **IPv6**, **TorV3**, **TorV2**, **I2P**, **CJDNS**, **Local**,
  **Unknown**

The `hello` frame advertises the node's reachable groups (`"networks"` field).
Peer exchange filters discovered addresses by the intersection of reachable
groups — nodes that cannot reach a given network class will not receive
addresses for it.

**NAT detection:** Each peer reports the connecting node's observed IP in the
`welcome` frame. When ≥ 2 distinct peers report the same IP, the node accepts
it as its external address (consensus threshold). The address is informational
only — the node never auto-rewrites its advertise setting.

**Tor support:** Setting `CORSA_PROXY=127.0.0.1:9050` routes connections through
a SOCKS5 proxy, enabling `.onion` address resolution.

### Trust model (TOFU)

Implemented in `trustStore` (`trust.go`):

1. Fingerprint address = `hex(sha256(ed25519_pubkey)[:20])`.
2. Box-key binding: `ed25519.Sign(identity_key, "corsa-boxkey-v1|" + address + "|" + boxkey_base64)`.
3. On first valid contact exchange, the key set is pinned locally in
   `trust-{port}.json` (Trust On First Use).
4. Later conflicting keys for the same address are rejected and recorded in
   the `"conflicts"` section for manual review.

### Gazeta — TTL-based encrypted notices

A dead-drop / bulletin-board channel for encrypted notices:

1. The sender encrypts a notice with the recipient's X25519 box key using an
   ephemeral key + AES-GCM.
2. Sends `publish_notice { ttl_seconds, ciphertext }`.
3. The notice is gossiped to all full nodes.
4. Peers store the notice until its TTL expires, then purge it.
5. Recipients fetch via `fetch_notices` and decrypt with their identity key.

### Persistence summary

| File | Contents |
|---|---|
| `peers-{port}.json` | Known peer addresses, scores, metadata |
| `trust-{port}.json` | TOFU-pinned contacts and conflict log |
| `queue-{port}.json` | Pending outbound frames, relay retry state |
| `chatlog.db` (SQLite) | Message history, delivery status (desktop only) |

---

## Русский

Связанная документация:

- [architecture.md](architecture.md) — структура проекта и модель запуска
- [protocol.md](protocol.md) — формат фреймов и версия протокола
- [encryption.md](encryption.md) — криптографические примитивы
- [dm_router.md](dm_router.md) — сервисный слой поверх mesh

Исходники: `internal/core/node/service.go`, `internal/core/node/peer_state.go`,
`internal/core/node/trust.go`, `internal/core/node/netgroup.go`,
`internal/core/transport/peer.go`

### Обзор

CORSA формирует peer-to-peer mesh, в котором каждая нода — как standalone relay
(`corsa-node`), так и встроенный desktop-инстанс (`corsa-desktop`) —
поддерживает прямые TCP-соединения с подмножеством peer'ов и распространяет
сообщения по сети через gossip. Центрального сервера нет; bootstrap-peer — это
просто первая нода, к которой подключается новый участник для начального
обнаружения.

### Топология mesh-сети

```mermaid
graph TD
    subgraph "Mesh-сеть CORSA"
        BS["Bootstrap-нода<br/>(full)"]
        N1["Нода A<br/>(full, desktop)"]
        N2["Нода B<br/>(full, desktop)"]
        N3["Нода C<br/>(full, relay)"]
        N4["Нода D<br/>(client, mobile)"]
    end

    N1 <-->|TCP-сессия| BS
    N2 <-->|TCP-сессия| BS
    N3 <-->|TCP-сессия| BS
    N1 <-->|TCP-сессия| N2
    N1 <-->|TCP-сессия| N3
    N2 <-->|TCP-сессия| N3
    N4 -->|TCP-сессия| N2
    N4 -->|TCP-сессия| N3

    style BS fill:#f9a825,stroke:#333
    style N4 fill:#81d4fa,stroke:#333
```
*Диаграмма 1 — Топология mesh-сети с bootstrap и peer-нодами*

### Процесс обнаружения peer'ов

```mermaid
sequenceDiagram
    participant New as Новая нода
    participant BS as Bootstrap Peer
    participant P1 as Peer 1
    participant P2 as Peer 2

    Note over New: Старт: читает bootstrap-<br/>адреса из env

    loop bootstrapLoop() каждые 2с
        New->>BS: TCP dial → hello
        BS-->>New: welcome (challenge, observed IP)
        New->>BS: auth_session (подписанный challenge)
        New->>BS: get_peers
        BS-->>New: peers [P1, P2, ...]
        Note over New: addPeerAddress(P1)<br/>addPeerAddress(P2)
    end

    New->>P1: TCP dial → hello → auth
    P1-->>New: welcome
    New->>P1: get_peers
    P1-->>New: peers [BS, P2, P3, ...]
    Note over New: Мержит новые адреса<br/>в локальный пул

    New->>P2: TCP dial → hello → auth
    P2-->>New: welcome
    Note over New: До 8 исходящих<br/>сессий одновременно
```
*Диаграмма 2 — Процесс bootstrap и обнаружения peer'ов*

### Роли нод

- **full** — ретранслирует DM, delivery receipts и Gazeta-notice от имени
  всей сети. Обе программы (`corsa-node` и `corsa-desktop`) по умолчанию
  используют эту роль.
- **client** — синхронизирует peer'ов и контакты, хранит только свой трафик,
  никогда не пересылает чужие сообщения. Будущие мобильные/лёгкие клиенты
  будут использовать эту роль.

### Обнаружение peer'ов

Каждая нода стартует со списком bootstrap-адресов (из `CORSA_BOOTSTRAP_PEERS`,
по умолчанию `65.108.204.190:64646`). При запуске `bootstrapLoop()` работает
каждые 2 секунды:

1. **`ensurePeerSessions()`** — устанавливает до 8 исходящих соединений,
   выбирая peer'ов по скору (наивысший первым).
2. **Обмен peer'ами** — при подключении к peer'у нода отправляет `get_peers`
   и получает фрейм `peers`. Обнаруженные адреса добавляются в локальный пул
   через `addPeerAddress()`, что создаёт gossip-механизм обнаружения.
3. **Вычищение устаревших peer'ов** — каждые 10 минут удаляются peer'ы со
   скором ≤ −20 и без успешных подключений за последние 24 часа. Bootstrap-
   и текущие peer'ы никогда не удаляются.
4. **Персистентность** — каждые 5 минут пул адресов сохраняется в
   `peers-{port}.json`. При перезапуске сохранённые peer'ы мержатся с
   bootstrap для быстрого переподключения.

### Скоринг и приоритет подключений

Каждый peer имеет скор репутации в диапазоне [−50, +100]:

- **+10** при успешном TCP-хендшейке (сброс счётчика ошибок)
- **−5** при ошибке подключения/протокола
- **−2** при штатном отключении

`peerDialCandidates()` сортирует peer'ов по убыванию скора. Первый сбой —
немедленная повторная попытка. При ≥ 2 последовательных ошибках —
экспоненциальное ожидание: `min(30с × 2^(ошибки−2), 30 мин)`. Успешное
подключение сбрасывает счётчик.

### Жизненный цикл соединения

**Исходящее:**

1. `runPeerSession()` подключается с таймаутом 1.5 с.
2. Клиент отправляет `hello` (identity, сервисы, тип ноды, доступные
   сетевые группы, объявленный listen-адрес).
3. Сервер отвечает `welcome` (версия протокола, challenge-nonce, наблюдаемый
   адрес, identity сервера).
4. Клиент подписывает challenge через `ed25519` и отправляет `auth_session`.
   Сервер проверяет; невалидные подписи накапливают ban-баллы (1 000 баллов
   → бан IP на 24 часа).
5. Сессия переходит в `servePeerSession()`: однократная начальная
   синхронизация (`get_peers`, `fetch_contacts`, flush pending фреймов),
   затем steady-state с heartbeat-пингами каждые ~2 мин и только
   event-driven push-фреймами (без периодического polling).

**Входящее:**

1. `handleConn()` читает `hello` peer'а.
2. Отправляет `welcome` со свежим challenge.
3. Ожидает `auth_session` и верифицирует.
4. Команды маршрутизируются через `handlePeerSessionFrame()`.

### Хендшейк соединения (подробнее)

```mermaid
sequenceDiagram
    participant C as Клиент (dialer)
    participant S as Сервер (listener)

    C->>S: TCP-соединение (таймаут 1.5с)
    C->>S: hello {identity, services,<br/>node_type, networks, listen_addr}
    S->>S: Валидация hello
    S-->>C: welcome {proto_version, challenge,<br/>observed_addr, server_identity}
    C->>C: ed25519.Sign(challenge)
    C->>S: auth_session {signature}
    S->>S: Проверка подписи
    alt Валидна
        S->>S: Переход в servePeerSession()
        Note over C,S: Сессия активна:<br/>heartbeat ~2 мин,<br/>только event-driven push
    else Невалидна
        S->>S: Ban-баллы += 1000
        Note over S: 1000 баллов → бан IP 24ч
        S--xC: Закрытие соединения
    end
```
*Диаграмма 3 — TCP-хендшейк и аутентификация*

### Жизненный цикл peer-сессии (полный flow)

```mermaid
sequenceDiagram
    participant A as Нода A (инициатор)
    participant B as Нода B (ответчик)

    Note over A,B: Фаза 1: Хендшейк

    A->>B: hello (version, address, client, box_key, pub_key, box_sig)
    B->>A: welcome (address, challenge, observed_address)

    Note over A,B: Фаза 2: Аутентификация

    A->>B: auth_session (address, signature)
    B->>A: auth_ok

    Note over A,B: Фаза 3: Подписка

    A->>B: subscribe_inbox (topic="dm", recipient=A.address)
    B->>A: subscribed

    Note over A,B: Фаза 4: Начальная синхронизация (однократно)

    A->>B: get_peers
    B->>A: peers [...]
    A->>B: fetch_contacts
    B->>A: contacts [...]

    A->>B: flush pending фреймов (push_message, push_delivery_receipt)

    Note over A,B: Фаза 5: Steady-state

    rect rgb(240, 248, 255)
        Note over A,B: Heartbeat (каждые ~2 мин)
        A->>B: ping
        B->>A: pong
    end

    rect rgb(255, 248, 240)
        Note over A,B: Ретрансляция сообщений (event-driven)
        B-->>A: push_message (новое сообщение для A)
        A-->>B: ack_delete
    end

    rect rgb(255, 248, 240)
        Note over A,B: Ретрансляция квитанций (event-driven)
        B-->>A: push_delivery_receipt
        A-->>B: ack_delete
    end

    rect rgb(240, 255, 240)
        Note over A,B: Анонс нового peer'а (event-driven)
        B-->>A: announce_peer (адрес нового peer'а)
        A-->>B: announce_peer_ack
        Note right of A: Без рекурсии:<br/>не ретранслируется дальше
    end

    rect rgb(255, 240, 240)
        Note over A,B: Обнаружение зависания
        Note right of A: Если pong не получен<br/>в течение 45с → disconnect
    end
```
*Диаграмма 4 — Жизненный цикл peer-сессии: хендшейк → аутентификация → синхронизация → steady-state*

### Мониторинг здоровья

- **Heartbeat** — пинги каждые ~2 мин (с jitter до 15 с). Если pong не
  приходит в течение 45 с (`pongStallTimeout`), peer объявляется зависшим
  и сессия разрывается.
- **Начальная синхронизация** — только при подключении: `get_peers`,
  `fetch_contacts`, flush pending фреймов. В steady-state нет
  периодического polling.
- **Анонс peer'ов** — когда новый peer аутентифицируется через входящее
  соединение, его объявленный listen-адрес анонсируется всем активным
  исходящим сессиям через `announce_peer`. Фрейм включает `node_type`
  пира ("full" или "client"), чтобы получатели знали тип предлагаемого
  узла. Анонс нерекурсивный: получатели сохраняют адрес локально, но
  не ретранслируют дальше. Локальные/приватные адреса исключаются. Если
  канал отправки переполнен, фрейм ставится в pending-очередь и
  доставляется после освобождения. Если фрейм с таким же ключом
  дедупликации уже в очереди, его payload **заменяется** на новую
  версию, чтобы изменения метаданных (например, `node_type`) не
  терялись пока очередь заблокирована.
  `node_type` — **обязательное** поле:
  на принимающей стороне оно валидируется (`isKnownNodeType`); фреймы с
  неизвестным или пустым `node_type` молча игнорируются — ack
  возвращается, но адрес пира не сохраняется. Локальные/приватные адреса
  (`NetGroupLocal`) фильтруются в самом handler'е `announce_peer`, но
  **не** в `addPeerAddress`/`promotePeerAddress` — эти низкоуровневые
  функции принимают LAN-пиры, чтобы dev/LAN-меши могли обнаруживать
  10.x / 192.168.x узлы через прямые hello/welcome и `get_peers`.
  Аутентифицированные отправители вызывают промоцию пира: cooldown
  сбрасывается (через `resolveHealthAddress`, чтобы fallback-вариант
  разделял cooldown с primary) и `node_type` обновляется, но пир
  **не** перемещается
  в начало dial-списка. Приоритет dial управляется локально — удалённые
  пиры не могут на него влиять (политика «никому не доверять»). Только
  локальная RPC-команда `add_peer` (явное действие пользователя)
  добавляет пир в начало списка.
  Неаутентифицированные отправители могут только добавлять **новые** пиры;
  если адрес уже известен, его `node_type` сохраняется, чтобы
  неаутентифицированный клиент не мог перетегировать «full»-пир как
  «client» и нарушить маршрутизацию.
  Разные порты на одном хосте считаются разными пирами (на одном IP
  могут работать несколько нод); модель fallback-вариантов разрешает
  same-host коллизии на этапе dial, а не на этапе discovery.
- **Обнаружение зависания** — на основе ответа heartbeat:
  - нет полезного трафика ≥ 2 мин (`heartbeatInterval`): _degraded_
  - нет полезного трафика ≥ 2 мин 45 с (`heartbeatInterval` + `pongStallTimeout`): _stalled_

Состояния peer'а: `healthy` → `degraded` → `stalled` → `reconnecting`.

**Агрегированный статус сети** определяется по количеству _пригодных_ пиров
(healthy + degraded). Stalled-пиры имеют живое TCP-соединение, но исключены
из маршрутизации сообщений, поэтому не считаются пригодными. Детали по
каждому пиру отображаются в breakdown-строке.

- `offline` — нет известных пиров
- `reconnecting` — все пиры переподключаются, ни один не подключён
- `limited` — ноль или один пригодный пир (stalled-only тоже попадает сюда)
- `warning` — есть пригодные пиры, но менее половины от общего числа
- `healthy` — как минимум половина известных пиров пригодна (2+)

### Маршрутизация сообщений

**Direct messages:**

1. Desktop шифрует сообщение X25519 box key получателя и отправляет
   `send_message` встроенной локальной ноде.
2. Нода проверяет ed25519 подпись, дедуплицирует, хранит.
3. Если сообщение локальное (sender или recipient — эта нода),
   вызывается `MessageStore.StoreMessage()` (desktop сохраняет в chatlog).
4. `gossipMessage()` рассылает сообщение лучшим 3 peer'ам через
   `routingTargetsForMessage()`:
   - broadcast (`recipient="*"`): все full-ноды
   - адресное: все full-ноды + конкретный получатель (если client)
5. Если у целевого peer'а нет активной сессии, сообщение ставится в
   `pending[address]` и отправляется при переподключении.

```mermaid
flowchart LR
    subgraph "Отправитель (Desktop)"
        UI["UI: SendMessage()"]
        ENC["Шифрование<br/>X25519 box key"]
    end

    subgraph "Локальная нода"
        VAL["Проверка ed25519 подписи<br/>+ дедупликация"]
        STORE["MessageStore.<br/>StoreMessage()"]
        GOSSIP["gossipMessage()<br/>→ лучшие 3 peer'а"]
    end

    subgraph "Relay-ноды"
        R1["Full-нода 1"]
        R2["Full-нода 2"]
    end

    subgraph "Нода получателя"
        RCV["Получение + проверка"]
        RSTORE["StoreMessage()"]
        RECEIPT["emitDeliveryReceipt()"]
    end

    UI --> ENC --> VAL --> STORE
    VAL --> GOSSIP
    GOSSIP --> R1 --> RCV
    GOSSIP --> R2 --> RCV
    RCV --> RSTORE --> RECEIPT

    style UI fill:#e1f5fe,stroke:#333
    style RECEIPT fill:#c8e6c9,stroke:#333
```
*Диаграмма 5 — Маршрутизация прямых сообщений и gossip*

**Delivery receipts:**

1. При получении адресованного DM `emitDeliveryReceipt()` автоматически
   генерирует квитанцию `"delivered"` и сохраняет через `storeDeliveryReceipt()`.
2. Квитанция распространяется обратно к отправителю.
3. Квитанции `"seen"` отправляются при вызове `MarkConversationSeen()` —
   когда активный чат на экране (`SelectPeer` и `AutoSelectPeer`).
4. Квитанции сохраняются в chatlog через `UpdateDeliveryStatus()` ДО генерации
   события, сохраняя инвариант «сначала БД, потом событие».

```mermaid
sequenceDiagram
    participant A as Нода отправителя
    participant Mesh as Mesh (gossip)
    participant B as Нода получателя

    A->>Mesh: send_message (зашифрованный DM)
    Mesh->>B: доставка сообщения
    B->>B: StoreMessage() → chatlog.db
    B->>B: emitDeliveryReceipt("delivered")
    B-->>Mesh: delivery_receipt "delivered"
    Mesh-->>A: delivery_receipt "delivered"
    A->>A: UpdateDeliveryStatus("delivered")

    Note over B: Пользователь открывает чат
    B->>B: MarkConversationSeen()
    B-->>Mesh: delivery_receipt "seen"
    Mesh-->>A: delivery_receipt "seen"
    A->>A: UpdateDeliveryStatus("seen")
```
*Диаграмма 6 — Поток квитанций доставки и обновление статусов*

**Транзитный relay:**

Сообщения, где ни sender, ни recipient не являются этой нодой, ретранслируются:

- Хранятся в `relayRetry` с TTL 3 минуты.
- Персистятся в `queue-{port}.json` между рестартами.
- Повторяются при переподключении через `retryRelayDeliveries()`.

### Очередь pending-фреймов

Исходящие фреймы, которые не могут быть отправлены сразу, ставятся в очередь
по адресу peer'а в `pending[address]`. Каждая запись хранит фрейм, время
постановки и счётчик повторов. При переподключении `flushPendingPeerFrames()`
дренирует очередь. Очередь персистится в `queue-{port}.json` и переживает
рестарты.

Legacy-записи с ключами по fallback-портам мигрируются при загрузке;
неразрешимые записи перемещаются в секцию `"orphaned"`.

### Сетевые группы и NAT

Peer'ы классифицируются по доступным сетевым группам через `ClassifyAddress()`:

- **IPv4**, **IPv6**, **TorV3**, **TorV2**, **I2P**, **CJDNS**, **Local**,
  **Unknown**

Фрейм `hello` объявляет доступные группы ноды (`"networks"`). Обмен peer'ами
фильтрует обнаруженные адреса по пересечению доступных групп — ноды, которые
не могут достичь определённый класс сети, не получат адресов для него.

**NAT-обнаружение:** Каждый peer сообщает наблюдаемый IP подключающейся ноды
в `welcome`. Когда ≥ 2 различных peer'ов сообщают один и тот же IP, нода
принимает его как внешний адрес (порог консенсуса). Адрес информативный —
нода никогда не перезаписывает настройку advertise автоматически.

**Поддержка Tor:** Установка `CORSA_PROXY=127.0.0.1:9050` направляет
соединения через SOCKS5-прокси, обеспечивая резолв `.onion`-адресов.

### Trust model (TOFU)

Реализован в `trustStore` (`trust.go`):

1. Fingerprint-адрес = `hex(sha256(ed25519_pubkey)[:20])`.
2. Привязка box-key: `ed25519.Sign(identity_key, "corsa-boxkey-v1|" + address + "|" + boxkey_base64)`.
3. При первом валидном обмене контактами набор ключей pin-ится локально в
   `trust-{port}.json` (Trust On First Use).
4. Последующие конфликтующие ключи для того же адреса отклоняются и
   записываются в секцию `"conflicts"` для ручной проверки.

### Gazeta — зашифрованные notice с TTL

Канал типа dead-drop / bulletin-board для зашифрованных notice:

1. Отправитель шифрует notice X25519 box key получателя, используя
   эфемерный ключ + AES-GCM.
2. Отправляет `publish_notice { ttl_seconds, ciphertext }`.
3. Notice распространяется через gossip по всем full-нодам.
4. Peer'ы хранят notice до истечения TTL, затем удаляют.
5. Получатели запрашивают через `fetch_notices` и расшифровывают своим
   identity key.

### Файлы персистентности

| Файл | Содержимое |
|---|---|
| `peers-{port}.json` | Известные адреса peer'ов, скоры, метаданные |
| `trust-{port}.json` | TOFU-pinned контакты и лог конфликтов |
| `queue-{port}.json` | Pending исходящие фреймы, состояние retry relay |
| `chatlog.db` (SQLite) | История сообщений, статусы доставки (только desktop) |
