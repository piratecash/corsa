# CORSA Desktop UI

## English

### Overview

The desktop UI is built with [Gio](https://gioui.org) — a portable immediate-mode GUI library for Go. The UI layer is thin: it reads state from the `DMRouter` via atomic snapshots and delegates all business logic to the service layer.

### Component hierarchy

```
Window (Gio event loop)
  ├── Header (console button, language selector, update badge)
  ├── Sidebar (contacts card)
  │   ├── Identity search
  │   ├── Recipient list (from router peers)
  │   │   └── Reachable indicator (green/gray dot)
  │   └── Context menu (copy, alias, delete)
  ├── Chat area
  │   ├── Message list (scrollable)
  │   └── Message bubbles (with delivery status)
  │       ├── Author + timestamp (DD.MM.YYYY HH:MM)
  │       ├── Reply quote (if reply): sender · date + quoted text
  │       │   └── Click scrolls to original message
  │       ├── Message body (selectable text)
  │       ├── Delivery status (sent/delivered/seen)
  │       └── Context menu (right-click: Reply, Copy)
  └── Composer card
      ├── Recipient display
      ├── Reply preview banner (when replying)
      ├── Message input
      ├── Send button
      └── Status line (send/delete/sync feedback)
```

### Initialization sequence

```mermaid
sequenceDiagram
    participant Main as main()
    participant App as desktop.Run()
    participant Node as node.Service
    participant Client as DesktopClient
    participant Router as DMRouter
    participant Cmd as CommandTable
    participant Win as Window

    Main->>App: desktop.Run()
    App->>App: config.Default()
    App->>App: identity.LoadOrCreate()
    App->>App: LoadPreferences()

    App->>App: eventBus = ebus.New()
    App->>Node: node.NewService(cfg, id, eventBus)
    App->>App: NodeRuntime.Start(ctx)
    Note over Node: Spawns: bootstrap loop,<br/>TCP listener, relay ticker,<br/>routing TTL loop

    App->>Client: NewDesktopClient(cfg, id, node)
    Note over Client: Creates chatlog.Store<br/>Registers as MessageStore

    App->>Router: NewDMRouter(client, fileBridge, eventBus)
    Note over Router: Empty peers, cache,<br/>32-slot event channel

    App->>Cmd: NewCommandTable()
    App->>Cmd: RegisterAllCommands(cmdTable, nodeService, client, router, metricsCollector)
    App->>Cmd: RegisterDesktopOverrides(cmdTable, client, nodeService)

    App->>App: rpc.NewServer(cfg, cmdTable, node)
    Note over App: HTTP server for<br/>external clients

    App->>Win: NewWindow(client, router, cmdTable, runtime, prefs)
    App->>Win: window.Run()
```

*Initialization sequence*

### DMRouter startup

```mermaid
sequenceDiagram
    participant Win as Window
    participant Router as DMRouter
    participant eBus as ebus.Bus
    participant Client as DesktopClient
    participant DB as chatlog.Store
    participant Node as node.Service

    Win->>Router: Start()
    Router->>eBus: subscribeEvents()
    Note over Router: Subscribes to:<br/>aggregate.status.changed,<br/>peer.connected/disconnected,<br/>peer.health.changed,<br/>contacts.changed,<br/>identity.changed
    Router->>Router: runStartup() [goroutine 1]
    Router->>Router: runEventListener() [goroutine 2]

    Note over Router: goroutine 1: initializeFromDB

    Router->>Router: resetIdentityState()
    Router->>Client: FetchConversationPreviews()
    Client->>DB: ReadLastEntryPerPeerCtx()
    Client->>DB: ListConversationsCtx()
    DB-->>Client: []ConversationPreview
    Client-->>Router: previews

    Router->>Router: seedPreviews(previews)
    Note over Router: ensurePeerLocked() for<br/>each chatlog peer.<br/>Sort: unread first,<br/>then by timestamp.

    Router->>Router: AutoSelectPeer(firstPeer)
    Router->>Client: FetchConversation(peer)
    Client->>DB: Read("dm", peer)
    DB-->>Router: []DirectMessage

    Router->>Router: pollHealth() [deferred, one-time]
    Router->>Client: ProbeNode()
    Client->>Node: fetch_peer_health, fetch_dm_headers, ...
    Node-->>Router: NodeStatus

    Router->>Router: close(startupDone)
    Note over Router: Real-time updates<br/>arrive via ebus events
```

*DMRouter startup sequence*

### Event-driven UI updates

The node layer pushes state changes via an internal event bus (`ebus.Bus`). The DMRouter subscribes to relevant topics and updates its snapshot on each event. Messages and receipts are still delivered via the legacy `SubscribeLocalChanges` channel during migration.

```mermaid
flowchart LR
    subgraph Node["node.Service"]
        MSG[New message arrives]
        RCV[Receipt update]
        PEER[Peer state change]
        AGG[Aggregate status change]
    end

    subgraph eBus["ebus.Bus"]
        PUB[Publish topic]
    end

    subgraph Router["DMRouter"]
        EVT[handleEvent]
        EBUS_H[ebus handler]
        SIDE[updateSidebarFromEvent]
        ENSURE[ensurePeerLocked]
        NOTIFY[notify UIEvent]
    end

    subgraph Window["Window"]
        SUB[Subscribe channel]
        INV[window.Invalidate]
        SNAP[router.Snapshot]
        LAYOUT[layout / render]
    end

    MSG --> EVT
    RCV --> EVT
    PEER --> PUB
    AGG --> PUB
    PUB --> EBUS_H
    EBUS_H --> NOTIFY
    EVT --> SIDE
    SIDE --> ENSURE
    ENSURE --> NOTIFY
    NOTIFY --> SUB
    SUB --> INV
    INV --> SNAP
    SNAP --> LAYOUT
```

*Event-driven UI update flow*

### Identity lifecycle

```mermaid
stateDiagram-v2
    [*] --> InMemory: App startup
    InMemory --> InMemory: New message (ensurePeerLocked)

    state InMemory {
        [*] --> Loaded: seedPreviews (from chatlog)
        Loaded --> Updated: updateSidebarFromEvent
        Updated --> Updated: repairUnreadFromHeaders
    }

    InMemory --> Deleted: RemovePeer()

    state Deleted {
        [*] --> TrustStoreCleared: DeleteContact
        TrustStoreCleared --> ChatlogCleared: DeletePeerHistory
        ChatlogCleared --> MemoryCleared: delete(peers), removePeerLocked, cache.Evict
        MemoryCleared --> UINotified: notify(UIEventSidebarUpdated)
    }

    Deleted --> [*]
```

*Identity lifecycle*

Identity enters the system through two paths:

1. **Startup** — `seedPreviews` reads conversation previews from the chatlog database and calls `ensurePeerLocked` for each peer address.
2. **Runtime** — when a new message arrives from an unknown identity, `updateSidebarFromEvent` and `repairUnreadFromHeaders` call `ensurePeerLocked` to add the peer.

Identity exits through `RemovePeer`:

1. `DeleteContact` — removes from the node trust store (persisted JSON file)
2. `DeletePeerHistory` — removes all chat messages from SQLite
3. In-memory cleanup — `peers`, `peerOrder`, `cache` cleared
4. UI notification — sidebar rebuilds from `peers` immediately

### Sidebar data source

The sidebar recipient list is built exclusively from the router's in-memory `peers` map. There is no dependency on polling or external contact sources:

```
snapRecipients()
  └── snap.Peers (router in-memory state)
      ├── Seeded from chatlog at startup
      ├── Updated by incoming messages in real-time
      └── Cleaned on RemovePeer
```

### UIEvent types

| Event | Trigger | UI effect |
|-------|---------|-----------|
| `UIEventMessagesUpdated` | New message, receipt update, conversation switch | Chat area redraws |
| `UIEventSidebarUpdated` | Peer added/removed, unread count changed, preview updated | Sidebar redraws |
| `UIEventStatusUpdated` | Health poll completed | Network status indicator updates |
| `UIEventBeep` | New incoming message (not during startup replay) | System notification sound |

### Reachable indicator

Each contact in the sidebar displays a small colored dot next to the peer name. The indicator has three states:

- **Green** — at least one route exists (identity is reachable through the mesh)
- **Gray** — no route available (identity is unreachable)
- **Gray outline** — reachability data is unavailable (probe failed or node not connected)

The reachability data is populated during each `ProbeNode` cycle. In embedded mode, `NodeProber.BuildReachableIDs()` (delegated from `DesktopClient.BuildReachableIDs()`) calls `node.Service.RoutingSnapshot()` directly (no RPC round-trip) and extracts all identities with a live `BestRoute`. In remote TCP mode (`localNode == nil`), it falls back to the `fetch_reachable_ids` frame, which performs the same logic on the node side. The reachable set covers all identities in the routing table — not just those from `fetch_identities` — so sidebar peers that entered via chatlog or DM headers also get correct status. Results are stored in `NodeStatus.ReachableIDs` and flow through the standard `RouterSnapshot` pipeline to the UI.

### Contact list sorting

The sidebar contact list uses 4-tier priority sorting. This is a UI/product concern — the router provides data (peers, unread counts, reachability), and the presentation layer (`sidebar_sort.go`) decides display order. Sorting runs on every frame render using the current `RouterSnapshot`, so any state change (unread cleared, preview refreshed, reachability updated) is immediately reflected without explicit re-sort triggers.

| Tier | Condition | Sort key |
|------|-----------|----------|
| 1 | Online + unread messages | Unread count descending |
| 2 | Online, no unread | Last message timestamp descending |
| 3 | Offline + unread messages | Unread count descending |
| 4 | Offline, no unread | Last message timestamp descending |

"Online" means `ReachableIDs[identity] == true` — at least one live route exists in the routing table.

The sort pipeline in `snapRecipients()`:

1. `mergeRecipientOrder()` — merges peers from `Peers` map with `PeerOrder` (router's internal ordering, used as stable tiebreaker)
2. `sortSidebarPeers()` — applies 4-tier sort using `RouterSnapshot.Peers` and `RouterSnapshot.NodeStatus.ReachableIDs`

When `ReachableIDs` is nil (probe not completed or failed), all peers are treated as offline, and the sort degrades gracefully to 2-tier (unread first, then by timestamp).

### Сортировка списка контактов

Sidebar список контактов использует 4-уровневую приоритетную сортировку. Это UI/продуктовая логика — роутер предоставляет данные (peers, счётчики непрочитанных, доступность), а слой представления (`sidebar_sort.go`) определяет порядок отображения. Сортировка выполняется на каждом кадре рендеринга из текущего `RouterSnapshot`, поэтому любое изменение состояния (очистка непрочитанных, обновление preview, изменение доступности) немедленно отражается без явных триггеров пересортировки.

| Уровень | Условие | Ключ сортировки |
|---------|---------|-----------------|
| 1 | Online + есть непрочитанные | Число непрочитанных по убыванию |
| 2 | Online, нет непрочитанных | Время последнего сообщения по убыванию |
| 3 | Offline + есть непрочитанные | Число непрочитанных по убыванию |
| 4 | Offline, нет непрочитанных | Время последнего сообщения по убыванию |

"Online" означает `ReachableIDs[identity] == true` — хотя бы один живой маршрут существует в таблице маршрутизации.

Конвейер сортировки в `snapRecipients()`:

1. `mergeRecipientOrder()` — объединяет peers из `Peers` map с `PeerOrder` (внутренний порядок роутера, используется как стабильный tiebreaker)
2. `sortSidebarPeers()` — применяет 4-уровневую сортировку используя `RouterSnapshot.Peers` и `RouterSnapshot.NodeStatus.ReachableIDs`

Когда `ReachableIDs` равен nil (проба не завершена или не удалась), все peers считаются offline, и сортировка корректно деградирует до 2-уровневой (непрочитанные первыми, затем по timestamp).

### RPC architecture

```mermaid
flowchart TD
    subgraph External["External clients"]
        CLI[corsa-cli]
        API[Third-party tools]
    end

    subgraph Desktop["Desktop app"]
        CON[Console window]
        WIN[Main window]
    end

    subgraph RPC["RPC layer"]
        HTTP[HTTP server]
        CMD[CommandTable]
    end

    subgraph Commands["Command groups"]
        SYS[System: help, ping, version]
        NET[Network: getPeers, addPeer]
        ID[Identity: fetchContacts,<br/>fetchTrustedContacts]
        MSG[Messages: sendDm,<br/>fetchMessages]
        CHAT[Chatlog: fetchChatlogPreviews]
        METRICS[Metrics: fetchTrafficHistory]
        DIAG[Diagnostic: recordPeerTraffic*,<br/>stopPeerTrafficRecording]
    end

    subgraph Core["Core services"]
        NODE[node.Service]
        ROUTER[DMRouter]
        CHATLOG[chatlog.Store]
        CAP[CaptureManager]
    end

    CLI --> HTTP
    API --> HTTP
    HTTP --> CMD
    CON --> CMD
    CMD --> SYS
    CMD --> NET
    CMD --> ID
    CMD --> MSG
    CMD --> CHAT
    CMD --> METRICS
    CMD --> DIAG
    SYS --> NODE
    NET --> NODE
    ID --> NODE
    MSG --> NODE
    MSG --> ROUTER
    CHAT --> CHATLOG
    DIAG --> CAP
```

*RPC architecture*

The `CommandTable` is a single registry of all available commands. Desktop UI calls `Execute()` directly (no HTTP round-trip). External clients go through the HTTP server which wraps the same `CommandTable`.

### Console Window — Traffic Recording Indicators

The Console Window (opened via the header console button) displays per-peer diagnostic information. When a capture session is active, the following UI elements appear:

- **Recording dot** — a small red ellipse on the peer card header next to the peer address. Visible when `NodeStatus.CaptureSessions` contains an `Active` entry whose `ConnID` matches the peer row.
- **Recording info row** — displayed below the peer card health data. Shows scope (`conn_id` / `ip` / `all`), file path (selectable text), capture start time, and dropped event count if non-zero. An error string is shown if the capture writer encountered a disk error.
- **Stop all recording banner** — a red banner at the top of the peers tab. Visible when `NodeStatus.CaptureSessions` contains any `Active` entry. Contains a "Stop all" button that dispatches `stopPeerTrafficRecording scope=all` via `CommandTable.Execute()`.

Capture sessions live in a dedicated `map[domain.ConnID]service.CaptureSession` field on `NodeStatus` — separate from `PeerHealth`. This separation guarantees that capture bookkeeping cannot corrupt peer-health rows: capture-start never materializes a peer row, and capture-stop never strips fields from one. The UI derives recording visibility by looking up the peer's `ConnID` in that map.

State is seeded from `ProbeNode` at startup — `captureSessionsFromFrame` extracts one `CaptureSession` per `fetch_peer_health` entry whose `Recording` flag is set — and kept live via two ebus topics published from `traffic_capture_bridge.go`:

- `TopicCaptureSessionStarted` inserts a `CaptureSession` keyed by the event's `ConnID` with `Active=true`, `FilePath`, `StartedAt`, `Scope`, and `Format` copied from the event. Unknown/empty `Format` falls back to `domain.CaptureFormatCompact`. A restart on the same `ConnID` overwrites any lingering stopped entry so diagnostic counters reset.
- `TopicCaptureSessionStopped` marks the matching entry `Active=false`, stamps `StoppedAt` from the monitor's injectable clock, and records the terminal `Error` / `DroppedEvents`. Stopped entries linger for `NodeStatusMonitor.captureRetention` (default 60 seconds) so the UI can surface the failure reason after the writer goes away. A stop event for an unknown `ConnID` is logged and ignored — no peer-row side effects.

The lazy TTL sweep runs at the start of every `applyCaptureStarted` and at the end of every `applyCaptureStopped`: entries whose `StoppedAt` is older than `captureRetention` are deleted in-place. There is no background goroutine — retention is bounded by the frequency of capture-handler invocations, which is acceptable because a stopped session only matters to the UI while the user is still looking at it.

The `CaptureSessionStarted` payload carries the overlay identity envelope (`Address`, `PeerID`, `Direction`) so the UI can still label a recording when the corresponding `PeerHealth` row has not yet arrived — the label is read directly off the `CaptureSession` rather than from a cross-referenced peer row. This removes the earlier class of bugs where capture-only placeholder rows survived after stop, accidentally graduated via address-scoped traffic events, or silently overwrote real health state.

The payload contract permits an empty `Address` when the publisher could not resolve the connection (torn down between `StartCapture` and the publish, or never tracked). The session is still stored on `NodeStatus.CaptureSessions` so the writer stays visible to the "Stop all recordings" path, but the desktop fallback treats such sessions as unlabeled: `captureHasIdentity` returns false when both `Address` and `PeerID` are empty, and `mergeCapturesIntoPeers` / `countUniquePeers` / `countConnectedPeers` all skip them. Without this gate, unresolved captures would render as blank peer cards and all collapse into a single phantom entry under the empty-string dedup key (`peerIdentityKey("", "") == ""`), inflating `known_peers` / `connected_peers` by exactly one regardless of how many unresolved captures are active.

`mergeCapturesIntoPeers` reconciles each active capture against `peers` with three ordered rules: (1) an existing row with the same `ConnID` is authoritative and the capture is skipped; (2) otherwise, if a `ConnID=0` address-level placeholder (seeded by `applySlotStateDelta` or `applyPeerPendingDelta`) shares the capture's `Address`, the placeholder is promoted in place — `ConnID`, `Direction`, and `Connected` come from the capture, while `SlotState`, `PendingCount`, and any already-observed `PeerID` are preserved; (3) otherwise a fresh synthetic row is appended via `synthesizePeerHealthFromCapture`. Promotion prevents the split-state duplicate where a slot-only placeholder and an orphan capture for the same peer would render as two separate cards until a later health delta reconciles them. The function still honors the "does not mutate the caller's slice" contract via copy-on-write: the input slice is cloned the first time a promotion is required so diagnostic snapshots keep reading the original placeholder unchanged.

---

## Русский

### Обзор

Desktop UI построен на [Gio](https://gioui.org) — кроссплатформенной immediate-mode GUI библиотеке для Go. UI-слой тонкий: читает состояние из `DMRouter` через атомарные снимки и делегирует всю бизнес-логику в сервисный слой.

### Иерархия компонентов

```
Window (Gio event loop)
  ├── Header (кнопка консоли, выбор языка, бейдж обновления)
  ├── Sidebar (карточка контактов)
  │   ├── Поиск identity
  │   ├── Список получателей (из peers роутера)
  │   │   └── Индикатор достижимости (зеленая/серая точка)
  │   └── Контекстное меню (копировать, псевдоним, удалить)
  ├── Область чата
  │   ├── Список сообщений (скроллируемый)
  │   └── Пузыри сообщений (со статусом доставки)
  │       ├── Автор + дата (ДД.ММ.ГГГГ ЧЧ:ММ)
  │       ├── Цитата ответа (если ответ): отправитель · дата + текст
  │       │   └── Клик прокручивает к оригинальному сообщению
  │       ├── Тело сообщения (выделяемый текст)
  │       ├── Статус доставки (отправлено/доставлено/прочитано)
  │       └── Контекстное меню (правый клик: Ответить, Копировать)
  └── Карточка ввода
      ├── Отображение получателя
      ├── Баннер предпросмотра ответа (при ответе)
      ├── Поле ввода сообщения
      ├── Кнопка отправки
      └── Строка статуса (обратная связь по отправке/удалению/синхронизации)
```

### Последовательность инициализации

```mermaid
sequenceDiagram
    participant Main as main()
    participant App as desktop.Run()
    participant Node as node.Service
    participant Client as DesktopClient
    participant Router as DMRouter
    participant Cmd as CommandTable
    participant Win as Window

    Main->>App: desktop.Run()
    App->>App: config.Default()
    App->>App: identity.LoadOrCreate()
    App->>App: LoadPreferences()

    App->>App: eventBus = ebus.New()
    App->>Node: node.NewService(cfg, id, eventBus)
    App->>App: NodeRuntime.Start(ctx)
    Note over Node: Запускает: bootstrap loop,<br/>TCP listener, relay ticker,<br/>routing TTL loop

    App->>Client: NewDesktopClient(cfg, id, node)
    Note over Client: Создает chatlog.Store<br/>Регистрирует как MessageStore

    App->>Router: NewDMRouter(client, fileBridge, eventBus)
    Note over Router: Пустые peers, cache,<br/>32-слотовый event channel

    App->>Cmd: NewCommandTable()
    App->>Cmd: RegisterAllCommands(cmdTable, nodeService, client, router, metricsCollector)
    App->>Cmd: RegisterDesktopOverrides(cmdTable, client, nodeService)

    App->>App: rpc.NewServer(cfg, cmdTable, node)
    Note over App: HTTP сервер для<br/>внешних клиентов

    App->>Win: NewWindow(client, router, cmdTable, runtime, prefs)
    App->>Win: window.Run()
```

*Последовательность инициализации*

### Запуск DMRouter

```mermaid
sequenceDiagram
    participant Win as Window
    participant Router as DMRouter
    participant eBus as ebus.Bus
    participant Client as DesktopClient
    participant DB as chatlog.Store
    participant Node as node.Service

    Win->>Router: Start()
    Router->>eBus: subscribeEvents()
    Note over Router: Подписка на:<br/>aggregate.status.changed,<br/>peer.connected/disconnected,<br/>peer.health.changed,<br/>contacts.changed,<br/>identity.changed
    Router->>Router: runStartup() [горутина 1]
    Router->>Router: runEventListener() [горутина 2]

    Note over Router: горутина 1: initializeFromDB

    Router->>Router: resetIdentityState()
    Router->>Client: FetchConversationPreviews()
    Client->>DB: ReadLastEntryPerPeerCtx()
    Client->>DB: ListConversationsCtx()
    DB-->>Client: []ConversationPreview
    Client-->>Router: previews

    Router->>Router: seedPreviews(previews)
    Note over Router: ensurePeerLocked() для<br/>каждого peer из chatlog.<br/>Сортировка: непрочитанные<br/>первыми, потом по времени.

    Router->>Router: AutoSelectPeer(firstPeer)
    Router->>Client: FetchConversation(peer)
    Client->>DB: Read("dm", peer)
    DB-->>Router: []DirectMessage

    Router->>Router: pollHealth() [deferred, однократно]
    Router->>Client: ProbeNode()
    Client->>Node: fetch_peer_health, fetch_dm_headers, ...
    Node-->>Router: NodeStatus

    Router->>Router: close(startupDone)
    Note over Router: Обновления в реальном<br/>времени через ebus события
```

*Последовательность запуска DMRouter*

### Event-driven обновление UI

Слой node.Service отправляет изменения состояния через внутреннюю шину событий (`ebus.Bus`). DMRouter подписывается на нужные топики и обновляет свой снапшот при каждом событии. Сообщения и квитанции доставки пока доставляются через legacy-канал `SubscribeLocalChanges` в процессе миграции.

```mermaid
flowchart LR
    subgraph Node["node.Service"]
        MSG[Приходит сообщение]
        RCV[Обновление статуса доставки]
        PEER[Изменение состояния пира]
        AGG[Изменение агрегатного статуса]
    end

    subgraph eBus["ebus.Bus"]
        PUB[Publish topic]
    end

    subgraph Router["DMRouter"]
        EVT[handleEvent]
        EBUS_H[ebus handler]
        SIDE[updateSidebarFromEvent]
        ENSURE[ensurePeerLocked]
        NOTIFY[notify UIEvent]
    end

    subgraph Window["Window"]
        SUB[Subscribe channel]
        INV[window.Invalidate]
        SNAP[router.Snapshot]
        LAYOUT[layout / render]
    end

    MSG --> EVT
    RCV --> EVT
    PEER --> PUB
    AGG --> PUB
    PUB --> EBUS_H
    EBUS_H --> NOTIFY
    EVT --> SIDE
    SIDE --> ENSURE
    ENSURE --> NOTIFY
    NOTIFY --> SUB
    SUB --> INV
    INV --> SNAP
    SNAP --> LAYOUT
```

*Поток event-driven обновлений UI*

### Жизненный цикл Identity

```mermaid
stateDiagram-v2
    [*] --> InMemory: Запуск приложения
    InMemory --> InMemory: Новое сообщение (ensurePeerLocked)

    state InMemory {
        [*] --> Loaded: seedPreviews (из chatlog)
        Loaded --> Updated: updateSidebarFromEvent
        Updated --> Updated: repairUnreadFromHeaders
    }

    InMemory --> Deleted: RemovePeer()

    state Deleted {
        [*] --> TrustStoreCleared: DeleteContact
        TrustStoreCleared --> ChatlogCleared: DeletePeerHistory
        ChatlogCleared --> MemoryCleared: delete(peers), removePeerLocked, cache.Evict
        MemoryCleared --> UINotified: notify(UIEventSidebarUpdated)
    }

    Deleted --> [*]
```

*Жизненный цикл identity*

Identity попадает в систему двумя путями:

1. **При запуске** — `seedPreviews` читает превью разговоров из chatlog БД и вызывает `ensurePeerLocked` для каждого адреса.
2. **В рантайме** — когда приходит сообщение от неизвестного identity, `updateSidebarFromEvent` и `repairUnreadFromHeaders` вызывают `ensurePeerLocked`.

Identity удаляется через `RemovePeer`:

1. `DeleteContact` — удаляет из trust store ноды (JSON файл)
2. `DeletePeerHistory` — удаляет все сообщения из SQLite
3. Очистка памяти — `peers`, `peerOrder`, `cache`
4. Уведомление UI — sidebar перестраивается из `peers` мгновенно

### Источник данных для sidebar

Список получателей в sidebar строится исключительно из in-memory map `peers` роутера. Нет зависимости от polling или внешних источников контактов:

```
snapRecipients()
  └── snap.Peers (in-memory состояние роутера)
      ├── Загружается из chatlog при старте
      ├── Обновляется входящими сообщениями в реальном времени
      └── Очищается при RemovePeer
```

### Типы UIEvent

| Event | Триггер | Эффект в UI |
|-------|---------|-------------|
| `UIEventMessagesUpdated` | Новое сообщение, обновление статуса доставки, переключение разговора | Перерисовка области чата |
| `UIEventSidebarUpdated` | Peer добавлен/удален, счетчик непрочитанных изменен, превью обновлено | Перерисовка sidebar |
| `UIEventStatusUpdated` | Завершен health poll | Обновление индикатора сети |
| `UIEventBeep` | Новое входящее сообщение (не во время стартового replay) | Системный звук уведомления |

### Индикатор достижимости

Каждый контакт в sidebar отображает маленькую цветную точку рядом с именем. Индикатор имеет три состояния:

- **Зелёный** — маршрут есть (identity достижим через mesh-сеть)
- **Серый** — маршрутов нет (identity недоступен)
- **Серый контур** — данные о достижимости недоступны (probe не удался или нода не подключена)

Данные о достижимости заполняются при каждом цикле `ProbeNode`. `NodeProber.BuildReachableIDs()` (делегация из `DesktopClient.BuildReachableIDs()`) вызывает `node.Service.RoutingSnapshot()` напрямую через embedded node (без RPC round-trip) и извлекает все identity с живым `BestRoute`. Набор достижимых identity строится из всей routing table — не только из `fetch_identities` — поэтому sidebar peers, попавшие через chatlog или DM headers, тоже получают корректный статус. Результат хранится в `NodeStatus.ReachableIDs` и проходит через стандартный pipeline `RouterSnapshot` до UI.

### Архитектура RPC

```mermaid
flowchart TD
    subgraph External["Внешние клиенты"]
        CLI[corsa-cli]
        API[Сторонние инструменты]
    end

    subgraph Desktop["Desktop приложение"]
        CON[Окно консоли]
        WIN[Главное окно]
    end

    subgraph RPC["RPC слой"]
        HTTP[HTTP сервер]
        CMD[CommandTable]
    end

    subgraph Commands["Группы команд"]
        SYS[System: help, ping, version]
        NET[Network: getPeers, addPeer]
        ID[Identity: fetchContacts,<br/>fetchTrustedContacts]
        MSG[Messages: sendDm,<br/>fetchMessages]
        CHAT[Chatlog: fetchChatlogPreviews]
        METRICS[Metrics: fetchTrafficHistory]
        DIAG[Diagnostic: recordPeerTraffic*,<br/>stopPeerTrafficRecording]
    end

    subgraph Core["Core сервисы"]
        NODE[node.Service]
        ROUTER[DMRouter]
        CHATLOG[chatlog.Store]
        CAP[CaptureManager]
    end

    CLI --> HTTP
    API --> HTTP
    HTTP --> CMD
    CON --> CMD
    CMD --> SYS
    CMD --> NET
    CMD --> ID
    CMD --> MSG
    CMD --> CHAT
    CMD --> METRICS
    CMD --> DIAG
    SYS --> NODE
    NET --> NODE
    ID --> NODE
    MSG --> NODE
    MSG --> ROUTER
    CHAT --> CHATLOG
    DIAG --> CAP
```

*Архитектура RPC*

`CommandTable` — единый реестр всех доступных команд. Desktop UI вызывает `Execute()` напрямую (без HTTP round-trip). Внешние клиенты работают через HTTP сервер, который оборачивает тот же `CommandTable`.

### Окно консоли — индикаторы записи трафика

Окно консоли (открывается кнопкой консоли в заголовке) отображает диагностическую информацию по каждому peer'у. Когда capture-сессия активна, появляются следующие UI-элементы:

- **Точка записи** — маленький красный эллипс на заголовке peer-карточки рядом с адресом. Виден когда `NodeStatus.CaptureSessions` содержит запись с `Active=true` и `ConnID`, совпадающим со строкой пира.
- **Строка информации о записи** — отображается под данными здоровья peer-карточки. Показывает scope (`conn_id` / `ip` / `all`), путь к файлу (выделяемый текст), время старта записи и количество потерянных событий если ненулевое. Строка ошибки показывается если capture writer столкнулся с ошибкой диска.
- **Баннер остановки записи** — красный баннер вверху вкладки peers. Виден когда `NodeStatus.CaptureSessions` содержит хотя бы одну запись с `Active=true`. Содержит кнопку "Stop all", которая отправляет `stopPeerTrafficRecording scope=all` через `CommandTable.Execute()`.

Capture-сессии хранятся в отдельном поле `map[domain.ConnID]service.CaptureSession` на `NodeStatus` — независимо от `PeerHealth`. Это разделение гарантирует, что capture-bookkeeping не может повредить строки peer-health: capture-start никогда не материализует строку пира, а capture-stop никогда не вычищает поля. UI определяет видимость записи, обращаясь по `ConnID` пира к этой карте.

Состояние изначально заполняется из `ProbeNode` при старте — `captureSessionsFromFrame` извлекает по одной `CaptureSession` на каждую запись `fetch_peer_health` с выставленным флагом `Recording` — и поддерживается актуальным через две ebus-темы, публикуемые из `traffic_capture_bridge.go`:

- `TopicCaptureSessionStarted` вставляет `CaptureSession` по ключу `ConnID` со значениями `Active=true`, `FilePath`, `StartedAt`, `Scope`, `Format`, скопированными из события. Неизвестный/пустой `Format` подменяется на `domain.CaptureFormatCompact`. Перезапуск на том же `ConnID` перезатирает любую "залежавшуюся" остановленную запись, чтобы сбросить диагностические счётчики.
- `TopicCaptureSessionStopped` помечает соответствующую запись как `Active=false`, фиксирует `StoppedAt` через инжектируемые часы монитора и записывает терминальные `Error` / `DroppedEvents`. Остановленные записи живут `NodeStatusMonitor.captureRetention` (по умолчанию 60 секунд), чтобы UI мог показать причину сбоя после ухода writer'а. Stop для неизвестного `ConnID` логируется и игнорируется — никаких побочек на peer-строки.

Ленивая чистка по TTL запускается в начале каждого `applyCaptureStarted` и в конце каждого `applyCaptureStopped`: записи, у которых `StoppedAt` старше `captureRetention`, удаляются in-place. Фоновой goroutine нет — частота чистки ограничена частотой вызовов capture-обработчиков, что приемлемо: остановленная сессия важна для UI ровно до тех пор, пока пользователь смотрит на неё.

Payload `CaptureSessionStarted` несёт overlay-идентичность (`Address`, `PeerID`, `Direction`), чтобы UI мог подписать запись, даже когда соответствующая строка `PeerHealth` ещё не пришла — лейбл читается прямо из `CaptureSession`, а не через cross-reference с peer-строкой. Это устраняет прежний класс багов, когда capture-only placeholder-строки выживали после stop, ошибочно "graduate"-или через address-scoped traffic-события или молча перезатирали реальное health-состояние.

Контракт payload разрешает пустой `Address`, если publisher не смог разрешить соединение (оно было закрыто между `StartCapture` и публикацией или никогда не отслеживалось). Сессия всё равно сохраняется в `NodeStatus.CaptureSessions`, чтобы writer оставался виден для пути "Stop all recordings", но desktop-fallback считает такие сессии неопознанными: `captureHasIdentity` возвращает false, когда оба поля `Address` и `PeerID` пусты, и `mergeCapturesIntoPeers` / `countUniquePeers` / `countConnectedPeers` их пропускают. Без этого фильтра неопознанные captures рендерились бы как пустые peer-карточки и все коллапсировали бы в единственную фантомную запись под пустым ключом дедупа (`peerIdentityKey("", "") == ""`), раздувая `known_peers` / `connected_peers` ровно на один элемент вне зависимости от количества активных неопознанных captures.

`mergeCapturesIntoPeers` сверяет каждую активную capture со списком `peers` по трём упорядоченным правилам: (1) строка с тем же `ConnID` авторитетна и capture пропускается; (2) иначе, если существует address-level placeholder с `ConnID=0` (создан `applySlotStateDelta` либо `applyPeerPendingDelta`) и совпадающим `Address`, placeholder promote'ится на месте — `ConnID`, `Direction` и `Connected` берутся из capture, а `SlotState`, `PendingCount` и уже наблюдаемый `PeerID` сохраняются; (3) иначе через `synthesizePeerHealthFromCapture` добавляется новая синтетическая строка. Promotion исключает split-state дубликат, при котором slot-only placeholder и сиротская capture для одного и того же peer'а рендерились бы как две отдельные карточки до прихода следующей health-delta. При этом инвариант "не мутировать слайс вызывающего" сохраняется через copy-on-write: входной слайс клонируется при первой же promotion, чтобы диагностические снапшоты продолжали видеть исходный placeholder без изменений.

