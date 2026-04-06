# Network Core — PeerConn Abstraction

## Problem

The `Service` struct in `internal/core/node/service.go` stores connection
state across 9 separate maps keyed by `net.Conn`:

| Map | Type | Purpose |
|-----|------|---------|
| `inboundConns` | `map[net.Conn]struct{}` | All active inbound connections |
| `inboundMetered` | `map[net.Conn]*MeteredConn` | Inbound conn → metered wrapper |
| `inboundTracked` | `map[net.Conn]struct{}` | Promoted connections (auth complete) |
| `connAuth` | `map[net.Conn]*connAuthState` | Authentication state |
| `connPeerInfo` | `map[net.Conn]*connPeerHello` | Identity, capabilities, staleness |
| `connSendCh` | `map[net.Conn]chan sendItem` | Per-connection write channel |
| `connWriterDone` | `map[net.Conn]chan struct{}` | Writer goroutine exit signal |
| `sessions` | `map[PeerAddress]*peerSession` | Outbound sessions (embeds net.Conn) |
| `inboundByIP` | `map[string]int` | Per-IP connection count |

54 functions accept `net.Conn` as a parameter. Any of them can call
`.Write()` directly, bypassing the designated single-writer goroutine.
The compiler cannot prevent this because `net.Conn` is an interface with
a public `Write` method.

Two of the three original unsafe write paths have been fixed:
`file_command` frames now go through `sendCh` (serialized by
`servePeerSession`), and `writeFrameToInbound` returns `false` on
`enqueueUnregistered` instead of falling back to direct `io.WriteString`.
The remaining unsafe path (`writeJSONFrame` / `writeJSONFrameSync`
falling back to direct `conn.Write` for outbound sessions) is tracked
in the roadmap.

## Design

### PeerConn type

```go
// PeerConn owns a single network connection and enforces the
// single-writer invariant at the type level. net.Conn is private —
// the only way to send data is through Send().
type PeerConn struct {
    id         connID
    direction  Direction           // inbound | outbound
    identity   domain.PeerIdentity // empty until handshake complete
    address    domain.PeerAddress
    caps       []domain.Capability
    networks   map[domain.NetGroup]struct{}

    // Writer — channel-serialized, one goroutine drains to socket.
    sendCh     chan sendItem
    writerDone chan struct{}

    // Reader — frames parsed by a dedicated goroutine.
    inboxCh    chan protocol.Frame  // outbound sessions only (request-reply)
    errCh      chan error           // outbound sessions only

    // Auth state (inbound only, nil for outbound).
    auth       *connAuthState

    // Metering.
    metered    *MeteredConn

    // Private — never exposed outside PeerConn methods.
    rawConn    net.Conn
    closeOnce  sync.Once

    // Staleness tracking.
    lastActivity time.Time
    connIDNum    uint64  // monotonic ID for diagnostics
}

type connID uint64

type Direction int

const (
    Inbound  Direction = iota
    Outbound
)
```

### Public API

```go
// Send enqueues a frame for writing. Non-blocking. Returns false if
// the write queue is full or the connection is closing.
func (pc *PeerConn) Send(frame protocol.Frame) bool

// SendSync enqueues a frame and blocks until the writer goroutine
// flushes it to the socket. Returns false on timeout or conn close.
func (pc *PeerConn) SendSync(frame protocol.Frame) bool

// HasCapability checks negotiated capabilities.
func (pc *PeerConn) HasCapability(cap domain.Capability) bool

// Identity returns the peer's Ed25519 fingerprint (empty before handshake).
func (pc *PeerConn) Identity() domain.PeerIdentity

// Close shuts down the connection (idempotent).
func (pc *PeerConn) Close()

// SetIdentity is called once during handshake when the peer's
// identity is established. After this call, the PeerConn can be
// indexed by identity in the Service.conns map.
func (pc *PeerConn) SetIdentity(id domain.PeerIdentity)

// SetCapabilities records the negotiated capability set.
func (pc *PeerConn) SetCapabilities(caps []domain.Capability)

// RemoteAddr returns the remote address (for logging/diagnostics only).
func (pc *PeerConn) RemoteAddr() string

// Direction returns Inbound or Outbound.
func (pc *PeerConn) Direction() Direction

// TouchActivity updates the last-activity timestamp.
func (pc *PeerConn) TouchActivity()
```

### What disappears

`net.Conn` is never passed to any function outside of `PeerConn` methods
and the handshake setup code. Functions that currently accept `net.Conn`
will accept `*PeerConn` instead. The 9 maps collapse into:

```go
// Primary index — all connections, inbound and outbound.
conns map[connID]*PeerConn

// Secondary indices (derived, rebuilt from conns).
connsByIdentity map[domain.PeerIdentity][]*PeerConn
connsByAddress  map[domain.PeerAddress]*PeerConn  // outbound only
```

### Writer goroutine lifecycle

```
NewPeerConn(rawConn, direction)
    │
    ├── spawns writerGoroutine(sendCh, rawConn)
    │       └── for item := range sendCh { rawConn.Write(item.data) }
    │
    ├── Send(frame) → sendCh <- item  (non-blocking)
    ├── SendSync(frame) → sendCh <- item{ack} → <-ack  (blocking)
    │
    └── Close()
            ├── close(sendCh)    → writer drains and exits
            ├── <-writerDone     → wait for drain
            └── rawConn.Close()  → TCP FIN
```

### Handshake flow

During handshake (hello/welcome exchange), `PeerConn` does not yet have
identity or capabilities. The handshake code uses `PeerConn.Send()` for
writing and reads from a `bufio.Reader` on `PeerConn.rawConn` — but only
within the single goroutine that owns the connection at that point.

```
1. Accept TCP connection → NewPeerConn(conn, Inbound)
2. handleConn reads hello frame (single goroutine, no concurrent writers)
3. pc.SetIdentity(hello.Address)
4. pc.SetCapabilities(negotiated)
5. Service registers pc in conns, connsByIdentity
6. From this point: Send() is the only write path
```

For outbound sessions, `servePeerSession` is the single owner:

```
1. Dial TCP → NewPeerConn(conn, Outbound)
2. servePeerSession sends hello via pc.Send()
3. Reads welcome, sets identity/caps
4. Service registers pc
5. Main loop: reads from pc.inboxCh, writes via pc.Send()
```

### Migration phases

**Step 1:** Create `peer_conn.go` with `PeerConn` type. Both inbound
`connWriter` and outbound `servePeerSession` write paths are unified
inside `PeerConn.writerGoroutine`.

**Step 2:** Replace `connSendCh` and `connWriterDone` maps with
`PeerConn.sendCh` and `PeerConn.writerDone`. Functions that called
`enqueueFrame(conn, data)` now call `pc.Send(frame)`.

**Step 3:** Replace `connPeerInfo` with `PeerConn.identity`, `.caps`,
`.networks`. Functions that looked up `connPeerInfo[conn]` now receive
`*PeerConn` directly.

**Step 4:** Replace `connAuth` with `PeerConn.auth`. Replace
`inboundConns`, `inboundMetered`, `inboundTracked` with queries on
`Service.conns`.

**Step 5:** Replace `peerSession` with `PeerConn` (outbound direction).
The `sessions` map becomes `connsByAddress`.

**Step 6:** Remove `net.Conn` from all function signatures. Replace
with `*PeerConn`. The `writeJSONFrame(conn, frame)` function becomes
`pc.Send(frame)` — no fallback path, no direct writes.

Each step is a self-contained commit. Tests pass after every step.

### Fallback elimination

The three unsafe fallback paths disappear:

| Before | After |
|--------|-------|
| `writeFrameToInbound`: `enqueueUnregistered` → direct `io.WriteString` | `pc.Send()` — always goes through channel; no fallback |
| `writeJSONFrame`: `enqueueUnregistered` → direct `conn.Write` | `pc.Send()` — unified for both directions |
| `writeJSONFrameSync`: `enqueueUnregistered` → direct `conn.Write` | `pc.SendSync()` — blocks on ack channel |

If the write channel is full or closed, `Send()` returns `false`. The
caller handles the failure (log + close connection). No silent fallback
to unprotected writes.

## Future: NetCore package (Phase 2)

After `PeerConn` is stable, the entire type and its writer goroutine
move into `internal/core/netcore`. `Service` communicates through an
interface:

```go
// internal/core/netcore/network.go
type Network interface {
    Send(dst PeerID, frame Frame) bool
    SendWithCap(dst PeerID, frame Frame, cap Capability) bool
    Broadcast(frame Frame, filter func(PeerID) bool)
    PeersByCapability(cap Capability) []PeerID
    Disconnect(dst PeerID)
}
```

`net.Conn` is not importable outside `netcore`. Any attempt to bypass
the write queue is a compile error, not a code review finding.

---

# Сетевое ядро — абстракция PeerConn

## Проблема

Структура `Service` в `internal/core/node/service.go` хранит состояние
соединений в 9 отдельных map'ах с ключом `net.Conn`:

| Map | Тип | Назначение |
|-----|-----|------------|
| `inboundConns` | `map[net.Conn]struct{}` | Все активные входящие соединения |
| `inboundMetered` | `map[net.Conn]*MeteredConn` | Входящее соед. → обёртка для метрик |
| `inboundTracked` | `map[net.Conn]struct{}` | Промотированные соединения (auth complete) |
| `connAuth` | `map[net.Conn]*connAuthState` | Состояние аутентификации |
| `connPeerInfo` | `map[net.Conn]*connPeerHello` | Identity, capabilities, staleness |
| `connSendCh` | `map[net.Conn]chan sendItem` | Канал записи для каждого соединения |
| `connWriterDone` | `map[net.Conn]chan struct{}` | Сигнал выхода writer-горутины |
| `sessions` | `map[PeerAddress]*peerSession` | Исходящие сессии (содержит net.Conn) |
| `inboundByIP` | `map[string]int` | Количество соединений по IP |

54 функции принимают `net.Conn` как параметр. Любая из них может вызвать
`.Write()` напрямую, обходя назначенную writer-горутину. Компилятор не
может этого предотвратить, потому что `net.Conn` — интерфейс с публичным
методом `Write`.

Два из трёх исходных небезопасных путей записи исправлены:
фреймы `file_command` теперь идут через `sendCh` (сериализуются
`servePeerSession`), а `writeFrameToInbound` возвращает `false` при
`enqueueUnregistered` вместо отката к прямому `io.WriteString`.
Оставшийся небезопасный путь (`writeJSONFrame` / `writeJSONFrameSync`
откатываются к прямому `conn.Write` для исходящих сессий) отслеживается
в roadmap.

## Дизайн

### Тип PeerConn

```go
// PeerConn владеет одним сетевым соединением и обеспечивает инвариант
// единственного writer'а на уровне типа. net.Conn приватен — единственный
// способ отправить данные — через Send().
type PeerConn struct {
    id         connID
    direction  Direction           // inbound | outbound
    identity   domain.PeerIdentity // пуст до завершения handshake
    address    domain.PeerAddress
    caps       []domain.Capability
    networks   map[domain.NetGroup]struct{}

    // Writer — сериализация через канал, одна горутина пишет в сокет.
    sendCh     chan sendItem
    writerDone chan struct{}

    // Reader — фреймы разбираются выделенной горутиной.
    inboxCh    chan protocol.Frame  // только для исходящих сессий
    errCh      chan error           // только для исходящих сессий

    // Состояние авторизации (только для входящих, nil для исходящих).
    auth       *connAuthState

    // Метрики.
    metered    *MeteredConn

    // Приватное — никогда не выставляется за пределы методов PeerConn.
    rawConn    net.Conn
    closeOnce  sync.Once

    // Трекинг активности.
    lastActivity time.Time
    connIDNum    uint64
}
```

### Публичный API

```go
func (pc *PeerConn) Send(frame protocol.Frame) bool       // неблокирующий
func (pc *PeerConn) SendSync(frame protocol.Frame) bool   // блокирующий с ack
func (pc *PeerConn) HasCapability(cap domain.Capability) bool
func (pc *PeerConn) Identity() domain.PeerIdentity
func (pc *PeerConn) Close()
func (pc *PeerConn) SetIdentity(id domain.PeerIdentity)
func (pc *PeerConn) SetCapabilities(caps []domain.Capability)
func (pc *PeerConn) RemoteAddr() string
func (pc *PeerConn) Direction() Direction
func (pc *PeerConn) TouchActivity()
```

### Что исчезает

`net.Conn` никогда не передаётся ни одной функции за пределами методов
`PeerConn` и кода инициализации handshake. Функции, которые сейчас
принимают `net.Conn`, будут принимать `*PeerConn`. 9 map'ов сворачиваются в:

```go
conns           map[connID]*PeerConn
connsByIdentity map[domain.PeerIdentity][]*PeerConn
connsByAddress  map[domain.PeerAddress]*PeerConn  // только исходящие
```

### Жизненный цикл writer-горутины

```
NewPeerConn(rawConn, direction)
    │
    ├── запускает writerGoroutine(sendCh, rawConn)
    │       └── for item := range sendCh { rawConn.Write(item.data) }
    │
    ├── Send(frame) → sendCh <- item  (неблокирующий)
    ├── SendSync(frame) → sendCh <- item{ack} → <-ack  (блокирующий)
    │
    └── Close()
            ├── close(sendCh)    → writer дренирует и завершается
            ├── <-writerDone     → ожидание дренажа
            └── rawConn.Close()  → TCP FIN
```

### Поток handshake

Во время handshake (обмен hello/welcome) `PeerConn` ещё не имеет
identity или capabilities. Код handshake использует `PeerConn.Send()`
для записи и читает из `bufio.Reader` на `PeerConn.rawConn` — но
только внутри единственной горутины, владеющей соединением в этот момент.

### Фазы миграции

**Шаг 1:** Создать `peer_conn.go` с типом `PeerConn`. Оба пути записи
(inbound `connWriter` и outbound `servePeerSession`) унифицируются
внутри `PeerConn.writerGoroutine`.

**Шаг 2:** Заменить map'ы `connSendCh` и `connWriterDone` на поля
`PeerConn`. Функции, вызывавшие `enqueueFrame(conn, data)`, теперь
вызывают `pc.Send(frame)`.

**Шаг 3:** Заменить `connPeerInfo` на `PeerConn.identity`, `.caps`,
`.networks`.

**Шаг 4:** Заменить `connAuth` на `PeerConn.auth`. Заменить
`inboundConns`, `inboundMetered`, `inboundTracked` запросами к
`Service.conns`.

**Шаг 5:** Заменить `peerSession` на `PeerConn` (outbound direction).
Map `sessions` становится `connsByAddress`.

**Шаг 6:** Удалить `net.Conn` из всех сигнатур функций. Заменить на
`*PeerConn`. Функция `writeJSONFrame(conn, frame)` становится
`pc.Send(frame)` — без fallback-пути, без прямых записей.

Каждый шаг — отдельный коммит. Тесты проходят после каждого шага.

### Устранение fallback-путей

Три небезопасных fallback-пути исчезают:

| До | После |
|----|-------|
| `writeFrameToInbound`: `enqueueUnregistered` → прямой `io.WriteString` | `pc.Send()` — всегда через канал; нет fallback |
| `writeJSONFrame`: `enqueueUnregistered` → прямой `conn.Write` | `pc.Send()` — унифицирован для обоих направлений |
| `writeJSONFrameSync`: `enqueueUnregistered` → прямой `conn.Write` | `pc.SendSync()` — блокируется на ack-канале |

Если канал записи полон или закрыт, `Send()` возвращает `false`.
Вызывающий код обрабатывает ошибку (log + закрытие соединения).
Никаких молчаливых откатов к незащищённым записям.

## Будущее: пакет NetCore (Фаза 2)

После стабилизации `PeerConn` весь тип и его writer-горутина переезжают
в `internal/core/netcore`. `Service` взаимодействует через интерфейс:

```go
type Network interface {
    Send(dst PeerID, frame Frame) bool
    SendWithCap(dst PeerID, frame Frame, cap Capability) bool
    Broadcast(frame Frame, filter func(PeerID) bool)
    PeersByCapability(cap Capability) []PeerID
    Disconnect(dst PeerID)
}
```

`net.Conn` невозможно импортировать за пределами `netcore`. Любая попытка
обойти очередь записи — ошибка компиляции, а не находка на code review.
