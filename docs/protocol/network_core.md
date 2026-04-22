# Network core (`internal/core/netcore`)

## English

### Purpose

`internal/core/netcore` is the transport core of the node. It owns every
raw `net.Conn` managed by the process, the writer goroutine that serialises
bytes onto the socket, the per-connection framing loop and the metered
wrapper that counts bytes in and out. Everything above the socket —
routing, auth, peer health, ban score, inbox dispatch, reachability — lives
on `node.Service` and is not part of this package.

The package replaced an earlier in-package `PeerConn` design where
`node.Service` kept nine parallel maps keyed by `net.Conn` and reached
straight into the socket from dozens of call-sites. Under the current
shape `net.Conn` is private to `netcore` and `node.Service` talks to the
transport only through a typed interface.

### Boundary: `netcore.Network`

`netcore.Network` is the narrow API surface the rest of the node uses to
move frames. It is ConnID-keyed, ctx-first and exposes only frame I/O,
enumeration, shutdown and a lightweight address accessor:

```go
type Network interface {
    SendFrame(ctx context.Context, id domain.ConnID, frame []byte) error
    SendFrameSync(ctx context.Context, id domain.ConnID, frame []byte) error
    Enumerate(ctx context.Context, dir Direction, fn func(domain.ConnID) bool)
    Close(ctx context.Context, id domain.ConnID) error
    RemoteAddr(id domain.ConnID) string
}
```

The interface deliberately does **not** expose accept / register /
unregister, does not expose `*netcore.NetCore` and does not expose
`net.Conn`. Auth, tracking, capabilities, peer health and any other
policy concern stay on `node.Service` — they are not the transport's job.

The production implementation is `networkBridge` in
`internal/core/node/network_bridge.go`: a thin adapter that resolves
`ConnID → *netcore.NetCore` under the registry lock, then delegates to
`NetCore.SendRaw` / `SendRawSync` / `Close` / `RemoteAddr`. The bridge
holds no state of its own and multiple calls to `Service.Network()` are
safe and cheap.

### Identity currency

The public key of `Network` is `domain.ConnID`. It is the transport-layer
identity of a single socket and is stable only for that socket's lifetime.
`domain.PeerIdentity` — the business / routing identity that survives
reconnects — does not appear in the transport interface; resolution
between `PeerIdentity` and `ConnID` is a concern of `node.Service`.

Inside `node.Service` the connection registry (`s.conns`) is keyed by
`ConnID`. A secondary index `s.connIDByNetConn` exists only so the
lifecycle carve-out (accept / unregister) can cross the `net.Conn` →
`ConnID` boundary exactly once per event; every other call site is
ConnID-first from the start.

### Read-side snapshot (`connInfo`)

Read walks over the registry do not hand callers a `*netcore.NetCore`
pointer. `forEachConnLocked` / `forEachInboundConnLocked` /
`forEachTrackedInboundConnLocked` call back with a value-typed `connInfo`
snapshot captured under `s.mu`:

```go
type connInfo struct {
    id           domain.ConnID
    remoteAddr   string
    address      domain.PeerAddress
    identity     domain.PeerIdentity
    capabilities []domain.Capability
    dir          netcore.Direction
    lastActivity time.Time
    tracked      bool
}
```

`capabilities` is a defensive copy; the caller cannot scribble back into
NetCore-owned storage. Writes to identity / address / auth keep their
handshake-time path through `coreForIDLocked`, which remains the single
carve-out that returns the live handle. The snapshot shape guarantees
that a walk callback cannot race with those writes.

### Single-writer invariant

Every `NetCore` owns its `net.Conn` privately. Frames reach the wire only
through one of two entry points:

- `NetCore.SendRaw(frame) SendStatus` — asynchronous; enqueues on the
  writer channel.
- `NetCore.SendRawSync(frame) SendStatus` — synchronous; enqueues and
  blocks on a per-frame ack until the writer flushes or the sync
  deadline elapses.

Both feed a single dedicated writer goroutine per connection. No other
code path writes to the socket. The goroutine owns `conn.Write` and is
the only place where partial-write / short-write can occur, so the
slow-peer eviction signal is well-defined: buffer saturation is the
writer's contention with the peer, not a race with an unrelated caller.

`SendStatus` is an internal enum of partial-failure outcomes (`SendOK`,
`SendBufferFull`, `SendWriterDone`, `SendTimeout`, `SendChanClosed`,
`SendMarshalError`, `SendCtxCancelled`, `SendStatusInvalid`). At the
bridge boundary `SendStatusToError` maps each value to the corresponding
exported sentinel (`ErrSendBufferFull`, `ErrSendWriterDone`,
`ErrSendTimeout`, `ErrSendChanClosed`, `ErrSendMarshalError`,
`ErrSendCtxCancelled`, `ErrSendInvalidStatus`) plus `ErrUnknownConn` for
the pre-flight registry miss. Callers discriminate via `errors.Is`,
never by string.

### Context honoured end-to-end on sync sends

`Network.SendFrameSync` takes a caller `ctx` and that `ctx` is observed
for the full lifetime of the call — including the flush-wait on the
writer. The bridge routes every sync send through
`NetCore.SendRawSyncCtx`, which returns `SendCtxCancelled` when the
caller cancels mid-flight and `SendTimeout` only when the internal
`syncFlushTimeout` (5 s) elapses with no cancellation.

This closes a prior gap where the non-ctx twin `SendRawSync` waited
solely on its internal 5 s deadline. Routing-layer cancellation — the
per-cycle context threaded through `fanoutAnnounceRoutes` and any
request-scoped timeout — now aborts the send wait instead of being
silently upgraded to `s.runCtx` at the routing-layer entry. The
defensive `ErrSendCtxCancelled` mapping exists for any future caller
that bypasses the bridge path.

### Lifecycle carve-out

`Network` is the working API for frames on already-registered
connections; it is not a factory for them. Accept, register and
unregister stay `net.Conn`-first inside `internal/core/node` because
the signature is dictated by structural role: a raw socket has no
`ConnID` until it is bound, and the `(net.Conn, ConnID)` binding is
the thing being created or torn down.

The frozen carve-out is exactly twelve functions:

- `internal/core/node/conn_registry.go` — `connIDForLocked`,
  `connIDFor`, `registerInboundConnLocked`, `attachOutboundCoreLocked`,
  `unregisterConnLocked`.
- `internal/core/node/service.go` — `handleConn` (inbound entry
  boundary), `registerInboundConn`, `unregisterInboundConn`,
  `isBlacklistedConn` (pre-registration IP policy), `ConnAuthState`
  and `SetConnAuthState` (pinned by the external `connauth.AuthStore`
  interface).
- `internal/core/node/peer_management.go` — `enableTCPKeepAlive`
  (operates on the raw socket by definition).

New `net.Conn`-first call sites outside that list are boundary
violations and must either migrate to ConnID-first or justify an
explicit extension of the carve-out at review.

### Test backend (`internal/core/netcore/netcoretest`)

Protocol-level tests do not open real TCP sockets. `netcoretest.Backend`
is an in-memory implementation of `netcore.Network` wired into `Service`
via `node.NewServiceWithNetwork(..., backend)`. It preserves the same
sentinel-error contract and the same per-ConnID ordering invariant as
the production bridge, and collapses the writer-goroutine model into a
buffered outbound channel (depth 128, matching the production
`sendChBuffer`). Tests observe what `Service` sends by draining
`backend.Outbound(id)` and drive inbound traffic with `backend.Inject`.

The naming convention mirrors `net/http/httptest`. The lifetime method
is `Backend.Shutdown()` rather than `Close()` because `netcore.Network`
already pins `Close(ctx, id)` for per-connection close and Go does not
allow two methods with the same name.

### Boundary enforcement: `make enforce-netcore-boundary`

The boundary is not aspirational. `scripts/enforce-netcore-boundary.sh`
is the canonical runner; `make enforce-netcore-boundary` is the CI
target. It runs fifteen grep-based gates plus a `net` stdlib import
whitelist against `internal/core/node` and asserts each against a
frozen baseline. Any drift — a new occurrence of a forbidden pattern,
a new `net.Conn`-accepting function beyond the frozen twelve, or a
new file in `internal/core/node` that imports `net` outside the
carve-out whitelist — exits non-zero.

The gates cover, in one line each:

1–4. Direct socket writes (`conn.Write` / `io.WriteString`) outside the
     transport owner, broken out per carve-out file so the expected
     baseline is exact.

5.   Raw `session.conn.Write` / `WriteTo` in `peer_management.go`.

6.   Parallel `map[net.Conn]*NetCore` registry.

7.   Primary registry regressed to `map[net.Conn]*connEntry`.

8.   Direct access to `s.conns` / `s.connIDByNetConn` outside
     `conn_registry.go`.

9.   Un-ack'd write-wrapper call-sites (`writeJSONFrame*ByID`,
     `enqueueFrame*ByID`, `sendFrameViaNetwork[Sync]`,
     `sendFrameBytesViaNetwork[Sync]`, `sendSessionFrameViaNetwork`).

10.  Untyped `uint64` ConnID identity in `node` / `domain`.

11.  Deleted `netCoreFor` / `meteredFor` / `isInboundTracked` public
     wrappers.

12.  `net` stdlib import whitelist in `internal/core/node` — the
     carve-out files plus `peer_provider.go` (peer-address policy)
     and `netgroup.go` (reachability grouping).

13.  Deleted `setTrackedLocked` mutation.

14.  Legacy walker signatures `forEach…ConnLocked(func(net.Conn, …))`
     and `(func(…*netcore.NetCore…))`.

15.  Legacy `inboundConnKey(*netcore.NetCore)` helper.

And the membership gate on the carve-out itself: exactly twelve
`net.Conn`-accepting functions / methods in `internal/core/node`
(eleven frozen `Service` methods plus `enableTCPKeepAlive`). Any growth
is a regression and fails the build.

The gate runs in CI on every push. Adding a new file or call-site that
requires loosening a gate is an explicit review decision, not a
drive-by edit.

---

## Русский

### Назначение

`internal/core/netcore` — сетевое ядро узла. Оно владеет каждым raw
`net.Conn`, writer-горутиной, которая сериализует байты на сокет,
циклом фреймирования для отдельной связи и metered-обёрткой, считающей
входящие и исходящие байты. Всё, что выше сокета — маршрутизация,
auth, peer health, ban score, inbox dispatch, reachability — живёт на
`node.Service` и к этому пакету отношения не имеет.

Пакет заменил более раннюю in-package схему `PeerConn`, где
`node.Service` держал девять параллельных `map`, ключёванных
`net.Conn`, и дёргал сокет из десятков call-sites напрямую. В текущей
форме `net.Conn` приватен внутри `netcore`, а `node.Service` общается
с транспортом только через типизированный интерфейс.

### Граница: `netcore.Network`

`netcore.Network` — узкая API-поверхность, через которую остальная
нода перемещает фреймы. ConnID-keyed, ctx-first, экспонирует только
frame I/O, enumeration, shutdown и лёгкий accessor адреса:

```go
type Network interface {
    SendFrame(ctx context.Context, id domain.ConnID, frame []byte) error
    SendFrameSync(ctx context.Context, id domain.ConnID, frame []byte) error
    Enumerate(ctx context.Context, dir Direction, fn func(domain.ConnID) bool)
    Close(ctx context.Context, id domain.ConnID) error
    RemoteAddr(id domain.ConnID) string
}
```

Интерфейс намеренно **не** выставляет accept / register / unregister,
не выставляет `*netcore.NetCore` и не выставляет `net.Conn`. Auth,
tracking, capabilities, peer health и прочие policy-заботы остаются
на `node.Service` — это не дело транспорта.

Production-реализация — `networkBridge` в
`internal/core/node/network_bridge.go`: тонкий adapter, который
резолвит `ConnID → *netcore.NetCore` под lock реестра и делегирует в
`NetCore.SendRaw` / `SendRawSync` / `Close` / `RemoteAddr`. Мост не
хранит собственного состояния, повторные вызовы `Service.Network()`
безопасны и дешёвы.

### Валюта идентичности

Публичным ключом `Network` является `domain.ConnID`. Это
transport-layer identity одного сокета, стабильная только на время
его жизни. `domain.PeerIdentity` — business / routing identity,
переживающая reconnects — в интерфейсе транспорта не появляется;
разрешение между `PeerIdentity` и `ConnID` — забота `node.Service`.

Внутри `node.Service` реестр соединений (`s.conns`) ключёван `ConnID`.
Secondary-индекс `s.connIDByNetConn` существует только для того,
чтобы lifecycle carve-out (accept / unregister) пересекал границу
`net.Conn → ConnID` ровно один раз на событие; все остальные
call-sites ConnID-first с самого начала.

### Read-side snapshot (`connInfo`)

Обход реестра на чтение не отдаёт caller'у указатель
`*netcore.NetCore`. `forEachConnLocked` /
`forEachInboundConnLocked` / `forEachTrackedInboundConnLocked`
вызывают callback с value-типизированным снимком `connInfo`,
снятым под `s.mu`:

```go
type connInfo struct {
    id           domain.ConnID
    remoteAddr   string
    address      domain.PeerAddress
    identity     domain.PeerIdentity
    capabilities []domain.Capability
    dir          netcore.Direction
    lastActivity time.Time
    tracked      bool
}
```

`capabilities` — защитная копия; caller не может писать обратно в
хранилище, принадлежащее NetCore. Запись identity / address / auth
остаётся на handshake-time пути через `coreForIDLocked`, который и
есть единственный carve-out, возвращающий живой handle. Форма снимка
гарантирует, что callback walker'а не гоняется с этими записями.

### Инвариант single-writer

Каждый `NetCore` владеет своим `net.Conn` приватно. Фреймы выходят на
провод только через одну из двух точек входа:

- `NetCore.SendRaw(frame) SendStatus` — асинхронно; энкью на writer
  канал.
- `NetCore.SendRawSync(frame) SendStatus` — синхронно; энкью и блок
  на per-frame ack, пока writer не сбросит или не сработает sync
  deadline.

Оба питают одну дедицированную writer-горутину на соединение. Никакой
другой code path в сокет не пишет. Горутина владеет `conn.Write` и
является единственным местом, где возможен partial-write /
short-write, так что сигнал slow-peer eviction чётко определён:
насыщение буфера — это contention writer'а с peer'ом, а не race со
сторонним caller'ом.

`SendStatus` — внутренний enum partial-failure исходов (`SendOK`,
`SendBufferFull`, `SendWriterDone`, `SendTimeout`, `SendChanClosed`,
`SendMarshalError`, `SendCtxCancelled`, `SendStatusInvalid`). На границе
bridge `SendStatusToError` мапит каждое значение в соответствующий
экспортированный sentinel (`ErrSendBufferFull`, `ErrSendWriterDone`,
`ErrSendTimeout`, `ErrSendChanClosed`, `ErrSendMarshalError`,
`ErrSendCtxCancelled`, `ErrSendInvalidStatus`) плюс `ErrUnknownConn`
для pre-flight miss реестра. Caller'ы дискриминируют через `errors.Is`,
никогда не по строке.

### Context соблюдается end-to-end на sync-отправках

`Network.SendFrameSync` принимает caller'ский `ctx`, и этот `ctx`
учитывается на всём протяжении вызова — включая flush-wait на writer'е.
Bridge маршрутизирует каждую sync-отправку через
`NetCore.SendRawSyncCtx`, который возвращает `SendCtxCancelled`, если
caller отменяет запрос mid-flight, и `SendTimeout` только тогда, когда
истекает внутренний `syncFlushTimeout` (5 с) без отмены.

Это закрывает прежний разрыв, где non-ctx-twin `SendRawSync` ждал
исключительно на своём внутреннем 5-секундном дедлайне. Отмена на
routing-слое — per-cycle контекст, пробрасываемый через
`fanoutAnnounceRoutes`, и любой request-scoped timeout — теперь
прерывает ожидание отправки, а не поднимается молча до `s.runCtx` на
входе в routing-слой. Defensive-маппинг `ErrSendCtxCancelled`
существует для любого будущего caller'а, который обойдёт bridge-путь.

### Lifecycle carve-out

`Network` — working API для фреймов уже зарегистрированных
соединений; это не factory для них. Accept, register и unregister
остаются `net.Conn`-first внутри `internal/core/node`, потому что
сигнатура диктуется структурной ролью: у raw-сокета нет `ConnID`,
пока он не привязан, и биндинг `(net.Conn, ConnID)` — это то самое,
что создаётся или разрушается.

Замороженный carve-out — ровно двенадцать функций:

- `internal/core/node/conn_registry.go` — `connIDForLocked`,
  `connIDFor`, `registerInboundConnLocked`,
  `attachOutboundCoreLocked`, `unregisterConnLocked`.
- `internal/core/node/service.go` — `handleConn` (entry boundary
  для inbound), `registerInboundConn`, `unregisterInboundConn`,
  `isBlacklistedConn` (pre-registration IP policy), `ConnAuthState`
  и `SetConnAuthState` (сигнатура пинится внешним интерфейсом
  `connauth.AuthStore`).
- `internal/core/node/peer_management.go` — `enableTCPKeepAlive`
  (по определению работает с raw-сокетом).

Новые `net.Conn`-first call-sites вне этого списка — нарушение
границы и должны либо мигрировать в ConnID-first, либо явно
обосновать расширение carve-out на ревью.

### Test backend (`internal/core/netcore/netcoretest`)

Protocol-level тесты не открывают реальных TCP-сокетов.
`netcoretest.Backend` — in-memory реализация `netcore.Network`,
которая втыкается в `Service` через
`node.NewServiceWithNetwork(..., backend)`. Она держит тот же
sentinel-error контракт и тот же инвариант per-ConnID ordering, что
и production bridge, и схлопывает модель writer-горутины в
buffered outbound канал (глубина 128 — совпадает с production
`sendChBuffer`). Тесты наблюдают, что посылает `Service`, дренируя
`backend.Outbound(id)`, и драйвят inbound трафик через
`backend.Inject`.

Naming convention зеркалит `net/http/httptest`. Lifetime-метод —
`Backend.Shutdown()`, а не `Close()`, потому что `netcore.Network`
уже пинит `Close(ctx, id)` за per-connection close, а Go не
разрешает два метода с одинаковым именем.

### Удержание границы: `make enforce-netcore-boundary`

Граница не декларативная. `scripts/enforce-netcore-boundary.sh` —
канонический runner; `make enforce-netcore-boundary` — CI-target.
Он прогоняет пятнадцать grep-based гейтов плюс whitelist импорта
stdlib `net` против `internal/core/node` и проверяет каждый против
замороженного baseline'а. Любой дрейф — новое вхождение запрещённого
паттерна, новая `net.Conn`-принимающая функция сверх двенадцати,
или новый файл в `internal/core/node`, импортирующий `net` вне
whitelist'а carve-out файлов — выходит non-zero.

Гейты покрывают, по строке на каждый:

1–4. Прямые socket writes (`conn.Write` / `io.WriteString`) вне
     транспортного владельца, разбитые по carve-out файлам так,
     чтобы ожидаемый baseline был точным.

5.   Raw `session.conn.Write` / `WriteTo` в `peer_management.go`.

6.   Параллельный `map[net.Conn]*NetCore` реестр.

7.   Primary реестр, регрессировавший к `map[net.Conn]*connEntry`.

8.   Прямой доступ к `s.conns` / `s.connIDByNetConn` вне
     `conn_registry.go`.

9.   Un-ack'd write-wrapper call-sites (`writeJSONFrame*ByID`,
     `enqueueFrame*ByID`, `sendFrameViaNetwork[Sync]`,
     `sendFrameBytesViaNetwork[Sync]`, `sendSessionFrameViaNetwork`).

10.  Untyped `uint64` ConnID identity в `node` / `domain`.

11.  Удалённые `netCoreFor` / `meteredFor` / `isInboundTracked`
     публичные wrapper'ы.

12.  Whitelist импорта `net` в `internal/core/node` — carve-out
     файлы плюс `peer_provider.go` (peer-address policy) и
     `netgroup.go` (reachability grouping).

13.  Удалённая мутация `setTrackedLocked`.

14.  Legacy walker-сигнатуры `forEach…ConnLocked(func(net.Conn, …))`
     и `(func(…*netcore.NetCore…))`.

15.  Legacy helper `inboundConnKey(*netcore.NetCore)`.

Плюс membership-gate на сам carve-out: ровно двенадцать
`net.Conn`-принимающих функций / методов в `internal/core/node`
(одиннадцать замороженных методов `Service` плюс
`enableTCPKeepAlive`). Любой рост — регрессия и валит сборку.

Гейт крутится в CI на каждом push. Добавление нового файла или
call-site, требующего ослабления гейта, — явное решение ревью, а
не мимоходом правка.
