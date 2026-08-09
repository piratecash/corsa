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

`capabilities` is a READ-ONLY alias of NetCore-owned storage (built via
`CapabilitiesRef`, no per-entry copy — the copy was a top alloc_space source on
the gossip fan-out hot path). Aliasing is safe because `domain.Capability` is an
immutable string and `pc.caps` is replace-only (never mutated in place), and
every snapshot consumer treats it read-only; consumers that must retain a
mutable copy (`connCapabilitiesForID`, the AnnounceTarget build) make their own.
Callers must not mutate the alias. Writes to identity / address / auth keep
their handshake-time path through `coreForIDLocked`, which remains the single
carve-out that returns the live handle. The snapshot shape guarantees that a
walk callback cannot race with those writes.

### Single-writer invariant

Every `NetCore` owns its `net.Conn` privately. Frames reach the wire only
through the entry points below (each has a tracked twin carrying a
per-frame outbound contract — see the next section):

- `NetCore.SendRaw(frame) SendStatus` — asynchronous; enqueues on the
  writer channel.
- `NetCore.SendRawSync(frame) SendStatus` — synchronous; enqueues and
  blocks on a per-frame ack until the writer flushes or the sync
  deadline elapses.

Both feed a single dedicated writer goroutine per connection. No other
code path writes to the socket. The goroutine owns `conn.Write` and is
the only place where partial-write / short-write can occur, so the
backpressure signal is well-defined: buffer saturation is the
writer's contention with the peer, not a race with an unrelated caller.
What the caller does with `SendBufferFull` is the caller's policy: the
fire-and-forget session path drops the frame and keeps the session
(best-effort contract — tearing the session down on saturation
converted transient backpressure into reconnect storms), while
genuinely dead sockets are detected by the per-write deadline in the
writer goroutine.

`SendStatus` is an internal enum of partial-failure outcomes (`SendOK`,
`SendBufferFull`, `SendWriterDone`, `SendTimeout`, `SendChanClosed`,
`SendMarshalError`, `SendCtxCancelled`, `SendStatusInvalid`). At the
bridge boundary `SendStatusToError` maps each value to the corresponding
exported sentinel (`ErrSendBufferFull`, `ErrSendWriterDone`,
`ErrSendTimeout`, `ErrSendChanClosed`, `ErrSendMarshalError`,
`ErrSendCtxCancelled`, `ErrSendInvalidStatus`) plus `ErrUnknownConn` for
the pre-flight registry miss. Callers discriminate via `errors.Is`,
never by string.

### Queue ownership: the writer queue is never closed

The writer queue has many producers on arbitrary goroutines and exactly
one consumer, and no producer can ever prove it is the last one. So the
queue is never closed as a Go channel: a `close` racing a producer's send
is a panic, not a status. The registry lookup that handed the caller its
`NetCore` is not a lease either — a background goroutine legitimately
keeps the connection across the whole teardown (`pushReceiptToSubscribers`
spawns `writePushFrame` per subscriber, and the shutdown path closes
connections without waiting for them).

`NetCore.Close` therefore does four things, in this order:

1. shut the send gate — from this instant every producer is answered
   `SendChanClosed`, including one that had already passed the door and
   is about to deposit its frame;
2. close the socket — this unblocks a writer parked in `conn.Write` on
   its deadline, so the wait in step 4 cannot inherit that deadline;
3. signal the writer's `closing` channel — the exit signal, deliberately
   a different channel from the queue;
4. wait for the writer to return, which happens only after it has
   released the whole queue residue.

The gate is monotonic, with one open and two shut states, and which shut
state a producer sees is part of the contract: `SendWriterDone` for a
socket that died under the writer, `SendChanClosed` for an orderly
teardown that owns the socket and must not be answered with another
`Close()`.

**The reason always comes from the gate, never from the channel that
fired.** A producer that is already past the door — waiting for a queue
slot, or waiting for its own frame to be flushed — wakes on `writerDone`,
and `writerDone` closes on BOTH death paths: the writer signals it after
a failed socket write and again on the exit `Close()` asked for. Deriving
the answer from it named a dead socket for every orderly shutdown, which
is precisely the answer that invites the caller to close a connection its
owner is already closing. The gate is raised before `writerDone` is
signalled on both paths, so it is always the more specific of the two;
the one state it cannot describe — writer gone, gate still open, i.e. the
writer goroutine panicked — is reported as `SendWriterDone`, which is
what a link that died with nobody owning the teardown is.

A frame a racing producer still deposits after step 3 is answered by the
gate and released together with the buffer when the `NetCore` is
collected — bounded by the queue depth and tied to the object's own
lifetime.

This is the rule the UPPER queue of the same path already follows
(`peerSession.sendCh`, fenced by `peerSession.sendMu` — see
`docs/locking.md`). Both queues state it the same way: the receiving side
never closes a channel it is not the only producer for.

### Tracked sends: the per-frame outbound contract

Alongside the two legacy entry points there is a metadata-carrying twin
pair, `NetCore.SendTracked(frame, *WriteTicket)` and
`NetCore.SendRawTracked(data, *WriteTicket)`. The ticket (see
`netcore.OutboundWrite`) carries exactly two optional things: a
`SendUntil` send deadline, and a `WriteGrace` bound on the socket write
of that one frame. A `nil` ticket is a fully inert ticket, so a frame
without a contract behaves exactly as it did before this surface
existed.

**The ticket travels one way.** It carries the frame's timing down to
the writer and brings nothing back: there is no observer, no callback
and no per-frame outcome value. The deadline is enforced by the writer
itself — `SendUntil` is re-checked immediately before the socket write,
not at enqueue time, because the frame may have waited in two queues
since — and an expired frame is simply not written. That is not a link
failure: the link is fine, this particular frame merely became worthless
while it waited, so the connection stays up and the writer goes on with
the frames behind it.

`WriteGrace` is the other half and is applied as the socket write
deadline for that one frame. A write that outlives its grace is the
opposite case: a frame cut in the middle desyncs the line protocol with
nothing left to resynchronise, so the socket is closed and the
connection is dead by definition.

The `SendStatus` return is therefore the whole answer a sender gets, and
it answers about ADMISSION rather than about the wire. `SendOK` means the
frame was accepted into a queue that was still live, and nothing more:
the writer may still drop it on its own `SendUntil`, or the link may
break before its turn comes.

A non-`SendOK` return is NOT uniformly "provably never written", and the
line runs between the two halves of the send path:

- **Refused at the door** — `SendMarshalError`, `SendBufferFull`, or the
  gate's `SendWriterDone` / `SendChanClosed` read BEFORE the offer. The
  frame never entered the queue, so it never reached the socket. This
  half is exact, and it is the half every retry and fallback policy is
  built on.
- **Refused after the offer** — the gate read once the frame is already
  in the queue. It proves only that the frame will not be written FROM
  THE QUEUE: the drain that follows the raised gate discards it. For an
  ASYNC frame there is no ack, so the one case the gate cannot place
  stays unresolved — this frame may have been written already and a
  LATER frame killed the link, and the status then reports a loss for
  bytes that left.
- **A sync wait that ended on something other than the ack** —
  `SendTimeout` or `SendCtxCancelled` from `awaitFlush`. The frame is
  still in the queue and a still-running writer may write it after the
  caller stopped waiting. The callers that must not leave it there
  (`enqueueFrameSyncByID`, `sendFrameViaNetworkSync`) close the
  connection on those two statuses, which is what makes the frame's fate
  final.

The one guarantee that IS exact in the other direction belongs to the
sync path: a closed ack proves the bytes left, the writer closes it after
the successful write and before it can raise the gate, and every wait
re-reads it without blocking before returning any failure. `select`
chooses uniformly among ready cases, so this second read — not the order
of the cases — is what keeps a flushed frame from being answered
`SendWriterDone`, `SendTimeout` or `SendCtxCancelled`.

Two consequences for the writer goroutine, both observable through
`SendStatus`. It no longer returns on the first failed write: it keeps
owning the queue in drain-only mode so that everything still buffered —
and anything a racing producer adds — is discarded in the same instant
the link breaks, instead of waiting for `Close()`. And from that instant
the queue is shut: every send entry point returns `SendWriterDone`
rather than `SendOK`, because the drain frees slots and "accepted" would
otherwise be reported for a frame the writer can only discard.

The gate is therefore read TWICE by every producer — before the offer and
again once the frame is already in the queue. One pre-offer read is not a
gate: a producer that read `gateOpen` a moment before the writer raised
the gate would still be answered `SendOK` for a frame landing in a queue
the drain has already walked past. The post-offer read closes that window rather than
shrinking it — atomics are sequentially consistent, so either the producer
sees the raised gate and answers its refusal, or the drain that follows
the raise starts after the item is already in the buffer and discards it.
The cost is one direction of imprecision, and it is the safe one: a frame written just
before a LATER frame killed the link may still be answered `SendWriterDone`
although its bytes left. Over-reporting a possible loss is the tolerable
error for a best-effort layer; under-reporting one is not.

`SendWriterDone` is therefore an ORDINARY outcome on a dying link, not a
programming error, and the fire-and-forget consumers in `node.Service`
(`enqueueFrameByID`, `enqueueSessionFrame`) treat it exactly like
`SendChanClosed`: the frame is dropped, the connection is NOT closed a
second time, and nothing is logged at error level. Handling it in the
"unexpected status" branch would print one ERROR line per inbound frame
for the whole teardown of a busy peer — a log storm precisely when the
log has to stay readable.

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
buffered outbound channel (depth 512, matching the production
`sendChBuffer`). Tests observe what `Service` sends by draining
`backend.Outbound(id)` and drive inbound traffic with `backend.Inject`.

The naming convention mirrors `net/http/httptest`. The lifetime method
is `Backend.Shutdown()` rather than `Close()` because `netcore.Network`
already pins `Close(ctx, id)` for per-connection close and Go does not
allow two methods with the same name.

Unlike production, the Backend DOES close its per-connection channels on
`Unregister` / `Close` / `Shutdown`, because tests read the close as "no
more frames". Production can afford the opposite rule — never close, let
the buffer die with the object — precisely because nothing there is
reading the queue as an observable.

Closing needs the same guarantee the production gate provides, that no
send is in flight when it happens, and the Backend gets it WITHOUT
holding the registry lock across the send. Each slot carries a `done`
channel and a counter of senders past the registry lookup, and teardown
runs in four ordered steps: drop the registration, raise `done`, join the
senders, close the channels. A sender therefore holds the registry lock
for the LOOKUP ONLY; its blocking offer selects on `done` and on its own
`ctx`.

That ordering is what makes "the Backend was shut down during the send" a
reachable outcome instead of a sentence in a doc comment. Holding the
read lock across the offer made it unreachable by construction: a sender
parked on a saturated channel blocked every writer of that mutex, and
`Shutdown`, `Unregister` and `Close` — the only three calls that could
release it — all need the mutex with write intent. `Inject` takes a `ctx`
for the same reason its outbound twin does: a test that saturates a
channel on purpose must be able to end its own wait.

The fence is read on BOTH SIDES of the offer by both send paths, blocking
and non-blocking alike. With room left in the channel a blocking offer
has two ready cases at once — the channel and `done` — and `select` picks
between them uniformly, so a slot torn down under the sender answered the
promised `ErrSendChanClosed` only about half the time; the non-blocking
twin had no `done` arm at all and answered `nil`. Neither is an answer
the Backend may give about a connection whose registration is already
gone, and Go cannot express precedence through the order of `select`
cases, so it is expressed by reads around the offer instead: one before
it, and one on the way out for the window the first cannot cover — the
fence raised after the sender has already read it down. A saturated
channel of a torn-down slot is answered the same way, because
`ErrSendBufferFull` invites a retry on a connection that will not be
there.

That answer is about the REGISTRATION and not about the frame. A frame
that reached the buffer stays in it: closing a Go channel does not
discard what is buffered, and teardown joins the senders before it closes
anything, so a reader draining `Outbound(id)` still receives it. The
Backend reports which connection the caller is now holding — gone — and
leaves the fate of the bytes to the reader, exactly as the production
gate's post-offer read reports about the queue rather than about the
socket.

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

`capabilities` — READ-ONLY алиас хранилища, принадлежащего NetCore
(строится через `CapabilitiesRef`, без per-entry копии — копия была
топ-источником alloc_space на горячем пути gossip fan-out). Алиасить
безопасно: `domain.Capability` — иммутабельная строка, а `pc.caps` только
заменяется целиком (никогда не мутируется in-place), и все потребители снимка
читают его read-only; те, кому нужна своя mutable-копия (`connCapabilitiesForID`,
сборка AnnounceTarget), делают её сами. Мутировать алиас нельзя. Запись
identity / address / auth остаётся на handshake-time пути через
`coreForIDLocked`, который и есть единственный carve-out, возвращающий живой
handle. Форма снимка гарантирует, что callback walker'а не гоняется с этими
записями.

### Инвариант single-writer

Каждый `NetCore` владеет своим `net.Conn` приватно. Фреймы выходят на
провод только через перечисленные ниже точки входа (у каждой есть
tracked-близнец, несущий контракт отправки на один кадр, — см. следующий
раздел):

- `NetCore.SendRaw(frame) SendStatus` — асинхронно; энкью на writer
  канал.
- `NetCore.SendRawSync(frame) SendStatus` — синхронно; энкью и блок
  на per-frame ack, пока writer не сбросит или не сработает sync
  deadline.

Оба питают одну дедицированную writer-горутину на соединение. Никакой
другой code path в сокет не пишет. Горутина владеет `conn.Write` и
является единственным местом, где возможен partial-write /
short-write, так что сигнал backpressure чётко определён:
насыщение буфера — это contention writer'а с peer'ом, а не race со
сторонним caller'ом. Что делать с `SendBufferFull` — policy caller'а:
fire-and-forget путь сессии дропает кадр и сохраняет сессию
(best-effort контракт — разрыв сессии по насыщению превращал
кратковременный backpressure в реконнект-штормы), а действительно
мёртвые сокеты детектируются per-write deadline'ом в writer-горутине.

`SendStatus` — внутренний enum partial-failure исходов (`SendOK`,
`SendBufferFull`, `SendWriterDone`, `SendTimeout`, `SendChanClosed`,
`SendMarshalError`, `SendCtxCancelled`, `SendStatusInvalid`). На границе
bridge `SendStatusToError` мапит каждое значение в соответствующий
экспортированный sentinel (`ErrSendBufferFull`, `ErrSendWriterDone`,
`ErrSendTimeout`, `ErrSendChanClosed`, `ErrSendMarshalError`,
`ErrSendCtxCancelled`, `ErrSendInvalidStatus`) плюс `ErrUnknownConn`
для pre-flight miss реестра. Caller'ы дискриминируют через `errors.Is`,
никогда не по строке.

### Владение очередью: очередь writer'а никогда не закрывается

У очереди writer'а много продюсеров на произвольных горутинах и ровно
один потребитель, и ни один продюсер не может доказать, что он
последний. Поэтому очередь никогда не закрывается как Go-канал: `close`,
гонящийся с отправкой продюсера, — это паника, а не статус. Поиск в
реестре, который выдал caller'у его `NetCore`, тоже не является арендой:
фоновая горутина законно удерживает соединение через весь teardown
(`pushReceiptToSubscribers` запускает `writePushFrame` на подписчика, а
путь остановки закрывает соединения, не дожидаясь их).

Поэтому `NetCore.Close` делает четыре вещи, именно в этом порядке:

1. закрывает gate отправки — с этого мига любой продюсер получает
   `SendChanClosed`, включая того, кто уже прошёл дверь и вот-вот
   положит свой кадр;
2. закрывает сокет — это разблокирует writer'а, застрявшего в
   `conn.Write` на своём дедлайне, чтобы ожидание из шага 4 не
   унаследовало этот дедлайн;
3. сигналит канал `closing` writer'а — сигнал выхода, намеренно
   отдельный канал, а не сама очередь;
4. ждёт возврата writer'а, который происходит только после того, как тот
   отпустил весь остаток очереди.

Gate монотонен, у него одно открытое и два закрытых состояния, и какое
именно закрытое состояние увидит продюсер — часть контракта:
`SendWriterDone` для сокета, умершего под writer'ом, и `SendChanClosed`
для упорядоченного teardown'а, который владеет сокетом и на который
нельзя отвечать ещё одним `Close()`.

**Причина всегда берётся из gate, а не из того, какой канал сработал.**
Продюсер, уже прошедший дверь, — ждущий слот в очереди или ждущий, пока
его кадр сбросят, — просыпается на `writerDone`, а `writerDone`
закрывается на ОБОИХ путях смерти: writer сигналит его и после неудачной
записи в сокет, и на выходе, о котором попросил `Close()`. Вывод причины
из него называл мёртвым сокетом любое штатное закрытие — ровно тот
ответ, который приглашает caller'а закрыть соединение, уже закрываемое
своим владельцем. Gate поднимается раньше сигнала `writerDone` на обоих
путях, поэтому он всегда конкретнее; единственное состояние, которое он
не описывает, — writer ушёл, а gate открыт, то есть горутина writer'а
паниковала, — отдаётся как `SendWriterDone`, чем и является линк,
умерший без владельца teardown'а.

Кадр, который гонящийся продюсер всё же положил после шага 3, получает
ответ от gate и освобождается вместе с буфером, когда собирается сам
`NetCore`, — он ограничен глубиной очереди и привязан к времени жизни
объекта.

Это то же правило, которому уже следует ВЕРХНЯЯ очередь того же пути
(`peerSession.sendCh` за забором `peerSession.sendMu` — см.
`docs/locking.md`). Обе очереди формулируют его одинаково: принимающая
сторона никогда не закрывает канал, единственным продюсером которого она
не является.

### Tracked-отправки: контракт отправки на один кадр

Рядом с двумя легаси-точками входа есть их близнецы, несущие
метаданные: `NetCore.SendTracked(frame, *WriteTicket)` и
`NetCore.SendRawTracked(data, *WriteTicket)`. Тикет (см.
`netcore.OutboundWrite`) несёт ровно две необязательные вещи: дедлайн
отправки `SendUntil` и ограничение `WriteGrace` на запись именно этого
кадра в сокет. `nil`-тикет полностью инертен, поэтому кадр без контракта
ведёт себя ровно так же, как до появления этой поверхности.

**Тикет едет в одну сторону.** Он доносит тайминг кадра до writer'а и
ничего не приносит обратно: ни наблюдателя, ни колбэка, ни
терминального исхода на кадр. Дедлайн держит сам writer — `SendUntil`
перепроверяется **непосредственно перед** записью в сокет, а не в момент
постановки, потому что кадр мог с тех пор простоять в двух очередях, — и
просроченный кадр просто не пишется. Это не отказ линка: линк в порядке,
а конкретно этот кадр обесценился, пока ждал, поэтому соединение
остаётся живым, и writer идёт дальше по кадрам за ним.

`WriteGrace` — вторая половина, и применяется он как дедлайн сокетной
записи этого одного кадра. Запись, пережившая свой грейс, — обратный
случай: кадр, разрезанный посередине, ломает line-протокол, и чинить его
нечем, поэтому сокет закрывается, а соединение мертво по определению.

Значит, весь ответ отправителю — это возврат `SendStatus`, и отвечает он
про ПРИЁМ В ОЧЕРЕДЬ, а не про провод. `SendOK` означает, что кадр принят
в очередь, которая на тот момент была жива, и не больше: writer ещё может
сбросить его по собственному `SendUntil`, а линк — отвалиться до того, как
дойдёт очередь.

Не-`SendOK` НЕ означает единообразно «кадр точно не записан», и граница
проходит между двумя половинами пути отправки:

- **Отказ на входе** — `SendMarshalError`, `SendBufferFull` или отказ
  gate (`SendWriterDone` / `SendChanClosed`), прочитанный ДО постановки
  в очередь. Кадр в очередь не попал, значит и до сокета не дошёл. Эта
  половина точна, и именно на ней построены все политики повтора и
  фолбэка.
- **Отказ после постановки** — gate, прочитанный, когда кадр уже лежит в
  очереди. Он доказывает лишь то, что кадр не будет записан ИЗ ОЧЕРЕДИ:
  дренаж, идущий за взводом gate, его выбросит. У АСИНХРОННОГО кадра нет
  ack, поэтому единственный случай, который gate не различает, остаётся
  нерешённым: этот кадр мог быть уже записан, а линк убил СЛЕДУЮЩИЙ кадр
  — и тогда статус сообщает о потере байтов, которые ушли.
- **Sync-ожидание, закончившееся не на ack** — `SendTimeout` или
  `SendCtxCancelled` из `awaitFlush`. Кадр всё ещё в очереди, и живой
  writer может записать его после того, как caller перестал ждать.
  Потребители, которым нельзя его там оставлять (`enqueueFrameSyncByID`,
  `sendFrameViaNetworkSync`), закрывают соединение на этих двух статусах
  — это и делает судьбу кадра окончательной.

Единственная гарантия, точная в обратную сторону, принадлежит sync-пути:
закрытый ack доказывает, что байты ушли — writer закрывает его после
успешной записи и раньше, чем успевает взвести gate, — и каждое ожидание
перечитывает его неблокирующе прежде, чем вернуть любую ошибку. `select`
выбирает равновероятно среди готовых веток, поэтому именно это второе
чтение, а не порядок case-ов, не даёт сброшенному кадру получить ответ
`SendWriterDone`, `SendTimeout` или `SendCtxCancelled`.

Два следствия для writer-горутины, оба наблюдаемы через `SendStatus`.
Она больше не выходит по первой неудачной записи: она продолжает
владеть очередью в режиме drain-only, чтобы всё ещё буферизованное — и
всё, что успеет добавить гонящийся продюсер, — было выброшено в тот же
миг, когда рвётся линк, а не по `Close()`. И с этого мига очередь
закрыта: каждая точка входа отправки возвращает `SendWriterDone` вместо
`SendOK`, потому что дренаж освобождает слоты и иначе «принято»
сообщалось бы про кадр, который writer может только выбросить.

Поэтому gate читается КАЖДЫМ продюсером ДВАЖДЫ — до постановки в очередь и
ещё раз, когда кадр в ней уже лежит. Одного чтения до постановки мало: продюсер,
прочитавший `gateOpen` за миг до того, как writer взвёл gate, всё равно получил
бы `SendOK` на кадр, попавший в очередь, мимо которой дренаж уже прошёл. Второе
чтение окно не сужает, а закрывает: атомики последовательно консистентны,
поэтому либо продюсер видит взведённый gate и отвечает его отказом, либо дренаж,
идущий следом за взводом, начинается уже после того, как элемент лёг в буфер, и
выбрасывает его. Цена — неточность ровно в одну сторону, и это безопасная
сторона: кадр, записанный прямо перед тем, как линк убил СЛЕДУЮЩИЙ кадр, может
получить `SendWriterDone`, хотя его байты ушли. Переоценить возможную потерю
для негарантированного слоя допустимо, недооценить — нет.

Поэтому `SendWriterDone` — обычный исход на умирающем линке, а не
programming error, и fire-and-forget-потребители в `node.Service`
(`enqueueFrameByID`, `enqueueSessionFrame`) обрабатывают его ровно как
`SendChanClosed`: кадр дропается, соединение повторно не закрывается,
ошибка в лог не пишется. Обработка в ветке «неожиданный статус» давала
бы по ERROR-строке на каждый входящий кадр на всё время разрыва с
активным пиром — лог-шторм ровно тогда, когда лог должен оставаться
читаемым.

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
buffered outbound канал (глубина 512 — совпадает с production
`sendChBuffer`). Тесты наблюдают, что посылает `Service`, дренируя
`backend.Outbound(id)`, и драйвят inbound трафик через
`backend.Inject`.

Naming convention зеркалит `net/http/httptest`. Lifetime-метод —
`Backend.Shutdown()`, а не `Close()`, потому что `netcore.Network`
уже пинит `Close(ctx, id)` за per-connection close, а Go не
разрешает два метода с одинаковым именем.

В отличие от продакшена, Backend ЗАКРЫВАЕТ свои per-connection каналы
на `Unregister` / `Close` / `Shutdown`, потому что тесты читают
закрытие как «кадров больше не будет». Продакшен может позволить себе
обратное правило — не закрывать вообще, буфер умирает вместе с
объектом — именно потому, что там никто не читает очередь как
наблюдаемое.

Закрытию нужна та же гарантия, что даёт продакшен-gate: в момент
закрытия ни одной отправки не идёт. Backend получает её БЕЗ удержания
замка реестра на время отправки. У каждого слота есть канал `done` и
счётчик отправителей, прошедших поиск в реестре, а teardown идёт
четырьмя упорядоченными шагами: снять регистрацию, поднять `done`,
дождаться отправителей, закрыть каналы. Отправитель держит замок
реестра ТОЛЬКО на поиск; его блокирующая отправка селектится на `done`
и на собственном `ctx`.

Этот порядок и делает исход «Backend закрыли во время отправки»
достижимым, а не строчкой в комментарии. Удержание read-замка на время
отправки делало его недостижимым по построению: отправитель, вставший
на переполненном канале, блокировал всех писателей этого мьютекса, а
`Shutdown`, `Unregister` и `Close` — единственные три вызова, способные
его отпустить, — берут мьютекс на запись. `Inject` принимает `ctx` по
той же причине, что и его исходящий близнец: тест, намеренно
насыщающий канал, обязан уметь закончить собственное ожидание.

Забор читается ПО ОБЕ СТОРОНЫ offer-а обоими путями отправки — и
блокирующим, и неблокирующим. Когда в канале есть место, у блокирующего
offer-а разом готовы две ветки — канал и `done`, — а `select` выбирает
между ними равновероятно, поэтому слот, снесённый под отправителем,
отвечал обещанным `ErrSendChanClosed` примерно в половине случаев; у
неблокирующего близнеца ветки `done` не было вовсе, и он отвечал `nil`.
Ни то, ни другое Backend не вправе отвечать про соединение, регистрация
которого уже снята, а выразить приоритет порядком case-ов в Go нельзя —
поэтому он выражен чтениями вокруг offer-а: одно до него и одно на
выходе, на то окно, которое первое закрыть не может, — забор поднят уже
после того, как отправитель прочитал его опущенным. Переполненный канал
снесённого слота отвечает так же: `ErrSendBufferFull` приглашает
повторить на соединении, которого уже не будет.

Этот ответ — про РЕГИСТРАЦИЮ, а не про кадр. Кадр, попавший в буфер, там
и остаётся: закрытие Go-канала не выбрасывает буферизованное, а teardown
дожидается отправителей прежде, чем что-либо закрыть, поэтому читатель
`Outbound(id)` его всё равно получит. Backend сообщает, каким соединением
владеет вызывающий, — уже никаким, — а судьбу байтов оставляет читателю,
ровно как post-offer чтение продакшен-gate сообщает про очередь, а не про
сокет.

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
