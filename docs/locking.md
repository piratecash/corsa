# Service Locking — Domain Mutex Split

## English

### Motivation

`node.Service` historically guarded every piece of in-memory state with a single `sync.RWMutex` (`s.mu`). That mutex covered at least seven distinct concerns — peer connections, message delivery queues, cryptographic knowledge, gossip topics, IP-level policy, file transfer, and aggregate status. Go's `sync.RWMutex` is writer-preferring: one queued writer blocks every new reader until every in-flight reader drains. Any write to any domain therefore serialised reads across every other domain.

Under reconnect storms this produced a reader-starvation failure class where `fetch_network_stats`, `fetch_peer_health`, `get_peers` and similar read-mostly RPCs stalled for 10–15 s while a burst of peer-management writers drained.

Phase 1 addressed the hot read paths by materialising atomic snapshots outside `s.mu`. Phase 2 attacks the root cause: the single mutex itself. The fields of `Service` decompose naturally into seven independent domains — a write to one domain must not block a reader in another.

### Domain mutex map

Every previously-`s.mu`-protected field moves to exactly one of the following mutexes. Each mutex is a `sync.RWMutex`; reader-dominated domains benefit from the RWMutex split, and we keep the shape uniform.

| Mutex | Fields |
|---|---|
| `peerMu` | `sessions`, `health`, `peerIDs`, `peerTypes`, `peerVersions`, `peerBuilds`, `inboundHealthRefs`, `identitySessions`, `identityRelaySessions`, `peers`, `conns`, `connIDByNetConn`, `connIDCounter`, `dialOrigin`, `persistedMeta` |
| `deliveryMu` | `pending`, `pendingKeys`, `orphaned`, `outbound`, `relayRetry`, `seenReceipts`, `receipts`, `upstream` |
| `knowledgeMu` | `known`, `boxKeys`, `pubKeys`, `boxSigs` |
| `gossipMu` | `topics`, `seen`, `subs`, `notices`, `events`, `lastExpiredCleanup` |
| `ipStateMu` | `bans`, `inboundByIP`, `observedAddrs`, `trustedSelfAdvertiseIP`, `observedIPHistoryByPeer`, `bannedIPSet`, `remoteBannedIPs` |
| `fileMu` | `fileStore`, `fileTransfer`, `fileRouter` |
| `statusMu` | `aggregateStatus`, `lastPublishedAggregateStatus`, `lastAggregateStatusPublishAt`, `versionPolicy`, `lastVersionPolicyPublishAt` |

Fields that remain outside this scheme:

- `listener` — written exactly once inside `Run` before `Accept`. Keeps its existing single writer discipline; readers observe it via `loadListener` if needed.
- `runCtx`, `done`, `connWg`, `backgroundWg`, `peerActivityNanos`, `trafficMu` / `lastTrafficSnap`, `*Snap` atomic pointers, `queuePersist` — already have their own synchronisation (ctx/chan, WaitGroup, sync.Map, `sync.Mutex`, `atomic.Pointer`). Not covered by the domain split.
- `lastPeerSave`, `lastPeerEvict`, `lastSync` — peer-lifetime timestamps; move with `peerMu`.
- `reachableGroups` — populated exactly once by `computeReachableGroups` during `New` and treated as immutable for the runtime lifetime of the `Service`. Concurrent reads of the unmutated map are safe without a lock, so no mutex is assigned. If a future change makes it runtime-mutable, it must gain a mutex (either by moving into `ipStateMu` with every reader taking the lock, or its own dedicated synchronisation) in the same commit that adds the writer.

### Lock ordering

All cross-domain sections must acquire locks in this exact order:

```
peerMu → deliveryMu → knowledgeMu → gossipMu → ipStateMu → fileMu → statusMu
```

This order is total: no code path may acquire a later mutex while holding none of the earlier ones, then later try to acquire an earlier one. A section that touches `peerMu` and `statusMu` acquires `peerMu` first, does its read/mutation, then `statusMu`. Never the reverse.

Violating the order is a deadlock bug. The order is enforced by convention today; a vet-style linter may be added later.

#### Why this order

The order places the most-contended domain first so that less-contended domains never hold their lock while waiting on a contended one. Measured hotness (write frequency) decreases roughly along the chain:

1. **peerMu** — every connect / disconnect / markPeerConnected / markPeerDisconnected mutates it; reconnect storms hit it hardest.
2. **deliveryMu** — every inbound / outbound message, every relay retry.
3. **knowledgeMu** — writes only during handshake and when a new identity binding is learned.
4. **gossipMu** — writes on topic publish / subscribe; reads on every `fetch_messages`.
5. **ipStateMu** — writes on ban events and observed-IP convergence; reads on every accept.
6. **fileMu** — writes only during active transfers; cold outside that.
7. **statusMu** — writes only when aggregate status actually changes.

### Cross-domain transaction patterns

Most Service methods touch a single domain. The sections below are **representative** patterns, not an exhaustive list: every cross-domain call site must follow the canonical order from §"Lock ordering" regardless of whether it appears here. The examples exist to teach the shape; absence from this list is not permission to reverse the order.

#### `markPeerConnected` / `markPeerDisconnected`
Touches `peerMu` (health / sessions / identity maps) and `statusMu` (recompute aggregate). Order: acquire `peerMu.Lock`, mutate, release; then acquire `statusMu.Lock` to refresh. The two sections do not overlap.

#### `onCMSessionEstablished`
Touches `peerMu` (sessions/health), `deliveryMu` (drain pending), `knowledgeMu` (record learned identity). Order follows the chain: `peerMu` → `deliveryMu` → `knowledgeMu`. Drains must not hold `peerMu` while waiting on `deliveryMu` unless acquired in that order.

#### `handleAuthSession`
Touches `peerMu` (bind identity to session) and `knowledgeMu` (cache pubkey). Order: `peerMu` → `knowledgeMu`.

#### `accumulateInboundTraffic`
Touches `peerMu` (update health byte counters) only. No cross-domain hold.

#### `handleConnectionNotice{code=peer-banned}`
Touches `ipStateMu` (record IP ban) and optionally `peerMu` (evict sessions from that IP). Order: `peerMu` → `ipStateMu`.

#### `flushPeerState` (peer state persistence snapshot)
Touches peer-domain (`persistedMeta`, `health`, `peerTypes`, `peerVersions`, `peerBuilds`, …) and `ipStateMu` (snapshots `bannedIPSet` and `remoteBannedIPs` for disk). Order: `peerMu.Lock` outer, `ipStateMu.RLock` nested inside. Both locks are released before disk I/O; afterwards `peerMu.Lock` is re-acquired briefly to update `lastPeerSave`. The two ipStateMu loops (local and remote bans) live inside the same RLock window so the persisted file represents a consistent snapshot of both tables.

Cross-lock constraint: `flushPeerState` MUST NOT call any `peerProvider.*` method from under `peerMu`. `PeerProvider.Candidates()` already runs under `pp.mu.RLock` and reaches back into Service via `RemoteBannedFn` / `BannedIPsFn` (edge `pp.mu → peerMu/ipStateMu`). Taking the reverse edge (`peerMu → pp.mu`) from inside `buildPeerEntriesLocked` would close the cycle and deadlock under Go's writer-preferring RWMutex in the three-goroutine interleaving: candidate refresher holds `pp.mu.RLock` and waits on `peerMu.RLock`; a concurrent `Promote`/`Add` queues on `pp.mu.Lock` blocking new pp.mu readers; `flushPeerState` holds `peerMu.Lock` and tries to enter `pp.mu.RLock` as a new reader. Snapshot `peerProvider.StaticSnapshotAll()` BEFORE taking `peerMu.Lock` and feed the snapshot into `buildPeerEntriesLocked`.

#### `recordOutboundAuthSuccess` (post-handshake ban recovery)
Touches peer-domain (`persistedMeta` via `clearRemoteBanLocked` and the peer-level bookkeeping in `repayMisadvertisePenaltyOnAuthLocked`) and `ipStateMu` (`remoteBannedIPs` via `clearRemoteIPBanLocked`, and `bans` via the inner section of `repayMisadvertisePenaltyOnAuthLocked`). Order: `peerMu.Lock` outer → `ipStateMu.Lock` nested. Two `ipStateMu` acquisitions happen inside the same outer `peerMu` region: one is taken internally by `repayMisadvertisePenaltyOnAuthLocked`, the other is an explicit `Lock/Unlock` around `clearRemoteIPBanLocked` because that helper requires the caller to already hold the ipStateMu write lock.

#### `storeIncomingMessage` (knowledge write + gossip dedup)
Touches `knowledgeMu` (write `known` for sender / recipient, read `pubKeys` for the non-DM spoof gate) and `gossipMu` (`seen` dedup check, `topics` append). Order: `knowledgeMu.Lock` FIRST, release, THEN `gossipMu.Lock` — canonical `knowledgeMu → gossipMu`. The two sections are SEQUENTIAL, not nested; splitting is semantically safe because the `known` writes are idempotent and the dedup decision reads only `seen`, not `known`, so a concurrent writer cannot produce a different outcome across the gap. The gossip section also temporarily releases `gossipMu` around the external `messageStore.StoreMessage` call (SQLite I/O must never happen under a domain mutex); the re-acquired window appends to `topics` and collects duplicate-diagnostic IDs before the final release.

The DM branch earlier in `storeIncomingMessage` reads `pubKeys`/`boxKeys`/`boxSigs` for signature verification under `knowledgeMu.RLock` as a standalone snapshot — no cross-domain hold, verification proceeds outside the lock.

Control DMs (`msg.Topic == protocol.TopicControlDM` — see `docs/dm-commands.md`) reuse this exact code path with three storage-side divergences:

1. The chatlog `messageStore.StoreMessage` call inside the gossip section is gated on `msg.Topic != protocol.TopicControlDM`.
2. The `s.topics[msg.Topic]` append is gated on `msg.Topic != protocol.TopicControlDM`. Control envelopes therefore never accumulate in `s.topics`. This avoids a memory leak — `retryableRelayMessages` and `queueStateSnapshotLocked` both read only `s.topics["dm"]`, so any control envelope put into `s.topics["dm-control"]` would be unread by every retry/snapshot consumer and grow unbounded. Routing/push fan-out remains unaffected because `executeGossipTargets` and `sendTableDirectedRelay` send frames on the wire on the fly without consulting `s.topics` as a backing store.
3. The LocalChange event branch publishes `LocalChangeNewControlMessage` on `ebus.TopicMessageControl` (instead of `LocalChangeNewMessage` on `ebus.TopicMessageNew`) and only does so when `msg.Recipient == s.identity.Address`. The sender side never receives its own outbound control DM as if it were inbound — sender-side state is updated synchronously inside `DMCrypto.SendControlMessage` / `DMRouter.SendMessageDelete` (the latter ships in slice B).

The locking discipline is identical to the data-DM path: `knowledgeMu` → `gossipMu` sequentially, with the same brief unlock around store I/O (now skipped entirely for control DMs, so the `gossipMu` section runs uninterrupted for them). No new mutex is taken and no new cross-domain edge is introduced; the only behavioural difference is which external side-effects run after the gossip section closes.

The relay-retry tracker (`trackRelayMessage` in `relay.go`) likewise rejects control DMs at its entry gate — the application-level retry on the sender side (`pendingDelete` in `DMRouter`, slice B) is the canonical retry mechanism for control DMs and the only one with a complete delivery contract (ack-driven termination, JSON persistence across restart).

#### `retryableRelayMessages` / `deleteBacklogMessageForRecipient` (delivery + gossip)
Touches `deliveryMu` (`relayRetry`) and `gossipMu` (`topics["dm"]`). Canonical order `deliveryMu → gossipMu`: `deliveryMu` OUTER, `gossipMu` INNER. In `retryableRelayMessages` the gossip side is a read-only snapshot (`gossipMu.RLock`) scoped tightly around the `topics["dm"]` copy; in `deleteBacklogMessageForRecipient` the gossip side is a write (`gossipMu.Lock`) that spans the entire filter loop because each matched envelope invalidates its `relayRetry` entry in the same iteration — the two mutations must stay atomic together.

#### `queueStateSnapshotLocked` (queue persistence snapshot)
Called with caller holding `deliveryMu.RLock` — snapshots `pending`, `relayRetry`, `receipts`, `outbound`, `orphaned`. Internally takes a brief `gossipMu.RLock` around the `topics["dm"]` iteration needed to filter the relay-queued envelopes. Order: `deliveryMu.RLock` OUTER (caller-held) → `gossipMu.RLock` INNER, canonical `deliveryMu → gossipMu`. The queue persister's `Snapshot` callback is the sole production caller and acquires `deliveryMu.RLock` itself; the `s.mu.RLock` previously used as a delivery placeholder was removed at step 5. Helpers follow the convention that `*Locked` disambiguates in its doc-comment which mutex the caller must hold; this one's doc-comment names `deliveryMu` and notes that the gossip slice is acquired internally.

#### `fetchInboxFrame` (gossip topics read + delivery receipt lookup)
Touches `gossipMu` (read `topics[topic]`) and `deliveryMu` (`receipts` via `hasReceiptForMessageLocked`). The two are expressed as SEQUENTIAL lock windows, not nested: take `gossipMu.RLock`, copy the topic slice, release; then for each DM envelope take a fresh `deliveryMu.RLock` just long enough to check receipt presence. Sequential splitting works because the topics slice is a local copy by the time the delivery read happens — there is no data-flow dependency that requires the gossip lock to remain held. It also keeps the canonical `deliveryMu → gossipMu` order from being violated: taking `deliveryMu.RLock` while holding `gossipMu.RLock` would be the reverse edge and is forbidden.

#### `queuePeerFrame` (peer→delivery cross-domain write)
Entry point for every outbound DM frame routed through the pending-queue backlog. Touches peer-domain (`s.resolveHealthAddress` reading `health`/`peerIDs`), `deliveryMu` (`pending`, `pendingKeys`, `outbound` via `noteOutboundQueuedLocked`), and status-domain (`s.aggregateStatus.PendingMessages` refreshed via `refreshAggregatePendingLocked`). Canonical order `peerMu → deliveryMu → statusMu`: `peerMu.Lock` OUTER → `deliveryMu.Lock` MIDDLE → `statusMu.Lock` INNERMOST around the aggregate-pending refresh and `aggregateStatus` snapshot read. All three locks observe the same delivery snapshot so that an enqueue, its dedup check against `pendingKeys`, and the aggregate pending-count refresh are atomically consistent. Every exit path — no-key, dup, per-peer overflow, global overflow, normal enqueue — releases the locks in LIFO order before notifying the persister via `queuePersist.MarkDirty` (the persister is decoupled from every domain mutex).

#### `flushPendingPeerFrames` / `flushPendingFireAndForget`
Drain the backlog for a peer once a session is available. Read peer-domain state (`resolveHealthAddress`, session lookup) and mutate `deliveryMu` (remove or requeue entries in `pending`/`pendingKeys`); refresh the aggregate status under `statusMu` when the backlog size changes. Canonical order `peerMu → deliveryMu → statusMu`: `peerMu.Lock` OUTER → `deliveryMu.Lock` MIDDLE → `statusMu.Lock` INNERMOST around the aggregate refresh. The requeue case (send failed, frames put back) re-takes the nested stack rather than holding the original window across network I/O — network writes must never happen under any domain mutex. The drain-empty fast path takes `peerMu.Lock` + `deliveryMu.RLock` + `statusMu.Lock` around `refreshAggregatePendingLocked` because the aggregate-pending recount reads delivery-domain fields and writes status-domain state.

#### `onCMSessionEstablished` / `onCMSessionTeardown` (peer lifecycle → delivery cleanup)
When `ConnectionManager` signals that a session became usable or is tearing down, the handler fixes peer-domain bookkeeping (health, identity maps) and then cancels or re-homes any pending delivery entries keyed to the outgoing address. Both handlers hold `peerMu.Lock` OUTER with a nested `deliveryMu.Lock` INNER, canonical `peerMu → deliveryMu`. The nested window covers both the `pending` lookup/removal and the `pendingKeys`/`outbound` bookkeeping so a concurrent `queuePeerFrame` cannot race a half-cleaned backlog.

#### `rebuildPeerHealthSnapshot` (hot-reads refresher for fetch_peer_health)
Builds the atomic snapshot consumed lock-free by the `fetch_peer_health` RPC. Reads peer-domain state (`health`, `peerIDs`, `peerVersions`, session map, capabilities) and delivery-domain state (`pending` for per-peer queued-count). Order: `peerMu.RLock` OUTER → `deliveryMu.RLock` INNER, canonical `peerMu → deliveryMu`. Both locks are released before the snapshot is `atomic.Pointer.Store`'d so the swap itself is lock-free. The cross-domain scope is intentional: the handler must render the per-peer pending count against the same peer roster it observed, otherwise a newly-arrived peer would appear with zero pending even when `queuePeerFrame` had already enqueued for it microseconds earlier.

#### `penalizeOldProtocolPeer` / `applySelfIdentityCooldown` (peer-delta emission callers)
Both call `emitPeerHealthDeltaLocked` after mutating peer-domain state. `emitPeerHealthDeltaLocked` requires the caller to hold BOTH `peerMu` and `deliveryMu` at least for read, because its body reaches into `publishAggregateStatusChangedLocked` → `refreshAggregatePendingLocked`, which counts `pending`/`orphaned`. Both callers wrap the emit with a short `deliveryMu.RLock`/`RUnlock` nested inside their existing `peerMu.Lock` region, preserving canonical `peerMu → deliveryMu` order. Version-policy mutations inside `penalizeOldProtocolPeer` (`recordIncompatibleObservationLocked`, `setVersionLockoutLocked`, `recomputeVersionPolicyLocked`) wrap `statusMu.Lock` INNERMOST around the full version-policy section since `versionPolicy` now lives under `statusMu`. The emit helper itself does not take `deliveryMu.RLock` — doing so would deadlock sites that already hold `deliveryMu.Lock` (e.g. `updatePeerStateLocked` → emit during a cleanup that mutates `pending`).

#### `updatePeerStateLocked` / `emitPeerHealthDeltaLocked` (aggregate-status recompute)
`updatePeerStateLocked` holds `peerMu.Lock` and, for the full duration of its body, `deliveryMu.RLock` via `defer`. The RLock covers the `s.pending` log line AND the downstream `publishAggregateStatusChangedLocked` → `refreshAggregatePendingLocked`/`computeAggregateStatusLocked` chain, so every delivery-domain read inside the same scope observes a consistent snapshot. The call to `publishAggregateStatusChangedLocked` is wrapped with a nested `statusMu.Lock`, canonical `peerMu → deliveryMu → statusMu`. `emitPeerHealthDeltaLocked`'s contract is documented as "caller holds both peerMu and deliveryMu at least for read"; it relies on the caller's scope rather than nesting its own RLock because several callers (`updatePeerStateLocked`, `onCMSessionTeardown`) already hold `deliveryMu.Lock` exclusively, and a self-nested RLock inside the helper would deadlock under Go's writer-preferring RWMutex whenever a writer queues between acquisitions.

#### Aggregate-status helpers (`computeAggregateStatusLocked`, `refreshAggregateStatusLocked`, `refreshAggregatePendingLocked`, `publishAggregateStatusChangedLocked`)
All four require the caller to hold `peerMu` AND `deliveryMu` at least for read (delivery domain — `pending`, `orphaned`; peer domain — `health` via `computeAggregateStatusLocked`). `computeAggregateStatusLocked` reads only peer-domain and delivery-domain fields and does NOT require `statusMu`. The other three (`refreshAggregateStatusLocked`, `refreshAggregatePendingLocked`, `publishAggregateStatusChangedLocked`) additionally require `statusMu.Lock` because they write `aggregateStatus`, `lastPublishedAggregateStatus`, or `lastAggregateStatusPublishAt`. Canonical order `peerMu → deliveryMu → statusMu`: OUTER → MIDDLE → INNERMOST. None of the helpers takes `deliveryMu.RLock` internally; a self-nested RLock is unsafe under writer-preferring semantics if a writer queues between acquisitions, and multiple callers already hold `deliveryMu.Lock` exclusively. The non-Locked entry point `refreshAggregateStatus` acquires `peerMu.Lock` then `deliveryMu.RLock` then `statusMu.Lock`, releases in LIFO order, and is the only place in the helper family where the full stack is acquired fresh.

Pure aggregate-status readers (`AggregateStatus`, `aggregateStatusFrame`, `VersionPolicySnapshot`) take ONLY `statusMu.RLock` — they read `aggregateStatus` / `versionPolicy` without touching peer or delivery state. This is the central Phase 2 win for Desktop-UI RPCs: a writer storm on `peerMu` no longer blocks a status read, because the reader holds a disjoint mutex.

### Reader path invariants

RPC hot paths that already read atomic snapshots (`fetch_network_stats`, `fetch_peer_health`, `get_peers`) remain lock-free; Phase 2 does not change them. What Phase 2 changes is the *refresher* goroutine's exposure to unrelated-domain contention.

Per-snapshot refresher lock footprint today:

- `networkStatsSnapshot` / `peerHealthSnapshot` — short `peerMu.RLock` (plus `deliveryMu.RLock` nested where per-peer queued-count is rendered). No IP-state callbacks, no `statusMu` hold.
- `cmSlotsSnapshot` — `cm.mu.RLock` only (separate mutex inside `ConnectionManager`, not covered by this split).
- `peersExchangeSnapshot` — more complex. `rebuildPeersExchangeSnapshot` takes a short `peerMu.RLock` for `persistedMeta` / `health`, releases it, then calls `peerProvider.Candidates()`. That Candidates call fires callbacks back into the Service: `BannedIPsFn` acquires `ipStateMu.RLock`, and `RemoteBannedFn` acquires `peerMu.RLock → ipStateMu.RLock` in the canonical order. A burst of IP-state writers (ban updates, observed-IP convergence) can therefore still delay this particular rebuild.

What Phase 2 eliminates is shared contention *across domains*: a `fileMu` or `statusMu` write no longer blocks any of the four refreshers. What Phase 2 does **not** eliminate is the intra-domain dependency above — debugging a `get_peers` staleness spike must still consider IP-state writer traffic, not just peer-domain traffic. That intra-domain coupling is intentional because `BannedIPsFn` / `RemoteBannedFn` are correctness gates on the candidate list; the trade-off is that the 500 ms staleness window on the snapshot absorbs those short blocks without reaching the RPC handler.

### Migration strategy

The split is implemented domain-by-domain, starting from the least-coupled domain. Each step is a self-contained commit:

1. `fileMu` — smallest surface, easiest to verify. **[done]**
2. `ipStateMu` — isolated from delivery paths. **[done]**
3. `knowledgeMu` — writes only during handshake. **[done]**
4. `gossipMu` — covers the `fetch_messages` reader path, cleanly bounded. **[done]**
5. `deliveryMu` — covers the relay / outbound queues. **[done]**
6. `peerMu` — largest domain; folded `statusMu` into the same commit. **[done]**
7. `statusMu` — folded in with `peerMu` (aggregate status is derived from peer + delivery state). **[done]**

Phase 2 is complete. The legacy `s.mu` field no longer exists on `Service`; every `*Locked` helper's doc-comment names exactly which domain mutex (or mutexes, in canonical order) the caller must hold. A reader must not assume any prior-era default; consult the helper's doc-comment.

### Testing contract

Every migration step must keep the existing node test suite green. Targeted regression tests from Phase 1 (reader-starvation reproducers in `*_starvation_test.go`) are re-run after every step to confirm the no-starvation property is preserved or improved.

---

## Russian

### Мотивация

`node.Service` исторически защищал всё in-memory состояние единым `sync.RWMutex` (`s.mu`). Этот мьютекс покрывал как минимум семь разных доменов — peer-подключения, очереди доставки сообщений, криптографическое «знание», gossip-топики, IP-политики, файловый трансфер и агрегированный статус. `sync.RWMutex` в Go отдаёт приоритет писателям: один поставленный в очередь writer блокирует всех новых readers до тех пор, пока не завершатся уже активные readers. Любая запись в любой домен сериализовала чтения во всех остальных.

Под штормом переподключений это давало класс багов reader-starvation: `fetch_network_stats`, `fetch_peer_health`, `get_peers` и другие read-mostly RPC стояли 10–15 с пока дренировалась очередь peer-management writers.

Фаза 1 решила проблему горячих read-путей через atomic snapshots вне `s.mu`. Фаза 2 атакует корень: сам мьютекс. Поля `Service` естественно раскладываются по семи независимым доменам — запись в один домен не должна блокировать чтения в другом.

### Карта доменных мьютексов

Каждое поле, ранее защищённое `s.mu`, переезжает ровно в один из мьютексов ниже. Все мьютексы — `sync.RWMutex`; read-dominated домены выигрывают от split RWMutex, и единообразная форма упрощает ревью.

Таблица полей — см. английскую секцию выше.

Вне схемы остаются:

- `listener` — пишется один раз в `Run` до `Accept`. Существующая дисциплина «один writer» сохраняется.
- `runCtx`, `done`, `connWg`, `backgroundWg`, `peerActivityNanos`, `trafficMu` / `lastTrafficSnap`, `*Snap` atomic pointers, `queuePersist` — уже имеют собственную синхронизацию (ctx/chan, WaitGroup, sync.Map, `sync.Mutex`, `atomic.Pointer`). Не входят в доменное разделение.
- `lastPeerSave`, `lastPeerEvict`, `lastSync` — peer-lifetime timestamps; едут с `peerMu`.
- `reachableGroups` — заполняется ровно один раз функцией `computeReachableGroups` при `New` и считается иммутабельным на весь runtime `Service`. Конкурентные чтения немутировавшейся карты безопасны без lock-а, поэтому мьютекс не назначается. Если в будущем появится runtime-запись, в том же коммите необходимо добавить синхронизацию (либо перевести поле под `ipStateMu` с захватом на каждом читателе, либо дать собственную синхронизацию) — иначе writer гонится со всеми нынешними читателями.

### Порядок захвата

Во всех кросс-доменных секциях locks должны захватываться в точно этом порядке:

```
peerMu → deliveryMu → knowledgeMu → gossipMu → ipStateMu → fileMu → statusMu
```

Порядок — тотальный: ни один путь кода не может захватить более поздний мьютекс не держа более раннего, и затем попытаться захватить более ранний. Секция, которая трогает `peerMu` и `statusMu`, сначала берёт `peerMu`, делает свою работу, отпускает; потом берёт `statusMu`. Никогда наоборот.

Нарушение порядка — deadlock-баг. Порядок сейчас держится соглашением; позже возможно добавить vet-линтер.

#### Почему именно такой порядок

Самый конкурируемый домен стоит первым, чтобы менее конкурируемые никогда не держали свой lock ожидая более конкурируемого. Измеряемая «горячесть» (частота записи) убывает примерно по цепочке:

1. **peerMu** — каждое connect/disconnect/mark* пишет сюда; шторма переподключений бьют сильнее всего.
2. **deliveryMu** — каждое входящее/исходящее сообщение, каждый relay-retry.
3. **knowledgeMu** — пишет только при handshake и при обучении нового identity binding.
4. **gossipMu** — пишет на publish/subscribe; читает каждый `fetch_messages`.
5. **ipStateMu** — пишет на ban events и observed-IP convergence; читает каждый accept.
6. **fileMu** — пишет только при активных трансферах; холодный вне этого.
7. **statusMu** — пишет только когда агрегированный статус реально меняется.

### Паттерны кросс-доменных транзакций

Большинство методов Service трогают один домен. Секции ниже — **репрезентативные**, а не исчерпывающие: любой кросс-доменный call site обязан соблюдать канонический порядок из §«Порядок захвата» независимо от того, попал он в список или нет. Примеры нужны чтобы показать форму; отсутствие пути ниже — не разрешение обратного порядка.

#### `markPeerConnected` / `markPeerDisconnected`
Трогает `peerMu` (health/sessions/identity) и `statusMu` (пересчёт агрегата). Порядок: взять `peerMu.Lock`, мутировать, отпустить; затем взять `statusMu.Lock` для refresh. Секции не перекрываются.

#### `onCMSessionEstablished`
Трогает `peerMu` (sessions/health), `deliveryMu` (drain pending), `knowledgeMu` (запомнить identity). Порядок по цепочке: `peerMu` → `deliveryMu` → `knowledgeMu`. Drains не должны держать `peerMu` пока ждут `deliveryMu` кроме как в этом порядке.

#### `handleAuthSession`
Трогает `peerMu` (привязка identity к сессии) и `knowledgeMu` (кеш pubkey). Порядок: `peerMu` → `knowledgeMu`.

#### `accumulateInboundTraffic`
Трогает только `peerMu` (обновление байтовых счётчиков health). Кросс-доменного hold'а нет.

#### `handleConnectionNotice{code=peer-banned}`
Трогает `ipStateMu` (записать IP-бан) и опционально `peerMu` (выкинуть сессии с этого IP). Порядок: `peerMu` → `ipStateMu`.

#### Жизненный цикл файл-трансфера
`fileMu` редко взаимодействует с другими доменами. Когда взаимодействует (отказ трансфера из-за ухода пира), порядок — `peerMu` → `fileMu`.

#### `flushPeerState` (снапшот peer state для персистентности)
Трогает peer-домен (`persistedMeta`, `health`, `peerTypes`, `peerVersions`, `peerBuilds`, …) и `ipStateMu` (снапшотит `bannedIPSet` и `remoteBannedIPs` для диска). Порядок: `peerMu.Lock` снаружи, `ipStateMu.RLock` внутри. Оба lock'а освобождаются до дискового I/O; после записи `peerMu.Lock` берётся повторно кратко, только для обновления `lastPeerSave`. Оба цикла под `ipStateMu.RLock` (локальные и удалённые баны) живут в одном окне RLock'а, чтобы файл на диске представлял согласованный снапшот обеих таблиц.

Кросс-lock-ограничение: `flushPeerState` НЕ ДОЛЖЕН вызывать любые методы `peerProvider.*` изнутри `peerMu`. `PeerProvider.Candidates()` уже выполняется под `pp.mu.RLock` и возвращается в Service через `RemoteBannedFn` / `BannedIPsFn` (ребро `pp.mu → peerMu/ipStateMu`). Обратное ребро (`peerMu → pp.mu`) изнутри `buildPeerEntriesLocked` замкнёт цикл и под writer-preferring `sync.RWMutex` Go приведёт к deadlock'у в тройной гонке: candidate refresher держит `pp.mu.RLock` и ждёт `peerMu.RLock`; параллельный `Promote`/`Add` встаёт в очередь на `pp.mu.Lock`, блокируя новых читателей pp.mu; `flushPeerState` держит `peerMu.Lock` и пытается стать новым читателем `pp.mu.RLock`. Снапшоть `peerProvider.StaticSnapshotAll()` ДО взятия `peerMu.Lock` и прокидывай снапшот параметром в `buildPeerEntriesLocked`.

#### `recordOutboundAuthSuccess` (восстановление после handshake)
Трогает peer-домен (`persistedMeta` через `clearRemoteBanLocked` и peer-level бухгалтерию в `repayMisadvertisePenaltyOnAuthLocked`) и `ipStateMu` (`remoteBannedIPs` через `clearRemoteIPBanLocked`, и `bans` через внутреннюю секцию `repayMisadvertisePenaltyOnAuthLocked`). Порядок: `peerMu.Lock` снаружи → `ipStateMu.Lock` внутри. Внутри одного внешнего `peerMu`-региона происходит две acquisition `ipStateMu`: первое берёт `repayMisadvertisePenaltyOnAuthLocked` само, второе — явная пара `Lock/Unlock` вокруг `clearRemoteIPBanLocked`, потому что этот helper требует от вызывающего уже держать write-lock `ipStateMu`.

#### `storeIncomingMessage` (knowledge-запись + gossip-dedup)
Трогает `knowledgeMu` (запись `known` по sender / recipient, чтение `pubKeys` для non-DM spoof-gate) и `gossipMu` (`seen`-dedup, `topics`-append). Порядок: СНАЧАЛА `knowledgeMu.Lock`, отпустить, ПОТОМ `gossipMu.Lock` — канонический `knowledgeMu → gossipMu`. Секции ПОСЛЕДОВАТЕЛЬНЫЕ, не вложенные; расщепление семантически безопасно: записи в `known` идемпотентны, а решение dedup читает только `seen`, не `known`, поэтому конкурентный writer не может получить другой итог в щели между секциями. В gossip-секции дополнительно происходит временный release/reacquire `gossipMu` вокруг внешнего вызова `messageStore.StoreMessage` (SQLite I/O не должно выполняться под доменным lock'ом); после возврата окно заново захватывается для append'а в `topics` и сбора диагностических ID до финального release.

DM-ветка раньше в `storeIncomingMessage` читает `pubKeys`/`boxKeys`/`boxSigs` для верификации подписи под `knowledgeMu.RLock` как standalone-снапшот — никакого кросс-доменного hold'а, верификация идёт уже вне lock'а.

#### `retryableRelayMessages` / `deleteBacklogMessageForRecipient` (delivery + gossip)
Трогают `deliveryMu` (`relayRetry`) и `gossipMu` (`topics["dm"]`). Канонический порядок `deliveryMu → gossipMu`: `deliveryMu` СНАРУЖИ, `gossipMu` ВНУТРИ. В `retryableRelayMessages` gossip-часть — это read-only snapshot (`gossipMu.RLock`), сжатый до копии `topics["dm"]`; в `deleteBacklogMessageForRecipient` gossip-часть — write (`gossipMu.Lock`) через весь цикл фильтрации, потому что каждое совпадение отменяет соответствующий `relayRetry` в той же итерации — обе мутации должны оставаться атомарно согласованными.

#### `queueStateSnapshotLocked` (снапшот очереди для персистентности)
Вызывается из caller'а, держащего `deliveryMu.RLock` — снапшотит `pending`, `relayRetry`, `receipts`, `outbound`, `orphaned`. Внутри сам берёт короткий `gossipMu.RLock` вокруг итерации по `topics["dm"]`, нужной чтобы отфильтровать relay-queued конверты. Порядок: `deliveryMu.RLock` СНАРУЖИ (держит caller) → `gossipMu.RLock` ВНУТРИ, канонический `deliveryMu → gossipMu`. Единственный production-caller — `Snapshot`-callback в `queueStatePersister`, он сам берёт `deliveryMu.RLock`; placeholder `s.mu.RLock`, который стоял тут до шага 5, удалён. Helpers следуют соглашению, что `*Locked` в doc-comment явно называет требуемый caller'у mutex; у этого — `deliveryMu`, плюс отметка что gossip-срез захватывается внутри.

#### `fetchInboxFrame` (чтение gossip-topics + lookup receipt'а из delivery)
Трогает `gossipMu` (чтение `topics[topic]`) и `deliveryMu` (`receipts` через `hasReceiptForMessageLocked`). Оформлено как ПОСЛЕДОВАТЕЛЬНЫЕ окна lock'ов, а не вложенно: сначала `gossipMu.RLock`, скопировать срез топика, release; затем для каждого DM-envelope отдельно короткий `deliveryMu.RLock` ровно под проверку receipt'а. Последовательное расщепление работает потому, что срез топиков уже локальная копия к моменту delivery-чтения — нет data-flow зависимости, требующей держать gossip-lock дальше. Это же сохраняет канонический порядок `deliveryMu → gossipMu`: захват `deliveryMu.RLock` при уже удерживаемом `gossipMu.RLock` был бы обратным ребром и запрещён.

#### `queuePeerFrame` (peer→delivery кросс-доменная запись)
Точка входа для каждого исходящего DM-frame, маршрутизируемого через backlog pending-очереди. Трогает peer-домен (`s.resolveHealthAddress` читает `health`/`peerIDs`), `deliveryMu` (`pending`, `pendingKeys`, `outbound` через `noteOutboundQueuedLocked`) и status-домен (`s.aggregateStatus.PendingMessages` через `refreshAggregatePendingLocked`). Канонический порядок `peerMu → deliveryMu → statusMu`: `peerMu.Lock` СНАРУЖИ → `deliveryMu.Lock` В СЕРЕДИНЕ → `statusMu.Lock` В САМОЙ СЕРДЦЕВИНЕ вокруг пересчёта агрегатного pending и чтения снапшота `aggregateStatus`. Все три lock'а видят один и тот же delivery-снапшот, поэтому enqueue, dedup-проверка по `pendingKeys` и пересчёт агрегатного pending атомарно согласованы. Все exit-пути — no-key, dup, per-peer overflow, global overflow, normal enqueue — освобождают lock'и в LIFO-порядке до уведомления персистера через `queuePersist.MarkDirty` (персистер развязан от всех доменных мьютексов).

#### `flushPendingPeerFrames` / `flushPendingFireAndForget`
Дренируют backlog пира, когда сессия стала доступна. Читают peer-домен (`resolveHealthAddress`, поиск сессии) и мутируют `deliveryMu` (удаление или requeue элементов в `pending`/`pendingKeys`); рефрешат агрегатный статус под `statusMu` при изменении размера backlog'а. Канонический порядок `peerMu → deliveryMu → statusMu`: `peerMu.Lock` СНАРУЖИ → `deliveryMu.Lock` В СЕРЕДИНЕ → `statusMu.Lock` В САМОЙ СЕРДЦЕВИНЕ вокруг refresh'а. Requeue-кейс (отправка провалилась, кадры возвращаются) берёт вложенный stack заново, а не держит исходное окно через network I/O — сетевые записи не должны происходить ни под одним доменным мьютексом. Быстрый путь drain-empty берёт `peerMu.Lock` + `deliveryMu.RLock` + `statusMu.Lock` вокруг `refreshAggregatePendingLocked`, потому что пересчёт агрегатного pending читает delivery-доменные поля и пишет status-доменное состояние.

#### `onCMSessionEstablished` / `onCMSessionTeardown` (peer lifecycle → delivery cleanup)
Когда `ConnectionManager` сигналит, что сессия стала рабочей или разбирается, хендлер поправляет peer-домен (health, identity maps), затем отменяет или перевешивает pending-записи по исходящему адресу. Оба хендлера держат `peerMu.Lock` СНАРУЖИ с вложенным `deliveryMu.Lock` ВНУТРИ, канонический `peerMu → deliveryMu`. Вложенное окно покрывает одновременно lookup/удаление в `pending` и бухгалтерию `pendingKeys`/`outbound`, чтобы параллельный `queuePeerFrame` не мог пересечься с полу-очищенным backlog'ом.

#### `rebuildPeerHealthSnapshot` (hot-reads refresher для fetch_peer_health)
Собирает atomic-снапшот, lock-free потребляемый RPC `fetch_peer_health`. Читает peer-домен (`health`, `peerIDs`, `peerVersions`, session map, capabilities) и delivery-домен (`pending` для per-peer queued-count). Порядок: `peerMu.RLock` СНАРУЖИ → `deliveryMu.RLock` ВНУТРИ, канонический `peerMu → deliveryMu`. Оба lock'а освобождаются до `atomic.Pointer.Store`, поэтому сам swap — lock-free. Кросс-доменный scope осмыслен: хендлер обязан рендерить per-peer pending-счётчик против того же roster'а пиров, который наблюдал, иначе только что появившийся пир был бы показан с нулём pending, хотя `queuePeerFrame` уже успел туда положить несколько микросекунд назад.

#### `penalizeOldProtocolPeer` / `applySelfIdentityCooldown` (вызыватели peer-delta emit)
Оба дёргают `emitPeerHealthDeltaLocked` после мутации peer-домена. Контракт `emitPeerHealthDeltaLocked` требует от вызывающего держать ОБА `peerMu` и `deliveryMu` как минимум на чтение, потому что его тело спускается в `publishAggregateStatusChangedLocked` → `refreshAggregatePendingLocked`, которая считает `pending`/`orphaned`. Оба вызывателя оборачивают вызов коротким `deliveryMu.RLock`/`RUnlock`, вложенным в уже удерживаемое `peerMu.Lock`-окно — канонический порядок `peerMu → deliveryMu` сохраняется. Мутации version-policy внутри `penalizeOldProtocolPeer` (`recordIncompatibleObservationLocked`, `setVersionLockoutLocked`, `recomputeVersionPolicyLocked`) оборачивают всю version-policy-секцию `statusMu.Lock` В САМОЙ СЕРДЦЕВИНЕ, потому что `versionPolicy` теперь живёт под `statusMu`. Сам helper `emitPeerHealthDeltaLocked` внутри `deliveryMu.RLock` не берёт — self-nested RLock deadlock'ится с сайтами, которые уже держат `deliveryMu.Lock` (например `updatePeerStateLocked` → emit во время cleanup'а, мутирующего `pending`).

#### `updatePeerStateLocked` / `emitPeerHealthDeltaLocked` (пересчёт агрегатного статуса)
`updatePeerStateLocked` держит `peerMu.Lock` и, на всю длительность тела функции, `deliveryMu.RLock` через `defer`. RLock покрывает и log-строку с `s.pending`, и нижележащую цепочку `publishAggregateStatusChangedLocked` → `refreshAggregatePendingLocked`/`computeAggregateStatusLocked`, поэтому все delivery-чтения внутри scope наблюдают согласованный снапшот. Вызов `publishAggregateStatusChangedLocked` обёрнут вложенным `statusMu.Lock`, канонический `peerMu → deliveryMu → statusMu`. Контракт `emitPeerHealthDeltaLocked` задокументирован как «caller держит и peerMu, и deliveryMu как минимум на чтение»; helper полагается на scope вызывающего, а не вкладывает RLock сам, потому что несколько вызывателей (`updatePeerStateLocked`, `onCMSessionTeardown`) уже держат `deliveryMu.Lock` эксклюзивно, а self-nested RLock под writer-preferring `sync.RWMutex` Go deadlock'ится всякий раз, когда writer встал между двумя acquisition'ами.

#### Хелперы агрегатного статуса (`computeAggregateStatusLocked`, `refreshAggregateStatusLocked`, `refreshAggregatePendingLocked`, `publishAggregateStatusChangedLocked`)
Все четыре требуют от caller'а держать `peerMu` И `deliveryMu` как минимум на чтение (delivery-домен — `pending`, `orphaned`; peer-домен — `health` через `computeAggregateStatusLocked`). `computeAggregateStatusLocked` читает только peer- и delivery-доменные поля и НЕ требует `statusMu`. Остальные три (`refreshAggregateStatusLocked`, `refreshAggregatePendingLocked`, `publishAggregateStatusChangedLocked`) дополнительно требуют `statusMu.Lock`, потому что пишут `aggregateStatus`, `lastPublishedAggregateStatus` или `lastAggregateStatusPublishAt`. Канонический порядок `peerMu → deliveryMu → statusMu`: СНАРУЖИ → В СЕРЕДИНЕ → В САМОЙ СЕРДЦЕВИНЕ. Ни один из хелперов не берёт `deliveryMu.RLock` сам; self-nested RLock небезопасен под writer-preferring семантикой если writer встал между acquisition'ами, и часть вызывателей уже держит `deliveryMu.Lock` эксклюзивно. Не-Locked точка входа `refreshAggregateStatus` захватывает `peerMu.Lock`, затем `deliveryMu.RLock`, затем `statusMu.Lock`, отпускает LIFO — это единственное место в семействе хелперов, где весь stack берётся заново изнутри.

Чистые читатели агрегатного статуса (`AggregateStatus`, `aggregateStatusFrame`, `VersionPolicySnapshot`) берут ТОЛЬКО `statusMu.RLock` — они читают `aggregateStatus` / `versionPolicy`, не трогая peer- или delivery-состояние. Это центральный выигрыш Фазы 2 для Desktop-UI RPC: писательский шторм на `peerMu` больше не блокирует чтение статуса, потому что читатель держит непересекающийся мьютекс.

### Инварианты read-path

Горячие RPC-пути, которые уже читают atomic snapshots (`fetch_network_stats`, `fetch_peer_health`, `get_peers`) остаются lock-free; Фаза 2 их не трогает. Что меняет Фаза 2 — это экспозицию *refresher*-горутины к контеншну несвязанных доменов.

Текущий lock-footprint по каждому snapshot-рефрешеру:

- `networkStatsSnapshot` / `peerHealthSnapshot` — короткий `peerMu.RLock` (плюс вложенный `deliveryMu.RLock` там, где считается per-peer queued-count). Callback'ов в ipState нет, `statusMu` не удерживается.
- `cmSlotsSnapshot` — только `cm.mu.RLock` (отдельный мьютекс внутри `ConnectionManager`, доменным разделением не покрывается).
- `peersExchangeSnapshot` — сложнее. `rebuildPeersExchangeSnapshot` берёт короткий `peerMu.RLock` для `persistedMeta` / `health`, отпускает его, затем вызывает `peerProvider.Candidates()`. Этот Candidates запускает callback'и обратно в Service: `BannedIPsFn` захватывает `ipStateMu.RLock`, а `RemoteBannedFn` — `peerMu.RLock → ipStateMu.RLock` в каноническом порядке. Всплеск писателей IP-state (ban-обновления, observed-IP convergence) поэтому всё ещё может задержать именно этот rebuild.

Что Фаза 2 убирает — это общий контеншн *между доменами*: запись в `fileMu` или `statusMu` больше не блокирует ни один из четырёх refresher'ов. Что Фаза 2 **не** убирает — это внутридоменная связь выше: при разборе staleness-спайка у `get_peers` нужно смотреть и на трафик писателей IP-state, а не только на peer-домен. Эта внутридоменная зависимость — сознательный компромисс: `BannedIPsFn` / `RemoteBannedFn` — это correctness-гейты списка кандидатов; плата за корректность — короткие блоки на IP-state, которые окно staleness 500 мс у snapshot'а поглощает до того, как они доходят до RPC-хендлера.

### Стратегия миграции

Разделение катится домен-за-доменом, от самого изолированного. Каждый шаг — самодостаточный commit:

1. `fileMu` — наименьшая поверхность, проще всего верифицировать. **[готово]**
2. `ipStateMu` — изолирован от delivery-путей. **[готово]**
3. `knowledgeMu` — пишет только при handshake. **[готово]**
4. `gossipMu` — покрывает reader-путь `fetch_messages`, чисто ограниченный. **[готово]**
5. `deliveryMu` — relay/outbound очереди. **[готово]**
6. `peerMu` — крупнейший домен; `statusMu` свёрнут в тот же коммит. **[готово]**
7. `statusMu` — свёрнут вместе с `peerMu` (агрегированный статус — производная от peer + delivery state). **[готово]**

Фаза 2 завершена. Поле `s.mu` на `Service` больше не существует; doc-comment каждого `*Locked`-хелпера явно называет, какой именно доменный мьютекс (или мьютексы, в каноническом порядке) требуется вызывающему. Не предполагать никаких значений по умолчанию из прошлого — смотреть doc-comment хелпера.

### Контракт тестов

Каждый шаг миграции должен оставлять существующий node test suite зелёным. Целевые regression-тесты из Фазы 1 (reader-starvation reproducers в `*_starvation_test.go`) перегоняются после каждого шага чтобы подтвердить: свойство no-starvation сохранено или улучшено.
