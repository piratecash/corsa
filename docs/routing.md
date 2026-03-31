# Routing Table — Distance Vector with Withdrawals

Related documentation:

- [mesh.md](mesh.md) — network layer overview
- [roadmap.md](roadmap.md) — full iteration plan
- [protocol.md](protocol.md) — wire protocol

Source: `internal/core/routing/types.go`, `internal/core/routing/table.go`

### Overview

The routing package implements a distance-vector routing table for the CORSA mesh network. It replaces blind gossip delivery with directed forwarding based on hop-count metrics. Gossip remains the fallback when no route is known.

### Core types

`RouteEntry` represents a single route in the table. Each entry is uniquely identified by the triple `(Identity, Origin, NextHop)` — the destination, who originated the advertisement, and the peer we learned it from. Two entries sharing the same triple are the same route at different points in time; the one with the higher `SeqNo` wins.

`AnnounceEntry` is the wire-safe projection of `RouteEntry`. It contains only the four fields transmitted in `announce_routes` frames (Identity, Origin, Hops, SeqNo). Produced by `RouteEntry.ToAnnounceEntry()` or `Table.AnnounceTo()`. The wire carries the sender's local hop count as-is — the receiver adds +1 when inserting into its own table (Phase 1.2 receive path).

`Table` is the thread-safe local storage for routes. It owns this node's identity (`localOrigin`) and a monotonic SeqNo counter per destination (`seqCounters`). Public operations: `AddDirectPeer`, `RemoveDirectPeer`, `UpdateRoute`, `WithdrawRoute`, `TickTTL`, `Announceable`, `AnnounceTo`, `Lookup`, and `Snapshot`.

`Snapshot` is an immutable point-in-time copy of the full table, safe to read without locks.

`RemoveDirectPeerResult` describes the outcome of a peer disconnect: wire-ready `[]AnnounceEntry` withdrawals (SeqNo already incremented, Hops=16) for own-origin direct routes, plus a count of transit routes silently invalidated locally.

### Key invariants

**Origin-aware SeqNo.** Sequence numbers are scoped per `(Identity, Origin)` pair. Only the origin node may advance the SeqNo. A route with a higher SeqNo unconditionally replaces one with a lower SeqNo for the same triple. For own-origin routes, the `Table` owns the monotonic counter (`seqCounters`) and advances it automatically in `AddDirectPeer` and `RemoveDirectPeer`. This ensures SeqNo discipline is enforced at the data structure level, not delegated to the caller.

**Dedup key.** The triple `(Identity, Origin, NextHop)` uniquely identifies a route lineage. Different origins or different next-hops produce independent entries.

**Split horizon.** When building an announcement for peer A, routes where `NextHop == A` are omitted. No fake `hops=16` withdrawal is sent — that would violate the per-origin SeqNo invariant.

**Withdrawal semantics.** A withdrawal sets `Hops = 16` (infinity) and requires strictly `SeqNo > old.SeqNo`. On the wire, only the origin may send a withdrawal. On peer disconnect, `RemoveDirectPeer` withdraws own-origin direct routes (returned as wire-ready `[]AnnounceEntry` with SeqNo already incremented) and silently invalidates transit routes locally. Transit nodes do not emit wire withdrawals for routes they did not originate.

**Trust hierarchy.** Per triple, `direct > hop_ack > announcement`. At the same SeqNo, a higher-trust source replaces a lower-trust one. Confirming one triple via hop_ack does not affect other triples, even those sharing the same NextHop.

**Route selection order.** `Lookup()` sorts by source priority first (direct > hop_ack > announcement), then by hops ascending within the same source tier. This means a 3-hop direct route is preferred over a 1-hop announcement — source trust outweighs hop count.

**TTL expiry.** Each entry carries an `ExpiresAt` timestamp (default 120s from insertion). `TickTTL()` removes expired entries. Shortened TTL for flapping peers is planned for a future phase when flapping-detection is implemented.

**NextHop is peer identity.** Routing operates at identity level, not transport addresses. A single peer identity may have multiple concurrent TCP sessions; session selection happens later in `node.Service`. Phase 1.2 requires an identity-to-session(s) index in `node.Service` to resolve identities to active connections.

**Entry validation.** `RouteEntry.Validate()` enforces structural invariants at insert time: Identity, Origin, and NextHop must be non-empty; Hops must be in range [1, 16]. Source-specific rules: direct routes (`RouteSourceDirect`) must have `Hops=1` and `NextHop == Identity` (directly connected neighbor). Malformed entries are rejected by `UpdateRoute` before acquiring locks or touching table state.

**Wire projection boundary.** The boundary between table-model and wire-format is explicit: `AnnounceTo(excludeVia)` returns `[]AnnounceEntry` with split horizon applied. The wire carries the sender's local hop count — no +1 is applied on the sender side; the receiver adds +1 when inserting into its own table (Phase 1.2). `RemoveDirectPeer` returns wire-ready `[]AnnounceEntry` withdrawals with SeqNo already incremented. Callers building `announce_routes` frames use `AnnounceTo` or `RemoveDirectPeer` results directly — no manual arithmetic is needed.

**SeqNo counter consistency.** When `UpdateRoute` accepts an own-origin entry (where `Origin == localOrigin`), it synchronizes the monotonic counter: `seqCounters[identity] = max(counter, entry.SeqNo)`. This ensures that tables pre-populated via `UpdateRoute` (e.g., restored from a snapshot or received via full sync) do not cause subsequent `AddDirectPeer`/`RemoveDirectPeer` to emit a stale SeqNo.

### Known limitations (Phase 1.1 scope)

The Phase 1.1 model invariants provide the foundation but do not yet constitute a working routing system. The following risks are tracked and must be resolved in subsequent phases before `mesh_routing_v1` is advertised.

**Convergence (Phase 1.2).** The model has strict withdrawal SeqNo, `RemoveDirectPeer()` with wire-ready withdrawals, and local transit invalidation. But there is no periodic or triggered `announce_routes` send/receive path yet — no `announce.go`, no incoming announcement handler, no triggered updates on connect/disconnect. Without these, the table cannot converge. All convergence items are tracked in Phase 1.2 checklist (announce loop, triggered updates, triggered withdrawal, incoming announce handling). The capability must not be advertised until Phase 1.2 is complete and convergence is verified in integration tests.

**Multi-session identity (Phase 1.2).** The routing table models direct connectivity per identity, not per TCP session. A single peer identity may have multiple concurrent sessions (inbound + outbound, reconnect). `AddDirectPeer()` is idempotent at the model level — calling it for an already-active peer refreshes TTL without bumping SeqNo. However, `RemoveDirectPeer()` unconditionally withdraws the direct route and invalidates transit routes. If the peer still has other live sessions, this produces a false withdrawal. The fix belongs in Phase 1.2 integration: `node.Service` must maintain an identity→session-count index and call `RemoveDirectPeer()` only when the last session for that identity closes (transition 1→0).

**Gossip fallback contract (Phase 1.2, release-blocking).** The routing table is a hint, not the truth. If `RelayNextHop` has no active session, lacks the capability, if send fails between lookup and enqueue, or if no route is known at all — delivery must not break but immediately degrade to the existing gossip path. This contract is release-blocking for Phase 1.2: without it, enabling table routing becomes a potential point of failure. Full multi-route failover (primary → secondary → gossip by hop_ack timeout) is deferred to Iteration 2.

**Announcement flooding (Phase 1.3).** `AnnounceTo()` exists but has no limit on routes per frame (roadmap specifies max 100), no fairness rotation, no pacing/jitter, and no per-peer rate limiting. A node with a large table could overwhelm peers with oversized frames. All anti-flooding measures are tracked in Phase 1.3 checklist (frame size limit, rotation fairness, periodic full sync, rate limits, quotas, jitter). These must be implemented before real-world rollout with untrusted peers.

**Routing loops (Iteration 1 risk).** Split horizon and withdrawal semantics reduce simple loop scenarios. However, the current design has no path vector, no poisoned reverse, and hop-limit applies to message relay TTL, not to DV convergence. For the Iteration 1 MVP, split horizon + withdrawal is considered the minimum viable loop prevention. If integration tests (Phase 1.2) reveal loopy behavior — especially around rapid disconnect/reannounce sequences — a fix must land in Iteration 1, not be deferred. Potential mitigations: triggered withdrawal propagation latency bounds, hold-down timer for recently withdrawn routes, or path recording in a future iteration.

**No global lookup.** Distance vector gives each node knowledge only about destinations reachable via its neighbors. There is no mechanism to discover an arbitrary identity in the network if no neighbor has advertised a route to it. This is not a defect — it is the fundamental boundary of the DV approach. If "find any user in the network" is needed, it requires a separate discovery/query layer (e.g., DHT in Iteration 4, or a dedicated lookup service). The routing table is not the right place for global search, and no patch to it will solve the problem.

### Wire format

The `announce_routes` frame carries a list of route entries. Only fields needed for convergence are transmitted on the wire; TTL and Source are derived locally.

```json
{
  "type": "announce_routes",
  "routes": [
    {"identity": "alice_fp", "origin": "bob_fp", "hops": 1, "seq": 42},
    {"identity": "carol_fp", "origin": "dave_fp", "hops": 16, "seq": 18}
  ]
}
```

A withdrawal is simply a route with `hops: 16` and an incremented `seq`.

### Capability gating

The `mesh_routing_v1` capability is defined but not yet advertised during the handshake. It will be enabled in Phase 1.2 when the full send/receive path for `announce_routes` is implemented. Only peers with this capability in their negotiated set will receive `announce_routes` frames. Legacy peers continue to function via gossip without disruption.

### Package layout

```
internal/core/routing/
    types.go       — RouteEntry, AnnounceEntry, RouteTriple, Snapshot, RouteSource
    table.go       — Table with all CRUD, query, disconnect, and wire-projection operations
    types_test.go  — unit tests for type invariants, validation, wire projection
    table_test.go  — unit tests for table operations and invariants
```

---

# Таблица маршрутизации — Distance Vector с Withdrawal

Связанная документация:

- [mesh.md](mesh.md) — обзор сетевого уровня
- [roadmap.md](roadmap.md) — полный план итераций
- [protocol.md](protocol.md) — протокол передачи данных

Исходники: `internal/core/routing/types.go`, `internal/core/routing/table.go`

### Обзор

Пакет routing реализует таблицу маршрутизации на основе distance-vector для mesh-сети CORSA. Заменяет слепую gossip-доставку на направленную пересылку по метрике количества хопов. Gossip остаётся fallback, когда маршрут неизвестен.

### Основные типы

`RouteEntry` — единичный маршрут в таблице. Каждая запись уникально идентифицируется тройкой `(Identity, Origin, NextHop)` — получатель, кто изначально объявил маршрут, и peer, от которого мы его узнали. Две записи с одинаковой тройкой — один и тот же маршрут в разное время; побеждает запись с большим `SeqNo`.

`AnnounceEntry` — проекция `RouteEntry` для передачи по проводу. Содержит только четыре поля из `announce_routes` фреймов (Identity, Origin, Hops, SeqNo). Создаётся через `RouteEntry.ToAnnounceEntry()` или `Table.AnnounceTo()`. На провод уходит локальное значение hops отправителя как есть — получатель делает +1 при вставке в свою таблицу (Phase 1.2 receive path).

`Table` — потокобезопасное локальное хранилище маршрутов. Владеет identity ноды (`localOrigin`) и монотонным счётчиком SeqNo по каждому destination (`seqCounters`). Публичные операции: `AddDirectPeer`, `RemoveDirectPeer`, `UpdateRoute`, `WithdrawRoute`, `TickTTL`, `Announceable`, `AnnounceTo`, `Lookup` и `Snapshot`.

`Snapshot` — иммутабельная копия таблицы на момент времени, безопасная для чтения без блокировок.

`RemoveDirectPeerResult` — результат отключения peer'а: wire-ready `[]AnnounceEntry` withdrawals (SeqNo уже инкрементирован, Hops=16) для own-origin direct-маршрутов, плюс количество transit-маршрутов, молча инвалидированных локально.

### Ключевые инварианты

**Origin-aware SeqNo.** Порядковые номера ограничены парой `(Identity, Origin)`. Только origin-нода может увеличивать SeqNo. Маршрут с большим SeqNo безусловно заменяет маршрут с меньшим для той же тройки. Для own-origin маршрутов `Table` владеет монотонным счётчиком (`seqCounters`) и автоматически инкрементирует его в `AddDirectPeer` и `RemoveDirectPeer`. Дисциплина SeqNo обеспечивается на уровне структуры данных, а не делегируется вызывающему коду.

**Ключ дедупликации.** Тройка `(Identity, Origin, NextHop)` уникально идентифицирует линию маршрута. Разные origin или разные NextHop создают независимые записи.

**Split horizon.** При формировании анонса для peer A маршруты с `NextHop == A` пропускаются. Фиктивный `hops=16` withdrawal не отправляется — это нарушило бы инвариант per-origin SeqNo.

**Семантика withdrawal.** Отзыв маршрута устанавливает `Hops = 16` (бесконечность) и требует строго `SeqNo > old.SeqNo`. На проводе только origin может отправить withdrawal. При отключении peer'а `RemoveDirectPeer` отзывает own-origin direct-маршруты (возвращаются как wire-ready `[]AnnounceEntry` с уже инкрементированным SeqNo) и молча инвалидирует transit-маршруты локально. Транзитные ноды не генерируют wire-withdrawal для чужих маршрутов.

**Иерархия доверия.** Для каждой тройки: `direct > hop_ack > announcement`. При одинаковом SeqNo источник с более высоким доверием заменяет менее доверенный. Подтверждение одной тройки через hop_ack не влияет на другие тройки, даже при общем NextHop.

**Порядок выбора маршрута.** `Lookup()` сортирует по приоритету source (direct > hop_ack > announcement), затем по hops ascending в пределах одного уровня source. Direct-маршрут с 3 хопами предпочитается announcement с 1 хопом — доверие к источнику важнее количества хопов.

**Истечение TTL.** Каждая запись имеет `ExpiresAt` (по умолчанию 120с с момента вставки). `TickTTL()` удаляет истёкшие записи. Укороченный TTL для нестабильных peer'ов запланирован на будущую фазу при появлении flapping-detection.

**NextHop — это identity peer'a.** Маршрутизация работает на уровне identity, а не транспортных адресов. Один peer identity может иметь несколько параллельных TCP-сессий; выбор сессии происходит позже в `node.Service`. Фаза 1.2 требует индекса identity→session(s) в `node.Service` для резолва identity в активные соединения.

**Валидация записей.** `RouteEntry.Validate()` проверяет структурные инварианты при вставке: Identity, Origin и NextHop не должны быть пустыми; Hops в диапазоне [1, 16]. Source-specific правила: direct-маршруты (`RouteSourceDirect`) должны иметь `Hops=1` и `NextHop == Identity` (непосредственный сосед). Невалидные записи отклоняются в `UpdateRoute` до захвата блокировок.

**Граница wire-проекции.** Граница между table-моделью и wire-форматом явная: `AnnounceTo(excludeVia)` возвращает `[]AnnounceEntry` с applied split horizon. На провод уходит локальное значение hops — +1 не применяется на стороне отправителя; получатель делает +1 при вставке в свою таблицу (Phase 1.2). `RemoveDirectPeer` возвращает wire-ready `[]AnnounceEntry` withdrawals с уже инкрементированным SeqNo. Caller'ы, формирующие `announce_routes` фреймы, используют `AnnounceTo` или результаты `RemoveDirectPeer` напрямую — ручная арифметика не нужна.

**Консистентность счётчика SeqNo.** Когда `UpdateRoute` принимает own-origin запись (где `Origin == localOrigin`), он синхронизирует монотонный счётчик: `seqCounters[identity] = max(counter, entry.SeqNo)`. Это гарантирует, что таблицы, предзаполненные через `UpdateRoute` (например, восстановленные из snapshot или полученные через full sync), не приведут к тому, что следующий `AddDirectPeer`/`RemoveDirectPeer` выдаст устаревший SeqNo.

### Известные ограничения (область Phase 1.1)

Инварианты модели Phase 1.1 закладывают фундамент, но ещё не составляют рабочую систему маршрутизации. Следующие риски отслеживаются и должны быть устранены в последующих фазах до рекламы `mesh_routing_v1`.

**Сходимость (Phase 1.2).** В модели есть strict withdrawal SeqNo, `RemoveDirectPeer()` с wire-ready withdrawals и локальная инвалидация transit-маршрутов. Но пока нет periodic/triggered `announce_routes` send/receive path — нет `announce.go`, нет обработчика входящих анонсов, нет triggered updates на connect/disconnect. Без этого таблица не может сходиться. Все элементы сходимости учтены в чеклисте Phase 1.2. Capability нельзя рекламировать до завершения Phase 1.2 и верификации сходимости в интеграционных тестах.

**Multi-session identity (Phase 1.2).** Routing table моделирует direct-связность на уровне identity, не TCP-сессии. У одного peer identity может быть несколько параллельных сессий (inbound + outbound, reconnect). `AddDirectPeer()` идемпотентен на уровне модели — вызов для уже активного peer'а обновляет TTL без инкремента SeqNo. Но `RemoveDirectPeer()` безусловно отзывает direct route и инвалидирует transit routes. Если у peer'а остались другие живые сессии, это ложный withdrawal. Фикс — в Phase 1.2 integration: `node.Service` должен вести identity→session-count индекс и вызывать `RemoveDirectPeer()` только при закрытии последней сессии (переход 1→0).

**Контракт gossip fallback (Phase 1.2, release-blocking).** Routing table — это подсказка, а не истина. Если у `RelayNextHop` нет активной сессии, отсутствует capability, send не удался между lookup и enqueue, или маршрут вообще неизвестен — доставка не должна ломаться, а немедленно деградировать в существующий gossip path. Этот контракт — release-blocking для Phase 1.2: без него включение table routing становится потенциальной точкой отказа. Полный multi-route failover (primary → secondary → gossip по timeout relay_hop_ack) отложен на Iteration 2.

**Спам анонсами (Phase 1.3).** `AnnounceTo()` существует, но нет лимита маршрутов на фрейм (roadmap ожидает max 100), нет fairness rotation, нет pacing/jitter и нет per-peer rate limiting. Нода с большой таблицей может перегрузить peer'ов огромными фреймами. Все anti-flooding меры учтены в чеклисте Phase 1.3 (лимит размера фрейма, rotation fairness, periodic full sync, rate limits, quotas, jitter). Они должны быть реализованы до реального rollout с недоверенными peer'ами.

**Петли маршрутизации (риск Iteration 1).** Split horizon и семантика withdrawal снижают риск простых loop-сценариев. Но в текущем дизайне нет path vector, нет poisoned reverse, и hop-limit относится к relay TTL сообщений, а не к DV convergence. Для MVP Iteration 1 split horizon + withdrawal считаются минимальной защитой от петель. Если интеграционные тесты (Phase 1.2) выявят loopy поведение — особенно вокруг быстрых disconnect/reannounce последовательностей — фикс должен войти в Iteration 1, а не откладываться. Возможные меры: ограничение латентности распространения triggered withdrawal, hold-down timer для недавно отозванных маршрутов, или path recording в будущей итерации.

**Нет глобального поиска.** Distance vector даёт каждой ноде знание только о destination'ах, достижимых через её соседей. Нет механизма обнаружить произвольную identity в сети, если ни один сосед не анонсировал маршрут к ней. Это не дефект — это фундаментальная граница DV-подхода. Если нужен «найти любого пользователя в сети», требуется отдельный discovery/query слой (например, DHT в Iteration 4 или выделенный lookup-сервис). Routing table — не то место для глобального поиска, и никакая латка в ней эту задачу не решит.

### Формат на проводе

Фрейм `announce_routes` содержит список записей маршрутов. На проводе передаются только поля, необходимые для конвергенции; TTL и Source определяются локально.

```json
{
  "type": "announce_routes",
  "routes": [
    {"identity": "alice_fp", "origin": "bob_fp", "hops": 1, "seq": 42},
    {"identity": "carol_fp", "origin": "dave_fp", "hops": 16, "seq": 18}
  ]
}
```

Withdrawal — это просто маршрут с `hops: 16` и увеличенным `seq`.

### Гейтинг по capability

Capability `mesh_routing_v1` определена, но ещё не рекламируется в handshake. Она будет включена в Фазе 1.2, когда будет реализован полный цикл отправки/получения `announce_routes`. Только peer'ы с этой capability в согласованном наборе будут получать фреймы `announce_routes`. Устаревшие peer'ы продолжают работать через gossip без нарушений.

### Структура пакета

```
internal/core/routing/
    types.go       — RouteEntry, AnnounceEntry, RouteTriple, Snapshot, RouteSource
    table.go       — Table со всеми CRUD, query, disconnect и wire-projection операциями
    types_test.go  — юнит-тесты инвариантов типов, валидации, wire-проекции
    table_test.go  — юнит-тесты операций таблицы и инвариантов
```
