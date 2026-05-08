# Routing Commands

## English

### fetchRouteTable

Full routing table snapshot. Returns every entry (active, withdrawn, expired) with detailed metadata.

```bash
corsa-cli fetchRouteTable
```

Response:
```json
{
  "routes": [
    {
      "identity": "abc123...",
      "origin": "def456...",
      "next_hop": {
        "identity": "abc123...",
        "address": "65.108.204.190:64646",
        "network": "ipv4"
      },
      "hops": 1,
      "seq_no": 5,
      "source": "direct",
      "withdrawn": false,
      "expired": false,
      "ttl_seconds": 98.5
    }
  ],
  "total": 3,
  "active": 2,
  "snapshot_at": "2026-04-01T12:00:00Z"
}
```

| Field | Type | Description |
|---|---|---|
| `routes` | array | All route entries in the table |
| `routes[].identity` | string | Destination peer Ed25519 fingerprint |
| `routes[].origin` | string | Peer that originally advertised this route |
| `routes[].next_hop` | object | Direct neighbor peer to forward through |
| `routes[].next_hop.identity` | string | Ed25519 fingerprint of the next-hop peer |
| `routes[].next_hop.address` | string | Live transport address, best-effort (omitted if peer disconnected) |
| `routes[].next_hop.network` | string | Live network type: `"ipv4"`, `"ipv6"`, `"torv3"`, etc., best-effort (omitted if disconnected) |
| `routes[].hops` | int | Distance to destination (0=local/self, 1=direct, 16=withdrawn) |
| `routes[].seq_no` | int | Monotonic sequence number scoped to origin |
| `routes[].source` | string | How route was learned: `"local"`, `"direct"`, `"hop_ack"`, `"announcement"` |
| `routes[].withdrawn` | bool | True if hops >= 16 |
| `routes[].expired` | bool | True if TTL has elapsed (relative to `snapshot_at`) |
| `routes[].ttl_seconds` | float | Remaining time-to-live in seconds (relative to `snapshot_at`) |
| `total` | int | Total entry count (including withdrawn/expired) |
| `active` | int | Non-withdrawn, non-expired entry count |
| `snapshot_at` | string | ISO 8601 timestamp when snapshot was taken |

All time-dependent fields (`expired`, `ttl_seconds`, reachable counts) are computed relative to `snapshot_at`, not wall clock, ensuring self-consistent responses. Routes are sorted deterministically by (identity, origin, next_hop.identity, source).

**Note on `next_hop.address` / `next_hop.network`:** these fields reflect *live* peer transport state queried after the routing snapshot. They are best-effort metadata — a peer may disconnect between the snapshot and the transport lookup. Routing fields (`identity`, `hops`, `seq_no`, `source`, `withdrawn`, `expired`, `ttl_seconds`) are fully atomic within `snapshot_at`.

### fetchRouteSummary

Compact routing table overview for dashboards and monitoring.

```bash
corsa-cli fetchRouteSummary
```

Response:
```json
{
  "snapshot_at": "2026-04-01T12:00:00Z",
  "total_entries": 5,
  "active_entries": 3,
  "withdrawn_entries": 2,
  "reachable_identities": 3,
  "direct_peers": 2,
  "flap_state": [
    {
      "peer_identity": "abc123...",
      "recent_withdrawals": 4,
      "in_hold_down": true,
      "hold_down_until": "2026-04-01T12:01:30Z"
    }
  ],
  "cap_admission": {
    "accepted": 1247,
    "accepted_replaced": 18,
    "rejected_full": 4,
    "rejected_all_protected": 0
  }
}
```

| Field | Type | Description |
|---|---|---|
| `snapshot_at` | string | RFC 3339 timestamp of the point-in-time snapshot |
| `total_entries` | int | Total route entries in table |
| `active_entries` | int | Non-withdrawn, non-expired entries |
| `withdrawn_entries` | int | Entries with hops=16 or expired |
| `reachable_identities` | int | Unique destinations with at least one active route |
| `direct_peers` | int | Destinations reachable via direct route (hops=1, source=direct) |
| `flap_state` | array | Peers with active flap detection state |
| `flap_state[].peer_identity` | string | Peer Ed25519 fingerprint |
| `flap_state[].recent_withdrawals` | int | Disconnect count within flap window |
| `flap_state[].in_hold_down` | bool | True if peer is in hold-down (penalized TTL) |
| `flap_state[].hold_down_until` | string | ISO 8601 timestamp when hold-down expires (only when in_hold_down=true) |
| `cap_admission` | object | Cumulative `MaxNextHopsPerOrigin` admission counters since process start. Stay at zero on tables with the cap disabled. |
| `cap_admission.accepted` | int | Entries admitted into a `(Identity, Origin)` bucket with room — no eviction. |
| `cap_admission.accepted_replaced` | int | Entries admitted by displacing the worst evictable candidate. `accepted_replaced / (accepted + accepted_replaced)` is the cap eviction rate; a rising rate signals route churn or an undersized cap. |
| `cap_admission.rejected_full` | int | Entries dropped because the bucket was at the cap and the incoming entry was not strictly better than the worst evictable candidate. |
| `cap_admission.rejected_all_protected` | int | Entries dropped because every member of a specific `(Identity, Origin)` bucket was direct/local (cap cannot evict those). Direct rows have `NextHop == Identity` so any one bucket holds at most one direct entry plus at most one local entry — this counter does NOT mean "many direct peers exceeded K". Effectively a "K=1 was too tight for this destination" marker; non-zero with K≥2 usually points at test fixtures or direct-restore edge cases. |

### fetchRouteLookup

Lookup routes for a specific destination identity. Returns routes sorted by preference (source priority, then hops ascending).

```bash
corsa-cli fetchRouteLookup <identity>
```

Response:
```json
{
  "identity": "abc123...",
  "routes": [
    {
      "origin": "def456...",
      "next_hop": "abc123...",
      "hops": 1,
      "seq_no": 5,
      "source": "direct",
      "ttl_seconds": 98.5
    },
    {
      "origin": "ghi789...",
      "next_hop": "ghi789...",
      "hops": 2,
      "seq_no": 3,
      "source": "announcement",
      "ttl_seconds": 45.2
    }
  ],
  "count": 2,
  "snapshot_at": "2026-04-01T12:00:00Z"
}
```

| Field | Type | Description |
|---|---|---|
| `identity` | string | Queried destination identity |
| `routes` | array | Routes sorted by preference (source trust desc, then hops asc) |
| `routes[].origin` | string | Peer that originated this route |
| `routes[].next_hop` | string | Peer to forward through |
| `routes[].hops` | int | Distance to destination |
| `routes[].seq_no` | int | Sequence number |
| `routes[].source` | string | `"local"`, `"direct"`, `"hop_ack"`, or `"announcement"` |
| `routes[].ttl_seconds` | float | Remaining TTL (relative to `snapshot_at`); 0 for local self-route |
| `count` | int | Number of routes found |
| `snapshot_at` | string | ISO 8601 timestamp when snapshot was taken |

When the queried identity matches the node's own identity, a synthetic local route (`source: "local"`, `hops: 0`) is always present. Returns `count: 0` and empty `routes` array if the identity is unknown or has no active routes. Returns 400 if `identity` argument is missing or malformed (must be 40-char lowercase hex).

### Snapshot freshness

`fetchRouteTable`, `fetchRouteSummary` and `fetchRouteLookup` all read the cached routing snapshot maintained by the node's hot-reads refresher (the same infrastructure that backs `fetch_network_stats`, `fetch_peer_health`, `get_peers` and the ConnectionManager slots view). The snapshot is rebuilt by a background goroutine when the routing table is dirty (i.e. some writer accepted a real mutation since the previous publish); idle ticks skip the rebuild.

Two staleness bounds apply:

- **Structural changes** (route accepted, withdrawn, replaced; direct peer added/removed; flap burst arming hold-down; flap-state cleanup after a writer touched the table) are visible to RPC readers within one refresh interval (currently ~500 ms) plus the time the publisher needs to acquire `routing.Table.t.mu.RLock`. This is the bound that matters for UI dashboards reacting to topology changes.
- **Time-derived state** (`expired`, `ttl_seconds`, `flap_state[].in_hold_down`) is bounded loosely. The dirty-flag publisher only republishes when a writer touches the table, but the wall-clock timestamps that drive these fields advance without writer events:
  - A finite-TTL route quietly elapses — no writer until `TickTTL` (every 10 s) rewrites the entry. `expired` and `ttl_seconds` therefore stay at the snapshot's own `snapshot_at`-based view until `TickTTL` republishes; worst-case lag for "route aged out" equals `TickTTL` interval (≈10 s) plus one refresh tick (≈500 ms).
  - `flap_state[].in_hold_down` flips from `true` to `false` when `holdDownUntil` elapses — also not a writer event. `TickTTL` clears the deadline and marks the table dirty so the next refresh publishes `in_hold_down=false`. Worst-case lag for hold-down expiry is therefore `TickTTL` interval (≈10 s) + one refresh tick (≈500 ms). Hold-down arming is a writer event (the disconnect that crossed the flap threshold) and is reflected within the structural bound. The snapshot also normalizes `flap_state[].hold_down_until` to absent whenever `in_hold_down=false`, so consumers never observe a past-timestamp deadline paired with `in_hold_down=false`.

  Code that must observe finite-TTL expiry or hold-down expiry promptly should evaluate `ExpiresAt`/`HoldDownUntil` against `time.Now()` directly rather than rely on the cached fields, or call `routing.Table.Lookup(peer)` (per-destination, O(K)) / `routing.Table.Snapshot()` (full table) for a strictly fresh view.

Other consequences:

- The routing read path takes no routing-table mutex. A writer storm on the routing table (announce loop convergence, mass disconnect, hop_ack burst) does not block the RPC.
- `snapshot_at` is the timestamp at which the cached snapshot was rebuilt, not the timestamp of the RPC call. Two RPCs issued within one refresh interval may report the same `snapshot_at`.
- Time-derived fields stay self-consistent within a single RPC response — every entry in one response is evaluated against the same `snapshot_at`.
- Code that needs strictly fresh state must read the routing table directly inside the node process; this path is not exposed over RPC. File-transfer reachability probes (`file_integration.isPeerReachable`), the locally-originated file send (`filerouter.Router.SendFileCommand` via `RouterConfig.RouteLookup`), and the diagnostic surface (`filerouter.Router.ExplainRoute`, also via `RouteLookup`) all call `routing.Table.Lookup(peer)` for an O(K) per-destination read so a route accepted moments before the call is visible immediately; integration tests that need the full picture call `routing.Table.Snapshot()` directly. The transit forwarding path inside `HandleInbound` keeps using the cached snapshot — an in-flight frame already carries its own metadata and a one-tick delay there is harmless.

**Caveat — `fetchRouteTable` next-hop enrichment.** The `next_hop.address` and `next_hop.network` fields are not part of the routing snapshot. They are resolved per unique next-hop identity through `RoutingProvider.PeerTransport`, which currently takes `s.peerMu.RLock`. A peer-domain writer storm can therefore still delay `fetchRouteTable` even though the routing-table read itself is lock-free. `fetchRouteSummary` and `fetchRouteLookup` are unaffected — they emit no live transport metadata. Migrating `PeerTransport` onto a cached peer-transport snapshot is tracked separately and is out of scope for the routing-snapshot phase.

### Availability

All routing commands require `RoutingProvider`. When the provider is nil (e.g., node without routing table), commands return 503 and are hidden from `help` output.

---

## Русский

### fetchRouteTable

Полный снапшот таблицы маршрутизации. Возвращает каждую запись (активную, отозванную, истёкшую) с подробными метаданными.

```bash
corsa-cli fetchRouteTable
```

Ответ:
```json
{
  "routes": [
    {
      "identity": "abc123...",
      "origin": "def456...",
      "next_hop": {
        "identity": "abc123...",
        "address": "65.108.204.190:64646",
        "network": "ipv4"
      },
      "hops": 1,
      "seq_no": 5,
      "source": "direct",
      "withdrawn": false,
      "expired": false,
      "ttl_seconds": 98.5
    }
  ],
  "total": 3,
  "active": 2,
  "snapshot_at": "2026-04-01T12:00:00Z"
}
```

| Поле | Тип | Описание |
|---|---|---|
| `routes` | array | Все записи маршрутов в таблице |
| `routes[].identity` | string | Ed25519 fingerprint целевого peer'a |
| `routes[].origin` | string | Peer, изначально объявивший этот маршрут |
| `routes[].next_hop` | object | Прямой сосед для пересылки пакетов |
| `routes[].next_hop.identity` | string | Ed25519 fingerprint next-hop peer'a |
| `routes[].next_hop.address` | string | Живой транспортный адрес, best-effort (отсутствует если peer отключён) |
| `routes[].next_hop.network` | string | Живой тип сети: `"ipv4"`, `"ipv6"`, `"torv3"` и т.д., best-effort (отсутствует если отключён) |
| `routes[].hops` | int | Расстояние до цели (1=прямой, 16=отозван) |
| `routes[].seq_no` | int | Монотонный sequence number, привязанный к origin |
| `routes[].source` | string | Как маршрут был получен: `"direct"`, `"hop_ack"`, `"announcement"` |
| `routes[].withdrawn` | bool | True если hops >= 16 |
| `routes[].expired` | bool | True если TTL истёк (относительно `snapshot_at`) |
| `routes[].ttl_seconds` | float | Оставшееся время жизни в секундах (относительно `snapshot_at`) |
| `total` | int | Общее количество записей (включая отозванные/истёкшие) |
| `active` | int | Количество не-отозванных, не-истёкших записей |
| `snapshot_at` | string | ISO 8601 timestamp момента снапшота |

Все зависящие от времени поля (`expired`, `ttl_seconds`, количество доступных) вычисляются относительно `snapshot_at`, а не текущего времени, обеспечивая внутренне согласованные ответы. Маршруты сортируются детерминированно по (identity, origin, next_hop.identity, source).

**Примечание о `next_hop.address` / `next_hop.network`:** эти поля отражают *живое* транспортное состояние peer'a, запрашиваемое после снапшота маршрутизации. Это best-effort метаданные — peer может отключиться между снапшотом и запросом транспорта. Поля маршрутизации (`identity`, `hops`, `seq_no`, `source`, `withdrawn`, `expired`, `ttl_seconds`) полностью атомарны в рамках `snapshot_at`.

### fetchRouteSummary

Компактный обзор таблицы маршрутизации для дашбордов и мониторинга.

```bash
corsa-cli fetchRouteSummary
```

Ответ:
```json
{
  "snapshot_at": "2026-04-01T12:00:00Z",
  "total_entries": 5,
  "active_entries": 3,
  "withdrawn_entries": 2,
  "reachable_identities": 3,
  "direct_peers": 2,
  "flap_state": [
    {
      "peer_identity": "abc123...",
      "recent_withdrawals": 4,
      "in_hold_down": true,
      "hold_down_until": "2026-04-01T12:01:30Z"
    }
  ],
  "cap_admission": {
    "accepted": 1247,
    "accepted_replaced": 18,
    "rejected_full": 4,
    "rejected_all_protected": 0
  }
}
```

| Поле | Тип | Описание |
|---|---|---|
| `snapshot_at` | string | RFC 3339 временная метка point-in-time снапшота |
| `total_entries` | int | Всего записей маршрутов в таблице |
| `active_entries` | int | Не-отозванные, не-истёкшие записи |
| `withdrawn_entries` | int | Записи с hops=16 или истёкшие |
| `reachable_identities` | int | Уникальные destinations с хотя бы одним активным маршрутом |
| `direct_peers` | int | Destinations, доступные через direct route (hops=1, source=direct) |
| `flap_state` | array | Peer'ы с активным состоянием flap detection |
| `flap_state[].peer_identity` | string | Ed25519 fingerprint peer'a |
| `flap_state[].recent_withdrawals` | int | Количество отключений в пределах flap window |
| `flap_state[].in_hold_down` | bool | True если peer в hold-down (укороченный TTL) |
| `flap_state[].hold_down_until` | string | ISO 8601 timestamp окончания hold-down (только при in_hold_down=true) |
| `cap_admission` | object | Кумулятивные счётчики admission'а `MaxNextHopsPerOrigin` с момента старта процесса. Остаются на нуле на таблицах с отключённым cap'ом. |
| `cap_admission.accepted` | int | Записи, admitted в `(Identity, Origin)` bucket с местом — без eviction. |
| `cap_admission.accepted_replaced` | int | Записи, admitted через displacement худшего evictable кандидата. `accepted_replaced / (accepted + accepted_replaced)` — cap eviction rate; растущий rate сигналит route churn или undersized cap. |
| `cap_admission.rejected_full` | int | Записи, отклонённые потому что bucket был at-cap и incoming не строго лучше worst evictable кандидата. |
| `cap_admission.rejected_all_protected` | int | Записи, отклонённые потому что каждый member конкретного `(Identity, Origin)` bucket'а был direct/local (cap не может evict'нуть таких). Direct-строки имеют `NextHop == Identity`, поэтому один bucket содержит максимум одну direct entry плюс максимум одну local entry — этот counter НЕ означает «много direct peer'ов превысили K». По сути это маркер «K=1 оказался слишком жёстким для этого destination»; ненулевое при K≥2 обычно указывает на тестовые fixtures или direct-restore edge case. |

### fetchRouteLookup

Поиск маршрутов для конкретного destination identity. Возвращает маршруты, отсортированные по предпочтению (приоритет source, затем hops по возрастанию).

```bash
corsa-cli fetchRouteLookup <identity>
```

Ответ:
```json
{
  "identity": "abc123...",
  "routes": [
    {
      "origin": "def456...",
      "next_hop": "abc123...",
      "hops": 1,
      "seq_no": 5,
      "source": "direct",
      "ttl_seconds": 98.5
    },
    {
      "origin": "ghi789...",
      "next_hop": "ghi789...",
      "hops": 2,
      "seq_no": 3,
      "source": "announcement",
      "ttl_seconds": 45.2
    }
  ],
  "count": 2,
  "snapshot_at": "2026-04-01T12:00:00Z"
}
```

| Поле | Тип | Описание |
|---|---|---|
| `identity` | string | Запрошенный destination identity |
| `routes` | array | Маршруты, отсортированные по предпочтению (trust source desc, затем hops asc) |
| `routes[].origin` | string | Peer, породивший этот маршрут |
| `routes[].next_hop` | string | Peer для пересылки |
| `routes[].hops` | int | Расстояние до цели |
| `routes[].seq_no` | int | Sequence number |
| `routes[].source` | string | `"local"`, `"direct"`, `"hop_ack"` или `"announcement"` |
| `routes[].ttl_seconds` | float | Оставшийся TTL (относительно `snapshot_at`); 0 для локального собственного маршрута |
| `count` | int | Количество найденных маршрутов |
| `snapshot_at` | string | ISO 8601 timestamp момента снапшота |

Когда запрашиваемый identity совпадает с собственным identity ноды, синтетический локальный маршрут (`source: "local"`, `hops: 0`) всегда присутствует. Возвращает `count: 0` и пустой массив `routes`, если identity неизвестен или не имеет активных маршрутов. Возвращает 400, если аргумент `identity` отсутствует или имеет неверный формат (должен быть 40-символьный hex в нижнем регистре).

### Свежесть снапшота

`fetchRouteTable`, `fetchRouteSummary` и `fetchRouteLookup` читают кэшированный снапшот маршрутизации, поддерживаемый фоновым refresher'ом ноды (та же инфраструктура, что обслуживает `fetch_network_stats`, `fetch_peer_health`, `get_peers` и view ConnectionManager). Снапшот перестраивается фоновой горутиной только когда таблица стала dirty (то есть какой-то writer принял реальную мутацию с момента предыдущего publish'а); idle-тики rebuild не делают.

Действуют две разные границы свежести:

- **Структурные изменения** (маршрут принят, отозван, заменён; добавлен/удалён direct peer; flap-burst, армирующий hold-down; flap-state cleanup после writer-touch'а) видны RPC-читателям не позже чем через один refresh-интервал (сейчас ~500 ms) плюс время, нужное publisher'у на захват `routing.Table.t.mu.RLock`. Это та граница, что важна для UI-дашбордов, реагирующих на изменения топологии.
- **Time-производные поля** (`expired`, `ttl_seconds`, `flap_state[].in_hold_down`) ограничены мягче. Dirty-флаг publisher срабатывает только когда writer трогает таблицу, но wall-clock timestamps, которые двигают эти поля, идут вперёд без writer-событий:
  - Маршрут с конечным TTL тихо протухает — никакого writer'а до тех пор, пока `TickTTL` (каждые 10 s) не перепишет запись. `expired` и `ttl_seconds` поэтому остаются на представлении относительно собственного `snapshot_at` снапшота, пока `TickTTL` не сделает republish; worst-case lag «маршрут протух» равен интервалу `TickTTL` (≈10 s) плюс один refresh-тик (≈500 ms).
  - `flap_state[].in_hold_down` переключается с `true` на `false` когда `holdDownUntil` истекает — тоже не writer-событие. `TickTTL` обнуляет deadline и помечает таблицу dirty, чтобы следующий refresh опубликовал `in_hold_down=false`. Worst-case lag для истечения hold-down поэтому равен `TickTTL` interval (≈10 s) + один refresh-тик (≈500 ms). Армирование hold-down — это writer-событие (disconnect, который пересёк flap threshold), и оно отражается в пределах структурной границы. Снапшот также нормализует `flap_state[].hold_down_until` до отсутствующего, когда `in_hold_down=false`, поэтому консумеры никогда не наблюдают past-timestamp deadline в паре с `in_hold_down=false`.

  Код, который обязан промптно отрабатывать TTL-протухание или истечение hold-down, должен сравнивать `ExpiresAt`/`HoldDownUntil` с `time.Now()` напрямую, а не полагаться на кэшированные поля, либо звать `routing.Table.Lookup(peer)` (per-destination, O(K)) / `routing.Table.Snapshot()` (вся таблица) для строго свежего вида.

Прочие следствия:

- Routing-чтение RPC не берёт мьютекс таблицы маршрутизации. Writer-шторм на таблице (конвергенция announce-цикла, массовый disconnect, всплеск hop_ack) не блокирует RPC.
- `snapshot_at` — это момент, когда кэшированный снапшот был перестроен, а не момент RPC-вызова. Два RPC внутри одного refresh-интервала могут вернуть один и тот же `snapshot_at`.
- Time-производные поля остаются самосогласованными в пределах одного ответа — каждая запись вычисляется относительно одного и того же `snapshot_at`.
- Коду, которому нужно строго свежее состояние, следует читать таблицу напрямую внутри процесса ноды; этот путь не экспонируется по RPC. File-transfer probe-ы (`file_integration.isPeerReachable`), locally-originated отправка файлов (`filerouter.Router.SendFileCommand` через `RouterConfig.RouteLookup`) и диагностический surface (`filerouter.Router.ExplainRoute`, тоже через `RouteLookup`) — все зовут `routing.Table.Lookup(peer)` для O(K) per-destination чтения, чтобы маршрут, принятый за моменты до вызова, был виден немедленно; интеграционные тесты, которым нужна полная картина, зовут `routing.Table.Snapshot()` напрямую. Transit-форвард внутри `HandleInbound` продолжает использовать кэшированный snapshot — in-flight frame уже несёт собственные metadata и one-tick задержка там безвредна.

**Оговорка — обогащение `next_hop` в `fetchRouteTable`.** Поля `next_hop.address` и `next_hop.network` не входят в снапшот маршрутизации. Они резолвятся per уникальный next-hop identity через `RoutingProvider.PeerTransport`, который в текущей реализации берёт `s.peerMu.RLock`. Writer-шторм в peer-домене поэтому всё ещё может задержать `fetchRouteTable`, хотя само чтение таблицы маршрутизации уже lock-free. `fetchRouteSummary` и `fetchRouteLookup` этой оговоркой не затрагиваются — они не отдают live transport metadata. Миграция `PeerTransport` на кэшированный peer-transport снапшот отслеживается отдельно и выходит за рамки этапа routing-снапшота.

### Доступность

Все routing-команды требуют `RoutingProvider`. Когда провайдер nil (например, нода без таблицы маршрутизации), команды возвращают 503 и скрыты из вывода `help`.
