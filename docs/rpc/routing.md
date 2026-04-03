# Routing Commands

## English

### fetch_route_table

Full routing table snapshot. Returns every entry (active, withdrawn, expired) with detailed metadata.

```bash
corsa-cli fetch_route_table
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

### fetch_route_summary

Compact routing table overview for dashboards and monitoring.

```bash
corsa-cli fetch_route_summary
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
  ]
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

### fetch_route_lookup

Lookup routes for a specific destination identity. Returns routes sorted by preference (source priority, then hops ascending).

```bash
corsa-cli fetch_route_lookup <identity>
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

### Availability

All routing commands require `RoutingProvider`. When the provider is nil (e.g., node without routing table), commands return 503 and are hidden from `help` output.

---

## Русский

### fetch_route_table

Полный снапшот таблицы маршрутизации. Возвращает каждую запись (активную, отозванную, истёкшую) с подробными метаданными.

```bash
corsa-cli fetch_route_table
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

### fetch_route_summary

Компактный обзор таблицы маршрутизации для дашбордов и мониторинга.

```bash
corsa-cli fetch_route_summary
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
  ]
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

### fetch_route_lookup

Поиск маршрутов для конкретного destination identity. Возвращает маршруты, отсортированные по предпочтению (приоритет source, затем hops по возрастанию).

```bash
corsa-cli fetch_route_lookup <identity>
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

### Доступность

Все routing-команды требуют `RoutingProvider`. Когда провайдер nil (например, нода без таблицы маршрутизации), команды возвращают 503 и скрыты из вывода `help`.
