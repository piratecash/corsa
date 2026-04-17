# Network Commands

## English

### POST /rpc/v1/network/peers

List all known peers. Returns the raw peer-exchange response from the node — the same semantic result regardless of transport (HTTP RPC or in-process desktop).

Response:
```json
{
  "type": "peers",
  "peers": [
    {
      "address": "65.108.204.190:64646",
      "network": "ipv4"
    }
  ]
}
```

### POST /rpc/v1/network/health

Peer health status.

Note: `fetchPeerHealth` response now includes a `capabilities` field for each peer — a list of negotiated capability tokens (e.g., `["mesh_relay_v1"]`). Legacy peers report `null` or empty array.

`fetchPeerHealth` also includes version diagnostic fields per peer: `last_error_code` (protocol error code of the most recent pre-handshake rejection, e.g. `incompatible-protocol-version`; cleared on successful reconnect and by `add_peer`), `last_disconnect_code` (protocol error code that caused the most recent post-handshake socket teardown, e.g. `frame-too-large`, `rate-limited`; empty when disconnect was clean or non-protocol; cleared on successful reconnect), `incompatible_version_attempts` (overlay-level accumulated attempt count), `last_incompatible_version_at` (timestamp of last incompatible-version observation), `observed_peer_version`, `observed_peer_minimum_version` (protocol version evidence from the last handshake attempt), and `version_lockout_active` (whether the peer has a persisted version lockout). The `fetch_aggregate_status` response embeds version policy fields directly (not as a separate block): `update_available` (bool), `update_reason` (one of `peer_build_newer`, `incompatible_version_reporters`, `peer_build_and_incompatible_version`, or empty), `incompatible_version_reporters` (count of distinct peer identities that reported incompatibility), `max_observed_peer_build`, and `max_observed_peer_version` (highest protocol version among incompatible peers — computed from runtime reporter observations and active persisted lockouts; zero when no incompatible evidence exists; informational for UI, no business logic branches on it). All version policy fields are `omitempty` for backward compatibility.

### POST /rpc/v1/network/stats

Aggregated network traffic statistics.

Response includes per-peer bytes sent/received and total node traffic.

### POST /rpc/v1/network/add_peer

Add a peer. This is the explicit operator override mechanism: in addition to adding/promoting the peer and clearing ban state, it also resets all incompatible-version diagnostics (`IncompatibleVersionAttempts`, `LastErrorCode`, observed version fields) and removes any persisted version lockout. The version policy is recomputed immediately so the lockout no longer contributes to `update_available`. The peer is dialled immediately after the override.

Request: `{"address": "host:port"}`

### POST /rpc/v1/network/active_connections

Snapshot of all currently live peer connections (both inbound and outbound). Unlike `getActivePeers` which returns ConnectionManager slot snapshots, this command returns connection-oriented data from the health subsystem — every TCP socket that has completed the handshake and is in a healthy, degraded, or stalled state.

Command name: `getActiveConnections` (snake_case alias: `get_active_connections`).

Response:
```json
{
  "version": 1,
  "connections": [
    {
      "peer_address": "65.108.204.190:64646",
      "remote_address": "65.108.204.190:64646",
      "identity": "abc123def456...",
      "direction": "outbound",
      "network": "ipv4",
      "state": "healthy",
      "conn_id": 42,
      "slot_state": "active"
    }
  ],
  "count": 1
}
```

Field semantics:

- `peer_address` — the address the node dials or accepts from (host:port).
- `remote_address` — the actual TCP endpoint; may differ from `peer_address` when the CM resolved a different port during connection.
- `identity` — peer's cryptographic identity string. Always present in the JSON (never omitted), but may be an empty string for peers that haven't completed identity exchange.
- `direction` — `"inbound"` or `"outbound"`.
- `network` — network group classification: `ipv4`, `ipv6`, `torv3`, `torv2`, `i2p`, `cjdns`, `local`, `unknown`. This is an open string enum — future versions may add new values without a version bump; clients should handle unknown values gracefully.
- `state` — connection health state: `healthy`, `degraded`, `stalled`.
- `conn_id` — unique connection identifier (nonzero for active connections).
- `slot_state` — CM slot state (`"active"`, `"initializing"`); omitted when no CM slot is associated with the connection.

Sort order: outbound connections first, then sorted by peer_address, remote_address, conn_id.

The `version` field enables forward-compatible evolution. See the design document (`docs/active-connections-rpc-design.md` §9) for the versioning contract and two-phase decode strategy.

---

## Русский

### POST /rpc/v1/network/peers

Список всех известных пиров. Возвращает raw peer-exchange ответ от ноды — одинаковый семантический результат вне зависимости от транспорта (HTTP RPC или in-process desktop).

Ответ:
```json
{
  "type": "peers",
  "peers": [
    {
      "address": "65.108.204.190:64646",
      "network": "ipv4"
    }
  ]
}
```

### POST /rpc/v1/network/health

Состояние здоровья пиров.

Примечание: ответ `fetchPeerHealth` теперь включает поле `capabilities` для каждого пира — список согласованных capability-токенов (например, `["mesh_relay_v1"]`). Legacy-пиры возвращают `null` или пустой массив.

`fetchPeerHealth` также включает диагностические поля версии для каждого пира: `last_error_code` (код ошибки протокола последнего отказа до handshake, напр. `incompatible-protocol-version`; очищается при успешном переподключении и командой `add_peer`), `last_disconnect_code` (код ошибки протокола, вызвавшего последнее разъединение после handshake, напр. `frame-too-large`, `rate-limited`; пустое при чистом отключении или не-протокольной ошибке; очищается при успешном переподключении), `incompatible_version_attempts` (накопительный счётчик попыток на overlay-уровне), `last_incompatible_version_at` (временная метка последнего наблюдения несовместимой версии), `observed_peer_version`, `observed_peer_minimum_version` (версионные данные из последней попытки handshake), и `version_lockout_active` (наличие персистированного version lockout). Ответ `fetch_aggregate_status` встраивает поля version policy напрямую (не как отдельный блок): `update_available` (bool), `update_reason` (одно из `peer_build_newer`, `incompatible_version_reporters`, `peer_build_and_incompatible_version` или пустое), `incompatible_version_reporters` (количество уникальных peer identity, сообщивших о несовместимости), `max_observed_peer_build` и `max_observed_peer_version` (наибольшая версия протокола среди несовместимых peer'ов — вычисляется из runtime-наблюдений репортёров и активных персистированных lockout'ов; ноль при отсутствии данных о несовместимости; информационное для UI, бизнес-логика не ветвится по нему). Все поля version policy используют `omitempty` для обратной совместимости.

### POST /rpc/v1/network/stats

Агрегированная статистика сетевого трафика.

Ответ включает количество байт отправленных/полученных по каждому пиру и общий трафик ноды.

### POST /rpc/v1/network/add_peer

Добавление пира. Это явный механизм override оператора: помимо добавления/повышения приоритета пира и сброса бана, команда также обнуляет всю диагностику несовместимых версий (`IncompatibleVersionAttempts`, `LastErrorCode`, поля наблюдаемых версий) и удаляет персистированный version lockout. Version policy пересчитывается немедленно, чтобы lockout больше не влиял на `update_available`. Дозвон к пиру запускается сразу после override.

Запрос: `{"address": "host:port"}`

### POST /rpc/v1/network/active_connections

Снимок всех текущих живых соединений с пирами (входящих и исходящих). В отличие от `getActivePeers`, который возвращает снимок слотов ConnectionManager, эта команда возвращает данные, ориентированные на соединения, из подсистемы health — каждый TCP-сокет, прошедший handshake и находящийся в состоянии healthy, degraded или stalled.

Имя команды: `getActiveConnections` (snake_case алиас: `get_active_connections`).

Ответ:
```json
{
  "version": 1,
  "connections": [
    {
      "peer_address": "65.108.204.190:64646",
      "remote_address": "65.108.204.190:64646",
      "identity": "abc123def456...",
      "direction": "outbound",
      "network": "ipv4",
      "state": "healthy",
      "conn_id": 42,
      "slot_state": "active"
    }
  ],
  "count": 1
}
```

Семантика полей:

- `peer_address` — адрес, по которому нода подключается или принимает подключение (host:port).
- `remote_address` — фактический TCP-эндпоинт; может отличаться от `peer_address`, когда CM разрешил другой порт при подключении.
- `identity` — криптографический идентификатор пира. Всегда присутствует в JSON (никогда не пропускается), но может быть пустой строкой для пиров, не завершивших обмен идентификацией.
- `direction` — `"inbound"` или `"outbound"`.
- `network` — классификация сетевой группы: `ipv4`, `ipv6`, `torv3`, `torv2`, `i2p`, `cjdns`, `local`, `unknown`. Это открытый строковый enum — будущие версии могут добавить новые значения без увеличения version; клиенты должны корректно обрабатывать неизвестные значения.
- `state` — состояние здоровья соединения: `healthy`, `degraded`, `stalled`.
- `conn_id` — уникальный идентификатор соединения (ненулевой для активных соединений).
- `slot_state` — состояние слота CM (`"active"`, `"initializing"`); пропускается, если с соединением не связан слот CM.

Порядок сортировки: исходящие соединения первыми, затем сортировка по peer_address, remote_address, conn_id.

Поле `version` обеспечивает forward-compatible эволюцию. Контракт версионирования и стратегия двухфазного декодирования описаны в проектном документе (`docs/active-connections-rpc-design.md` §9).
