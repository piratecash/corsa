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
