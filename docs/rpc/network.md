# Network Commands

## English

### POST /rpc/v1/network/peers

List all known peers. In desktop mode, returns enriched JSON with health data and per-peer `client_version` / `client_build`. Peers without a known build report `client_build: 0`.

Response (desktop mode):
```json
{
  "type": "peers",
  "count": 1,
  "total": 2,
  "connected": [
    {
      "address": "65.108.204.190:64646",
      "network": "ipv4",
      "client_version": "0.19-alpha",
      "client_build": 19,
      "state": "healthy",
      "connected": true,
      "bytes_sent": 1024,
      "bytes_received": 2048,
      "total_traffic": 3072
    }
  ],
  "pending": [],
  "known_only": ["192.0.2.1:64646"],
  "peers": [...]
}
```

### POST /rpc/v1/network/health

Peer health status.

Note: `fetch_peer_health` response now includes a `capabilities` field for each peer — a list of negotiated capability tokens (e.g., `["mesh_relay_v1"]`). Legacy peers report `null` or empty array.

### POST /rpc/v1/network/stats

Aggregated network traffic statistics.

Response includes per-peer bytes sent/received and total node traffic.

### POST /rpc/v1/network/add_peer

Add a peer.

Request: `{"address": "host:port"}`

---

## Русский

### POST /rpc/v1/network/peers

Список всех известных пиров. В desktop-режиме возвращает обогащённый JSON с данными о здоровье и полями `client_version` / `client_build` для каждого пира. Пиры без известного билда возвращают `client_build: 0`.

Ответ (desktop-режим):
```json
{
  "type": "peers",
  "count": 1,
  "total": 2,
  "connected": [
    {
      "address": "65.108.204.190:64646",
      "network": "ipv4",
      "client_version": "0.19-alpha",
      "client_build": 19,
      "state": "healthy",
      "connected": true,
      "bytes_sent": 1024,
      "bytes_received": 2048,
      "total_traffic": 3072
    }
  ],
  "pending": [],
  "known_only": ["192.0.2.1:64646"],
  "peers": [...]
}
```

### POST /rpc/v1/network/health

Состояние здоровья пиров.

Примечание: ответ `fetch_peer_health` теперь включает поле `capabilities` для каждого пира — список согласованных capability-токенов (например, `["mesh_relay_v1"]`). Legacy-пиры возвращают `null` или пустой массив.

### POST /rpc/v1/network/stats

Агрегированная статистика сетевого трафика.

Ответ включает количество байт отправленных/полученных по каждому пиру и общий трафик ноды.

### POST /rpc/v1/network/add_peer

Добавление пира.

Запрос: `{"address": "host:port"}`
