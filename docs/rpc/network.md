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

### POST /rpc/v1/network/stats

Aggregated network traffic statistics.

Response includes per-peer bytes sent/received and total node traffic.

### POST /rpc/v1/network/add_peer

Add a peer.

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

### POST /rpc/v1/network/stats

Агрегированная статистика сетевого трафика.

Ответ включает количество байт отправленных/полученных по каждому пиру и общий трафик ноды.

### POST /rpc/v1/network/add_peer

Добавление пира.

Запрос: `{"address": "host:port"}`
