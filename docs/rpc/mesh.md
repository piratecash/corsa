# Mesh Commands

## English

### POST /rpc/v1/mesh/relay_status

Relay subsystem diagnostics. Returns the current state of the hop-by-hop relay engine — active forwarding states and the number of relay-capable peers.

Response:
```json
{
  "type": "relay_status",
  "status": "ok",
  "count": 5,
  "limit": 3
}
```

| Field | Type | Description |
|---|---|---|
| `type` | string | Always `"relay_status"` |
| `status` | string | `"ok"` when the relay subsystem is running |
| `count` | int | Number of active relay forward states — messages currently being relayed through this node |
| `limit` | int | Number of connected peers with the `mesh_relay_v1` capability |

`count` reflects transient forwarding state: each message being relayed creates a `relayForwardState` entry that lives for up to 180 seconds (TTL). A high `count` indicates heavy relay traffic; zero means no messages are currently in transit through this node.

`limit` indicates how many outbound peers can participate in relay forwarding. If `limit` is zero, the node has no relay-capable neighbors and relay messages will be dropped (falling back to gossip).

See [../protocol/relay.md](../protocol/relay.md) for the full relay protocol specification.

---

## Русский

### POST /rpc/v1/mesh/relay_status

Диагностика подсистемы ретрансляции. Возвращает текущее состояние hop-by-hop relay — активные forwarding states и количество relay-capable пиров.

Ответ:
```json
{
  "type": "relay_status",
  "status": "ok",
  "count": 5,
  "limit": 3
}
```

| Поле | Тип | Описание |
|---|---|---|
| `type` | string | Всегда `"relay_status"` |
| `status` | string | `"ok"` когда подсистема ретрансляции работает |
| `count` | int | Количество активных relay forward states — сообщения, проходящие через этот узел |
| `limit` | int | Количество подключённых пиров с capability `mesh_relay_v1` |

`count` отражает временное состояние пересылки: каждое ретранслируемое сообщение создаёт запись `relayForwardState` со временем жизни до 180 секунд (TTL). Высокое значение `count` указывает на интенсивный relay-трафик; ноль означает, что через этот узел сейчас не проходят сообщения.

`limit` показывает, сколько исходящих пиров могут участвовать в ретрансляции. Если `limit` равен нулю — у узла нет relay-capable соседей и relay-сообщения будут отброшены (с fallback на gossip).

Подробная спецификация: [../protocol/relay.md](../protocol/relay.md).
