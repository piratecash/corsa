# RPC Commands

## English

Per-command-group documentation for the CORSA RPC layer. For architecture overview, configuration, CLI usage, and integration guide see the parent [rpc.md](../rpc.md).

### Command Index

| Group | Commands | File |
|---|---|---|
| [System](system.md) | `help`, `ping`, `hello`, `version` | [system.md](system.md) |
| [Network](network.md) | `get_peers`, `fetch_peer_health`, `fetch_network_stats`, `add_peer` | [network.md](network.md) |
| [Identity](identity.md) | `fetch_identities`, `fetch_contacts`, `fetch_trusted_contacts` | [identity.md](identity.md) |
| [Message](message.md) | `fetch_messages`, `fetch_message_ids`, `fetch_message`, `fetch_inbox`, `fetch_pending_messages`, `fetch_delivery_receipts`, `fetch_dm_headers`, `send_dm` | [message.md](message.md) |
| [File Transfer](file.md) | `send_file_announce`, `fetch_file_transfers`, `fetch_file_mapping`, `retry_file_chunk`, `start_file_download`, `cancel_file_download` | [file.md](file.md) |
| [Chatlog](chatlog.md) | `fetch_chatlog`, `fetch_chatlog_previews`, `fetch_conversations` | [chatlog.md](chatlog.md) |
| [Notice](notice.md) | `fetch_notices` | [notice.md](notice.md) |
| [Mesh](mesh.md) | `fetch_relay_status` | [mesh.md](mesh.md) |
| [Metrics](metrics.md) | `fetch_traffic_history` | [metrics.md](metrics.md) |
| [Routing](routing.md) | `fetch_route_table`, `fetch_route_summary`, `fetch_route_lookup` | [routing.md](routing.md) |

### Universal Dispatch

**POST /rpc/v1/exec** — execute any registered command.

Request:
```json
{"command": "ping", "args": {}}
```

Response: command-specific JSON.

### Raw Frame Dispatch

**POST /rpc/v1/frame** — accept a raw JSON protocol frame and dispatch it through `CommandTable`, falling back to `HandleLocalFrame` for unregistered frame types.

Request: a raw protocol frame JSON object with a `type` field:
```json
{"type": "fetch_chatlog", "topic": "dm", "address": "peer-addr-123"}
```

Response: the command's response (JSON).

Dispatch logic:

1. The frame's `type` is extracted as the command name, wire field names are normalized to RPC arg names via `normalizeFrameArgs`.
2. If the command is registered in `CommandTable`, it is dispatched there.
3. If the command is not found in `CommandTable`, the raw frame is forwarded to `HandleLocalFrame` with all caller-supplied wire fields preserved.

This endpoint is only available when the server is created with a `NodeProvider`.

---

## Русский

Документация по группам команд RPC-слоя CORSA. Обзор архитектуры, конфигурация, CLI и интеграция — в родительском [rpc.md](../rpc.md).

### Индекс команд

| Группа | Команды | Файл |
|---|---|---|
| [Системные](system.md) | `help`, `ping`, `hello`, `version` | [system.md](system.md) |
| [Сеть](network.md) | `get_peers`, `fetch_peer_health`, `fetch_network_stats`, `add_peer` | [network.md](network.md) |
| [Идентификация](identity.md) | `fetch_identities`, `fetch_contacts`, `fetch_trusted_contacts` | [identity.md](identity.md) |
| [Сообщения](message.md) | `fetch_messages`, `fetch_message_ids`, `fetch_message`, `fetch_inbox`, `fetch_pending_messages`, `fetch_delivery_receipts`, `fetch_dm_headers`, `send_dm` | [message.md](message.md) |
| [Файловый трансфер](file.md) | `send_file_announce`, `fetch_file_transfers`, `fetch_file_mapping`, `retry_file_chunk`, `start_file_download`, `cancel_file_download` | [file.md](file.md) |
| [История чатов](chatlog.md) | `fetch_chatlog`, `fetch_chatlog_previews`, `fetch_conversations` | [chatlog.md](chatlog.md) |
| [Уведомления](notice.md) | `fetch_notices` | [notice.md](notice.md) |
| [Mesh](mesh.md) | `fetch_relay_status` | [mesh.md](mesh.md) |
| [Метрики](metrics.md) | `fetch_traffic_history` | [metrics.md](metrics.md) |
| [Маршрутизация](routing.md) | `fetch_route_table`, `fetch_route_summary`, `fetch_route_lookup` | [routing.md](routing.md) |

### Универсальная диспетчеризация

**POST /rpc/v1/exec** — выполнение любой зарегистрированной команды.

Запрос:
```json
{"command": "ping", "args": {}}
```

Ответ: JSON, специфичный для команды.

### Диспетчеризация фреймов

**POST /rpc/v1/frame** — принимает сырой JSON-фрейм протокола и диспетчеризует его через `CommandTable`, с fallback на `HandleLocalFrame` для незарегистрированных типов фреймов.

Запрос: сырой JSON-объект фрейма протокола с полем `type`:
```json
{"type": "fetch_chatlog", "topic": "dm", "address": "peer-addr-123"}
```

Ответ: JSON ответа команды.

Логика диспетчеризации:

1. Из фрейма извлекается `type` как имя команды, wire-имена полей нормализуются в RPC-аргументы через `normalizeFrameArgs`.
2. Если команда зарегистрирована в `CommandTable`, она диспетчеризуется туда.
3. Если команда не найдена в `CommandTable`, сырой фрейм пересылается в `HandleLocalFrame` с сохранением всех wire-полей отправителя.

Эндпоинт доступен только когда сервер создан с `NodeProvider`.
