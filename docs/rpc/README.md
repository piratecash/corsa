# RPC Commands

## English

Per-command-group documentation for the CORSA RPC layer. For architecture overview, configuration, CLI usage, and integration guide see the parent [rpc.md](../rpc.md).

### Command Index

| Group | Commands | File |
|---|---|---|
| [System](system.md) | `help`, `ping`, `hello`, `version` | [system.md](system.md) |
| [Network](network.md) | `getPeers`, `fetchPeerHealth`, `fetchNetworkStats`, `addPeer`, `fetchReachableIds` | [network.md](network.md) |
| [Identity](identity.md) | `fetchIdentities`, `fetchContacts`, `fetchTrustedContacts`, `deleteTrustedContact`, `importContacts` | [identity.md](identity.md) |
| [Message](message.md) | `fetchMessages`, `fetchMessageIds`, `fetchMessage`, `fetchInbox`, `fetchPendingMessages`, `fetchDeliveryReceipts`, `fetchDmHeaders`, `sendDm`, `sendMessage`, `importMessage`, `sendDeliveryReceipt` | [message.md](message.md) |
| [File Transfer](file.md) | `sendFileAnnounce`, `fetchFileTransfers`, `fetchFileMapping`, `retryFileChunk`, `startFileDownload`, `cancelFileDownload` | [file.md](file.md) |
| [Chatlog](chatlog.md) | `fetchChatlog`, `fetchChatlogPreviews`, `fetchConversations` | [chatlog.md](chatlog.md) |
| [Notice](notice.md) | `fetchNotices`, `publishNotice` | [notice.md](notice.md) |
| [Mesh](mesh.md) | `fetchRelayStatus` | [mesh.md](mesh.md) |
| [Metrics](metrics.md) | `fetchTrafficHistory` | [metrics.md](metrics.md) |
| [Routing](routing.md) | `fetchRouteTable`, `fetchRouteSummary`, `fetchRouteLookup` | [routing.md](routing.md) |
### Universal Dispatch

**POST /rpc/v1/exec** — execute any registered command.

Request:
```json
{"command": "ping", "args": {}}
```

Response: command-specific JSON.

### Raw Frame Dispatch

**POST /rpc/v1/frame** — accept a raw JSON protocol frame and dispatch it through `CommandTable`. Only registered frame types are accepted; unregistered types are rejected with 400 Bad Request.

Request: a raw protocol frame JSON object with a `type` field:
```json
{"type": "fetch_chatlog", "topic": "dm", "address": "peer-addr-123"}
```

Response: the command's response (JSON).

Dispatch logic:

1. The frame's `type` is extracted as the command name, wire field names are normalized to RPC arg names via `normalizeFrameArgs`.
2. If the command is registered in `CommandTable`, it is dispatched there.
3. If the command is not found in `CommandTable`, the request is rejected with 400 Bad Request (`unknown frame type: <name>`).

This endpoint is only available when the server is created with a `NodeProvider`.

---

## Русский

Документация по группам команд RPC-слоя CORSA. Обзор архитектуры, конфигурация, CLI и интеграция — в родительском [rpc.md](../rpc.md).

### Индекс команд

| Группа | Команды | Файл |
|---|---|---|
| [Системные](system.md) | `help`, `ping`, `hello`, `version` | [system.md](system.md) |
| [Сеть](network.md) | `getPeers`, `fetchPeerHealth`, `fetchNetworkStats`, `addPeer`, `fetchReachableIds` | [network.md](network.md) |
| [Идентификация](identity.md) | `fetchIdentities`, `fetchContacts`, `fetchTrustedContacts`, `deleteTrustedContact`, `importContacts` | [identity.md](identity.md) |
| [Сообщения](message.md) | `fetchMessages`, `fetchMessageIds`, `fetchMessage`, `fetchInbox`, `fetchPendingMessages`, `fetchDeliveryReceipts`, `fetchDmHeaders`, `sendDm`, `sendMessage`, `importMessage`, `sendDeliveryReceipt` | [message.md](message.md) |
| [Файловый трансфер](file.md) | `sendFileAnnounce`, `fetchFileTransfers`, `fetchFileMapping`, `retryFileChunk`, `startFileDownload`, `cancelFileDownload` | [file.md](file.md) |
| [История чатов](chatlog.md) | `fetchChatlog`, `fetchChatlogPreviews`, `fetchConversations` | [chatlog.md](chatlog.md) |
| [Уведомления](notice.md) | `fetchNotices`, `publishNotice` | [notice.md](notice.md) |
| [Mesh](mesh.md) | `fetchRelayStatus` | [mesh.md](mesh.md) |
| [Метрики](metrics.md) | `fetchTrafficHistory` | [metrics.md](metrics.md) |
| [Маршрутизация](routing.md) | `fetchRouteTable`, `fetchRouteSummary`, `fetchRouteLookup` | [routing.md](routing.md) |
### Универсальная диспетчеризация

**POST /rpc/v1/exec** — выполнение любой зарегистрированной команды.

Запрос:
```json
{"command": "ping", "args": {}}
```

Ответ: JSON, специфичный для команды.

### Диспетчеризация фреймов

**POST /rpc/v1/frame** — принимает сырой JSON-фрейм протокола и диспетчеризует его через `CommandTable`. Только зарегистрированные типы фреймов принимаются; незарегистрированные типы отклоняются с 400 Bad Request.

Запрос: сырой JSON-объект фрейма протокола с полем `type`:
```json
{"type": "fetch_chatlog", "topic": "dm", "address": "peer-addr-123"}
```

Ответ: JSON ответа команды.

Логика диспетчеризации:

1. Из фрейма извлекается `type` как имя команды, wire-имена полей нормализуются в RPC-аргументы через `normalizeFrameArgs`.
2. Если команда зарегистрирована в `CommandTable`, она диспетчеризуется туда.
3. Если команда не найдена в `CommandTable`, запрос отклоняется с 400 Bad Request (`unknown frame type: <name>`).

Эндпоинт доступен только когда сервер создан с `NodeProvider`.
