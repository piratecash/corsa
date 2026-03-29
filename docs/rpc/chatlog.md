# Chat History Commands

## English

Available only in desktop mode. In standalone node mode returns 503.

### POST /rpc/v1/chatlog/entries

Fetch chat history entries for a specific peer conversation.

Request: `{"topic": "dm", "peer_address": "address"}`

| Field | Type | Required | Description |
|---|---|---|---|
| `topic` | string | Yes | Message topic (default: `"dm"`) |
| `peer_address` | string | Yes | Transport address of the conversation peer |

#### CLI

```bash
# Positional arguments (topic peer_address)
corsa-cli fetch_chatlog dm 10.0.0.5:64646

# Named arguments
corsa-cli fetch_chatlog topic=dm peer_address=10.0.0.5:64646

# JSON
corsa-cli '{"type": "fetch_chatlog", "topic": "dm", "address": "10.0.0.5:64646"}'
```

Note: JSON wire format uses `address`, the RPC handler expects `peer_address`. The `normalizeFrameArgs` layer maps `address` → `peer_address` automatically when pasting wire frames into the console.

#### Console

```
fetch_chatlog dm 10.0.0.5:64646
```

### POST /rpc/v1/chatlog/previews

Last message from each peer. No arguments.

#### CLI

```bash
corsa-cli fetch_chatlog_previews
```

#### Console

```
fetch_chatlog_previews
```

### POST /rpc/v1/chatlog/conversations

Metadata for all conversations. No arguments.

#### CLI

```bash
corsa-cli fetch_conversations
```

#### Console

```
fetch_conversations
```

---

## Русский

Доступно только в desktop-режиме. В режиме standalone-ноды возвращает 503.

### POST /rpc/v1/chatlog/entries

Получение записей истории чата для конкретного собеседника.

Запрос: `{"topic": "dm", "peer_address": "address"}`

| Поле | Тип | Обязательное | Описание |
|---|---|---|---|
| `topic` | string | Да | Топик сообщений (по умолчанию: `"dm"`) |
| `peer_address` | string | Да | Транспортный адрес собеседника |

#### CLI

```bash
# Позиционные аргументы (topic peer_address)
corsa-cli fetch_chatlog dm 10.0.0.5:64646

# Именованные аргументы
corsa-cli fetch_chatlog topic=dm peer_address=10.0.0.5:64646

# JSON
corsa-cli '{"type": "fetch_chatlog", "topic": "dm", "address": "10.0.0.5:64646"}'
```

Примечание: JSON wire-формат использует `address`, RPC-обработчик ожидает `peer_address`. Слой `normalizeFrameArgs` автоматически маппит `address` → `peer_address` при вставке wire-фреймов в консоль.

#### Консоль

```
fetch_chatlog dm 10.0.0.5:64646
```

### POST /rpc/v1/chatlog/previews

Последнее сообщение от каждого пира. Без аргументов.

#### CLI

```bash
corsa-cli fetch_chatlog_previews
```

#### Консоль

```
fetch_chatlog_previews
```

### POST /rpc/v1/chatlog/conversations

Метаданные всех разговоров. Без аргументов.

#### CLI

```bash
corsa-cli fetch_conversations
```

#### Консоль

```
fetch_conversations
```
