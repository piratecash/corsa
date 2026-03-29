# Message Commands

## English

### POST /rpc/v1/message/list

Fetch messages for a topic.

Request: `{"topic": "global"}`

#### CLI

```bash
# Positional (topic defaults to "global")
corsa-cli fetch_messages
corsa-cli fetch_messages dm

# Named
corsa-cli fetch_messages topic=dm

# JSON
corsa-cli '{"type": "fetch_messages", "topic": "dm"}'
```

#### Console

```
fetch_messages
fetch_messages dm
```

### POST /rpc/v1/message/ids

Fetch message IDs for a topic.

Request: `{"topic": "global"}`

#### CLI

```bash
corsa-cli fetch_message_ids
corsa-cli fetch_message_ids dm
```

#### Console

```
fetch_message_ids
fetch_message_ids dm
```

### POST /rpc/v1/message/get

Fetch a single message by topic and ID.

Request: `{"topic": "dm", "id": "message-uuid"}`

#### CLI

```bash
# Positional (topic id)
corsa-cli fetch_message dm 550e8400-e29b-41d4-a716-446655440001

# Named
corsa-cli fetch_message topic=dm id=550e8400-e29b-41d4-a716-446655440001
```

#### Console

```
fetch_message dm 550e8400-e29b-41d4-a716-446655440001
```

### POST /rpc/v1/message/inbox

Fetch inbox messages for a recipient.

Request: `{"topic": "dm", "recipient": "address (optional, defaults to self)"}`

#### CLI

```bash
# Positional (topic defaults to "dm", recipient optional)
corsa-cli fetch_inbox
corsa-cli fetch_inbox dm a1b2c3d4...

# Named
corsa-cli fetch_inbox topic=dm recipient=a1b2c3d4...
```

#### Console

```
fetch_inbox
fetch_inbox dm a1b2c3d4...
```

### POST /rpc/v1/message/pending

Fetch pending (undelivered) messages.

Request: `{"topic": "dm"}`

#### CLI

```bash
corsa-cli fetch_pending_messages
corsa-cli fetch_pending_messages dm
```

#### Console

```
fetch_pending_messages
fetch_pending_messages dm
```

### POST /rpc/v1/message/receipts

Fetch delivery receipts.

Request: `{"recipient": "address (optional, defaults to self)"}`

#### CLI

```bash
corsa-cli fetch_delivery_receipts
corsa-cli fetch_delivery_receipts a1b2c3d4...
```

#### Console

```
fetch_delivery_receipts
fetch_delivery_receipts a1b2c3d4...
```

### POST /rpc/v1/message/dm_headers

Fetch direct message headers. No arguments.

#### CLI

```bash
corsa-cli fetch_dm_headers
```

#### Console

```
fetch_dm_headers
```

### POST /rpc/v1/message/send_dm

Queue a direct message for delivery (desktop mode only, returns 503 on standalone node).

Request: `{"to": "address", "body": "message text"}`

Response: `{"status": "queued", "to": "address"}`. The message is accepted for async delivery via DMRouter. Actual delivery happens in a background goroutine — check delivery receipts for confirmation.

#### CLI

```bash
# Positional (to body...)
corsa-cli send_dm a1b2c3d4... hello world

# Named
corsa-cli send_dm to=a1b2c3d4... body="hello world"

# JSON
corsa-cli '{"type": "send_dm", "to": "a1b2c3d4...", "body": "hello world"}'
```

Note: JSON wire format uses `recipient`, the RPC handler expects `to`. The `normalizeFrameArgs` layer maps `recipient` → `to` automatically when pasting wire frames into the console.

#### Console

```
send_dm a1b2c3d4... hello world
```

---

## Русский

### POST /rpc/v1/message/list

Получение сообщений по топику.

Запрос: `{"topic": "global"}`

#### CLI

```bash
# Позиционные (topic по умолчанию "global")
corsa-cli fetch_messages
corsa-cli fetch_messages dm

# Именованные
corsa-cli fetch_messages topic=dm
```

#### Консоль

```
fetch_messages
fetch_messages dm
```

### POST /rpc/v1/message/ids

Получение ID сообщений по топику.

Запрос: `{"topic": "global"}`

#### CLI

```bash
corsa-cli fetch_message_ids
corsa-cli fetch_message_ids dm
```

#### Консоль

```
fetch_message_ids
fetch_message_ids dm
```

### POST /rpc/v1/message/get

Получение одного сообщения по топику и ID.

Запрос: `{"topic": "dm", "id": "message-uuid"}`

#### CLI

```bash
# Позиционные (topic id)
corsa-cli fetch_message dm 550e8400-e29b-41d4-a716-446655440001

# Именованные
corsa-cli fetch_message topic=dm id=550e8400-e29b-41d4-a716-446655440001
```

#### Консоль

```
fetch_message dm 550e8400-e29b-41d4-a716-446655440001
```

### POST /rpc/v1/message/inbox

Получение входящих сообщений для получателя.

Запрос: `{"topic": "dm", "recipient": "address (опционально, по умолчанию — свой)"}`

#### CLI

```bash
# Позиционные (topic по умолчанию "dm", recipient опционально)
corsa-cli fetch_inbox
corsa-cli fetch_inbox dm a1b2c3d4...

# Именованные
corsa-cli fetch_inbox topic=dm recipient=a1b2c3d4...
```

#### Консоль

```
fetch_inbox
fetch_inbox dm a1b2c3d4...
```

### POST /rpc/v1/message/pending

Получение ожидающих доставки сообщений.

Запрос: `{"topic": "dm"}`

#### CLI

```bash
corsa-cli fetch_pending_messages
corsa-cli fetch_pending_messages dm
```

#### Консоль

```
fetch_pending_messages
fetch_pending_messages dm
```

### POST /rpc/v1/message/receipts

Получение квитанций доставки.

Запрос: `{"recipient": "address (опционально, по умолчанию — свой)"}`

#### CLI

```bash
corsa-cli fetch_delivery_receipts
corsa-cli fetch_delivery_receipts a1b2c3d4...
```

#### Консоль

```
fetch_delivery_receipts
fetch_delivery_receipts a1b2c3d4...
```

### POST /rpc/v1/message/dm_headers

Получение заголовков прямых сообщений. Без аргументов.

#### CLI

```bash
corsa-cli fetch_dm_headers
```

#### Консоль

```
fetch_dm_headers
```

### POST /rpc/v1/message/send_dm

Постановка прямого сообщения в очередь доставки (только desktop-режим, возвращает 503 на standalone-ноде).

Запрос: `{"to": "address", "body": "текст сообщения"}`

Ответ: `{"status": "queued", "to": "address"}`. Сообщение принято для асинхронной доставки через DMRouter. Фактическая отправка происходит в фоновой goroutine — используйте delivery receipts для подтверждения.

#### CLI

```bash
# Позиционные (to body...)
corsa-cli send_dm a1b2c3d4... hello world

# Именованные
corsa-cli send_dm to=a1b2c3d4... body="hello world"

# JSON
corsa-cli '{"type": "send_dm", "to": "a1b2c3d4...", "body": "hello world"}'
```

Примечание: JSON wire-формат использует `recipient`, RPC-обработчик ожидает `to`. Слой `normalizeFrameArgs` автоматически маппит `recipient` → `to` при вставке wire-фреймов в консоль.

#### Консоль

```
send_dm a1b2c3d4... hello world
```
