# Message Commands

## English

### POST /rpc/v1/message/list

Fetch messages for a topic.

Request: `{"topic": "global"}`

#### CLI

```bash
# Positional (topic defaults to "global")
corsa-cli fetchMessages
corsa-cli fetchMessages dm

# Named
corsa-cli fetchMessages topic=dm

# JSON
corsa-cli '{"type": "fetch_messages", "topic": "dm"}'
```

#### Console

```
fetchMessages
fetchMessages dm
```

### POST /rpc/v1/message/ids

Fetch message IDs for a topic.

Request: `{"topic": "global"}`

#### CLI

```bash
corsa-cli fetchMessageIds
corsa-cli fetchMessageIds dm
```

#### Console

```
fetchMessageIds
fetchMessageIds dm
```

### POST /rpc/v1/message/get

Fetch a single message by topic and ID.

Request: `{"topic": "dm", "id": "message-uuid"}`

#### CLI

```bash
# Positional (topic id)
corsa-cli fetchMessage dm 550e8400-e29b-41d4-a716-446655440001

# Named
corsa-cli fetchMessage topic=dm id=550e8400-e29b-41d4-a716-446655440001
```

#### Console

```
fetchMessage dm 550e8400-e29b-41d4-a716-446655440001
```

### POST /rpc/v1/message/inbox

Fetch inbox messages for a recipient.

Request: `{"topic": "dm", "recipient": "address (optional, defaults to self)"}`

#### CLI

```bash
# Positional (topic defaults to "dm", recipient optional)
corsa-cli fetchInbox
corsa-cli fetchInbox dm a1b2c3d4...

# Named
corsa-cli fetchInbox topic=dm recipient=a1b2c3d4...
```

#### Console

```
fetchInbox
fetchInbox dm a1b2c3d4...
```

### POST /rpc/v1/message/pending

Fetch pending (undelivered) messages.

Request: `{"topic": "dm"}`

#### CLI

```bash
corsa-cli fetchPendingMessages
corsa-cli fetchPendingMessages dm
```

#### Console

```
fetchPendingMessages
fetchPendingMessages dm
```

### POST /rpc/v1/message/receipts

Fetch delivery receipts.

Request: `{"recipient": "address (optional, defaults to self)"}`

#### CLI

```bash
corsa-cli fetchDeliveryReceipts
corsa-cli fetchDeliveryReceipts a1b2c3d4...
```

#### Console

```
fetchDeliveryReceipts
fetchDeliveryReceipts a1b2c3d4...
```

### POST /rpc/v1/message/dm_headers

Fetch direct message headers. No arguments.

#### CLI

```bash
corsa-cli fetchDmHeaders
```

#### Console

```
fetchDmHeaders
```

### POST /rpc/v1/message/send_dm

Queue a direct message for delivery (desktop mode only, returns 503 on standalone node).

Request: `{"to": "address", "body": "message text", "reply_to": "<uuid-v4>"}` (`reply_to` is optional — omit for regular messages, provide a message ID to create a reply).

Validation (synchronous, returns 400 on failure):

- `to` and `body` are required (non-empty strings).
- `reply_to` must be a valid UUID v4 string when present.
- When chatlog is available (desktop mode), `reply_to` is checked against the conversation history — referencing a non-existent message is rejected before queueing.

Response: `{"status": "pending", "to": "address"}`. The message is accepted for async delivery via DMRouter. Actual delivery happens in a background goroutine — check delivery receipts for confirmation.

#### CLI

```bash
# Positional (to body...) — reply_to is not available in positional mode
corsa-cli sendDm a1b2c3d4... hello world

# Named
corsa-cli sendDm to=a1b2c3d4... body="hello world"

# Named with reply
corsa-cli sendDm to=a1b2c3d4... body="I agree" reply_to=e4a7c391-5f02-4b8a-9d1e-0f3a6b7c8d2e

# JSON (command name first, single JSON argument with fields)
corsa-cli sendDm '{"to": "a1b2c3d4...", "body": "hello world"}'

# JSON with reply
corsa-cli sendDm '{"to": "a1b2c3d4...", "body": "I agree", "reply_to": "e4a7c391-5f02-4b8a-9d1e-0f3a6b7c8d2e"}'
```

Note: JSON wire format uses `recipient`, the RPC handler expects `to`. The `normalizeFrameArgs` layer maps `recipient` → `to` automatically when pasting wire frames into the console. The `reply_to` field is only available via named args or JSON — positional mode joins all tokens after `<to>` into `body`. The `reply_to` value is encrypted inside the PlainMessage envelope — the relay server never sees it.

#### Console

```
sendDm a1b2c3d4... hello world
```

---

## Русский

### POST /rpc/v1/message/list

Получение сообщений по топику.

Запрос: `{"topic": "global"}`

#### CLI

```bash
# Позиционные (topic по умолчанию "global")
corsa-cli fetchMessages
corsa-cli fetchMessages dm

# Именованные
corsa-cli fetchMessages topic=dm
```

#### Консоль

```
fetchMessages
fetchMessages dm
```

### POST /rpc/v1/message/ids

Получение ID сообщений по топику.

Запрос: `{"topic": "global"}`

#### CLI

```bash
corsa-cli fetchMessageIds
corsa-cli fetchMessageIds dm
```

#### Консоль

```
fetchMessageIds
fetchMessageIds dm
```

### POST /rpc/v1/message/get

Получение одного сообщения по топику и ID.

Запрос: `{"topic": "dm", "id": "message-uuid"}`

#### CLI

```bash
# Позиционные (topic id)
corsa-cli fetchMessage dm 550e8400-e29b-41d4-a716-446655440001

# Именованные
corsa-cli fetchMessage topic=dm id=550e8400-e29b-41d4-a716-446655440001
```

#### Консоль

```
fetchMessage dm 550e8400-e29b-41d4-a716-446655440001
```

### POST /rpc/v1/message/inbox

Получение входящих сообщений для получателя.

Запрос: `{"topic": "dm", "recipient": "address (опционально, по умолчанию — свой)"}`

#### CLI

```bash
# Позиционные (topic по умолчанию "dm", recipient опционально)
corsa-cli fetchInbox
corsa-cli fetchInbox dm a1b2c3d4...

# Именованные
corsa-cli fetchInbox topic=dm recipient=a1b2c3d4...
```

#### Консоль

```
fetchInbox
fetchInbox dm a1b2c3d4...
```

### POST /rpc/v1/message/pending

Получение ожидающих доставки сообщений.

Запрос: `{"topic": "dm"}`

#### CLI

```bash
corsa-cli fetchPendingMessages
corsa-cli fetchPendingMessages dm
```

#### Консоль

```
fetchPendingMessages
fetchPendingMessages dm
```

### POST /rpc/v1/message/receipts

Получение квитанций доставки.

Запрос: `{"recipient": "address (опционально, по умолчанию — свой)"}`

#### CLI

```bash
corsa-cli fetchDeliveryReceipts
corsa-cli fetchDeliveryReceipts a1b2c3d4...
```

#### Консоль

```
fetchDeliveryReceipts
fetchDeliveryReceipts a1b2c3d4...
```

### POST /rpc/v1/message/dm_headers

Получение заголовков прямых сообщений. Без аргументов.

#### CLI

```bash
corsa-cli fetchDmHeaders
```

#### Консоль

```
fetchDmHeaders
```

### POST /rpc/v1/message/send_dm

Постановка прямого сообщения в очередь доставки (только desktop-режим, возвращает 503 на standalone-ноде).

Запрос: `{"to": "address", "body": "текст сообщения", "reply_to": "<uuid-v4>"}` (`reply_to` — необязательное поле; пропустите для обычных сообщений, укажите ID сообщения для создания ответа).

Валидация (синхронная, возвращает 400 при ошибке):

- `to` и `body` обязательны (непустые строки).
- `reply_to` должен быть валидным UUID v4, если указан.
- При наличии chatlog (desktop-режим) `reply_to` проверяется по истории переписки — ссылка на несуществующее сообщение отклоняется до постановки в очередь.

Ответ: `{"status": "pending", "to": "address"}`. Сообщение принято для асинхронной доставки через DMRouter. Фактическая отправка происходит в фоновой goroutine — используйте delivery receipts для подтверждения.

#### CLI

```bash
# Позиционные (to body...) — reply_to недоступен в позиционном режиме
corsa-cli sendDm a1b2c3d4... hello world

# Именованные
corsa-cli sendDm to=a1b2c3d4... body="hello world"

# Именованные с ответом
corsa-cli sendDm to=a1b2c3d4... body="Согласен" reply_to=e4a7c391-5f02-4b8a-9d1e-0f3a6b7c8d2e

# JSON (имя команды первым, затем один JSON-аргумент с полями)
corsa-cli sendDm '{"to": "a1b2c3d4...", "body": "hello world"}'

# JSON с ответом
corsa-cli sendDm '{"to": "a1b2c3d4...", "body": "Согласен", "reply_to": "e4a7c391-5f02-4b8a-9d1e-0f3a6b7c8d2e"}'
```

Примечание: JSON wire-формат использует `recipient`, RPC-обработчик ожидает `to`. Слой `normalizeFrameArgs` автоматически маппит `recipient` → `to` при вставке wire-фреймов в консоль. Поле `reply_to` доступно только через именованные аргументы или JSON — в позиционном режиме все токены после `<to>` объединяются в `body`. Значение `reply_to` шифруется внутри PlainMessage-конверта — relay-сервер его не видит.

#### Консоль

```
sendDm a1b2c3d4... hello world
```
