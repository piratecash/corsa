# CORSA Protocol

## English

Related crypto documentation:

- [encryption.md](encryption.md)

### Status

- version: `v8`
- stability: experimental
- transport: plain TCP
- framing: line-based UTF-8 terminated by `\n`
- primary frame format: JSON object per line
- identity: `ed25519`
- message encryption: `X25519 + AES-GCM`
- direct-message signatures: `ed25519`

### Network config

Relevant environment variables:

- `CORSA_LISTEN_ADDRESS`
- `CORSA_ADVERTISE_ADDRESS`
- `CORSA_BOOTSTRAP_PEER`
- `CORSA_BOOTSTRAP_PEERS`
- `CORSA_IDENTITY_PATH`
- `CORSA_TRUST_STORE_PATH`
- `CORSA_NODE_TYPE`
- `CORSA_MAX_CLOCK_DRIFT_SECONDS`

Rules:

- `CORSA_BOOTSTRAP_PEERS` overrides `CORSA_BOOTSTRAP_PEER`
- default seed if nothing is set: `65.108.204.190:64646`
- `CORSA_NODE_TYPE=full` enables relay/forwarding
- `CORSA_NODE_TYPE=client` disables forwarding but keeps sync, inbox, and local storage
- default message clock drift: `600` seconds

### Handshake

Primary JSON desktop request:

```json
{
  "type": "hello",
  "version": 1,
  "client": "desktop",
  "client_version": "0.3-alpha"
}
```

Fields:

- `type` — required; frame kind, here always `hello`
- `version` — required; protocol frame version
- `client` — required; caller kind such as `desktop` or `node`
- `client_version` — optional; app/client build version in wire form

Primary JSON node-to-node request:

```json
{
  "type": "hello",
  "version": 1,
  "client": "node",
  "listen": "203.0.113.10:64646",
  "node_type": "full",
  "client_version": "0.3-alpha",
  "services": [
    "identity",
    "contacts",
    "messages",
    "gazeta",
    "relay"
  ],
  "address": "<fingerprint>",
  "pubkey": "<base64-ed25519-pubkey>",
  "boxkey": "<base64-x25519-pubkey>",
  "boxsig": "<base64url-ed25519-signature>"
}
```

Fields:

- `type` — required; frame kind, here `hello`
- `version` — required; protocol frame version
- `client` — required; caller kind, here `node`
- `listen` — optional; address this node advertises to peers
- `node_type` — optional; node role, currently `full` or `client`
- `client_version` — optional; node software version
- `services` — optional; declared capabilities supported by the node
- `address` — optional; node identity fingerprint
- `pubkey` — optional; base64 `ed25519` public key for identity/authenticity
- `boxkey` — optional; base64 `X25519` public key for encrypted traffic
- `boxsig` — optional; signature binding `boxkey` to the identity key

Response:

```json
{
  "type": "welcome",
  "version": 1,
  "node": "corsa",
  "network": "gazeta-devnet",
  "node_type": "full",
  "client_version": "0.3-alpha",
  "services": [
    "identity",
    "contacts",
    "messages",
    "gazeta",
    "relay"
  ],
  "address": "<fingerprint>",
  "pubkey": "<base64-ed25519-pubkey>",
  "boxkey": "<base64-x25519-pubkey>",
  "boxsig": "<base64url-ed25519-signature>"
}
```

Fields:

- `type` — required; frame kind, here `welcome`
- `version` — required; protocol frame version
- `node` — required; server implementation name
- `network` — required; logical network name
- `node_type` — optional; responder role
- `client_version` — optional; responder software version
- `services` — optional; responder capabilities
- `address` — optional; responder fingerprint identity
- `pubkey` — optional; responder identity key
- `boxkey` — optional; responder encryption key
- `boxsig` — optional; signature proving the encryption key belongs to the responder identity

Role rules:

- `full` nodes forward mesh traffic
- `client` nodes do not relay traffic onward
- desktop and standalone console node default to `full`
- future mobile/light clients should use `client`
- current client version: `0.3 alpha`
- wire form used in handshake: `0.3-alpha`

### Peer sync

Request:

```json
{
  "type": "get_peers"
}
```

Response:

```json
{
  "type": "peers",
  "count": 2,
  "peers": [
    "127.0.0.1:64646",
    "127.0.0.1:64647"
  ]
}
```

Fields:

- `type` — required; frame kind, here `peers`
- `count` — required; number of peer addresses in `peers`
- `peers` — required; advertised peer endpoints

### Contacts

Request:

```json
{
  "type": "fetch_contacts"
}
```

Response:

```json
{
  "type": "contacts",
  "count": 2,
  "contacts": [
    {
      "address": "<address1>",
      "boxkey": "<boxkey1>",
      "pubkey": "<pubkey1>",
      "boxsig": "<boxsig1>"
    },
    {
      "address": "<address2>",
      "boxkey": "<boxkey2>",
      "pubkey": "<pubkey2>",
      "boxsig": "<boxsig2>"
    }
  ]
}
```

Fields:

- `type` — required; frame kind, here `contacts`
- `count` — required; number of contact records
- `contacts` — required; array of known contact identities
- `contacts[].address` — required; fingerprint identity of the contact
- `contacts[].pubkey` — required; contact `ed25519` identity key
- `contacts[].boxkey` — required; contact `X25519` encryption key
- `contacts[].boxsig` — required; signature binding the contact encryption key to its identity key

Trust rules:

- `pubkey` must hash to the advertised fingerprint address
- `boxkey` must be signed by that identity key
- the first valid key set is pinned locally
- later conflicting keys are ignored

### Direct messages

Primary JSON request:

```json
{
  "type": "send_message",
  "topic": "dm",
  "id": "550e8400-e29b-41d4-a716-446655440001",
  "address": "a1b2c3",
  "recipient": "d4e5f6",
  "flag": "sender-delete",
  "created_at": "2026-03-19T12:00:05Z",
  "ttl_seconds": 0,
  "body": "<ciphertext-token>"
}
```

Fields:

- `type` — required; frame kind, here `send_message`
- `topic` — required; logical message channel such as `global` or `dm`
- `id` — required; UUID of the message
- `address` — required; sender fingerprint
- `recipient` — required; target fingerprint or `*` for broadcast
- `flag` — required; delete/retention rule for the message
- `created_at` — required; sender timestamp in RFC3339 UTC form
- `ttl_seconds` — optional; TTL used by `auto-delete-ttl`, otherwise `0`
- `body` — required; plaintext for public topics or ciphertext for `dm`

Responses:

```json
{
  "type": "message_stored",
  "topic": "dm",
  "count": 1,
  "id": "550e8400-e29b-41d4-a716-446655440001"
}
{
  "type": "message_known",
  "topic": "dm",
  "count": 1,
  "id": "550e8400-e29b-41d4-a716-446655440001"
}
{
  "type": "error",
  "code": "message-timestamp-out-of-range"
}
```

Message log request:

```json
{
  "type": "fetch_messages",
  "topic": "dm"
}
```

Response:

```json
{
  "type": "messages",
  "topic": "dm",
  "count": 1,
  "messages": [
    {
      "id": "<id>",
      "flag": "sender-delete",
      "created_at": "2026-03-19T12:00:05Z",
      "ttl_seconds": 0,
      "sender": "a1b2c3",
      "recipient": "d4e5f6",
      "body": "<ciphertext-token>"
    }
  ]
}
```

Fields:

- `type` — required; frame kind, here `messages`
- `topic` — required; requested topic
- `count` — required; number of returned messages
- `messages` — required; array of message objects
- `messages[].id` — required; UUID of the message
- `messages[].sender` — required; sender fingerprint
- `messages[].recipient` — required; recipient fingerprint or `*`
- `messages[].flag` — required; retention/delete rule
- `messages[].created_at` — required; original sender timestamp
- `messages[].ttl_seconds` — optional; auto-delete TTL if used
- `messages[].body` — required; message payload or ciphertext

Message UUID index request:

```json
{
  "type": "fetch_message_ids",
  "topic": "dm"
}
```

Response:

```json
{
  "type": "message_ids",
  "topic": "dm",
  "count": 2,
  "ids": [
    "uuid1",
    "uuid2"
  ]
}
```

Fields:

- `type` — required; frame kind, here `message_ids`
- `topic` — required; requested topic
- `count` — required; number of UUIDs in the list
- `ids` — required; list of message UUIDs only, for lightweight sync

Single message request:

```json
{
  "type": "fetch_message",
  "topic": "dm",
  "id": "uuid1"
}
```

Response:

```json
{
  "type": "message",
  "topic": "dm",
  "id": "uuid1",
  "item": {
    "id": "uuid1",
    "flag": "sender-delete",
    "created_at": "2026-03-19T12:00:05Z",
    "ttl_seconds": 0,
    "sender": "a1b2c3",
    "recipient": "d4e5f6",
    "body": "<ciphertext-token>"
  }
}
```

Fields:

- `type` — required; frame kind, here `message`
- `topic` — required; requested topic
- `id` — required; requested UUID
- `item` — optional; full message object for that UUID

Inbox request:

```json
{
  "type": "fetch_inbox",
  "topic": "dm",
  "recipient": "d4e5f6"
}
```

Response:

```json
{
  "type": "inbox",
  "topic": "dm",
  "recipient": "d4e5f6",
  "count": 1,
  "messages": [
    {
      "id": "<id>",
      "flag": "sender-delete",
      "created_at": "2026-03-19T12:00:05Z",
      "ttl_seconds": 0,
      "sender": "a1b2c3",
      "recipient": "d4e5f6",
      "body": "<ciphertext-token>"
    }
  ]
}
```

Fields:

- `type` — required; frame kind, here `inbox`
- `topic` — required; requested topic
- `recipient` — required; identity for which the inbox view was filtered
- `count` — required; number of visible messages
- `messages` — required; filtered message array readable by this inbox query

Notes:

- for `dm`, `<body>` is ciphertext
- messages outside the allowed clock drift are rejected and not forwarded
- `auto-delete-ttl` messages are removed after `ttl-seconds`
- `fetch_message_ids` can be used as a lightweight direct-message index
- `fetch_message` can be used to load one DM by UUID

Message flags:

- `immutable` — nobody may delete the message
- `sender-delete` — only the sender may delete it
- `any-delete` — any participant may delete it
- `auto-delete-ttl` — the message is deleted automatically using `ttl-seconds`
- nodes verify `dm` signatures before store/relay
- desktops verify signatures again before decrypt/render

### Gazeta

Publish request:

```json
{
  "type": "publish_notice",
  "ttl_seconds": 30,
  "ciphertext": "<ciphertext-token>"
}
```

Fields:

- `type` — required; frame kind, here `publish_notice`
- `ttl_seconds` — required; notice lifetime in seconds
- `ciphertext` — required; encrypted Gazeta payload

Responses:

```json
{
  "type": "notice_stored",
  "id": "<notice-id>",
  "expires_at": 1234567890
}
{
  "type": "notice_known",
  "id": "<notice-id>",
  "expires_at": 1234567890
}
```

Fetch request:

```json
{
  "type": "fetch_notices"
}
```

Response:

```json
{
  "type": "notices",
  "count": 1,
  "notices": [
    {
      "id": "<id>",
      "expires_at": 1234567890,
      "ciphertext": "<ciphertext-token>"
    }
  ]
}
```

Fields:

- `type` — required; frame kind, here `notices`
- `count` — required; number of active notices
- `notices` — required; array of active encrypted notices
- `notices[].id` — required; notice identifier derived by the node
- `notices[].expires_at` — required; expiration time as Unix seconds
- `notices[].ciphertext` — required; encrypted notice payload

### Errors

Possible JSON error codes:

```json
{
  "type": "error",
  "code": "unknown-command"
}
{
  "type": "error",
  "code": "invalid-send-message"
}
{
  "type": "error",
  "code": "invalid-fetch-messages"
}
{
  "type": "error",
  "code": "invalid-fetch-message-ids"
}
{
  "type": "error",
  "code": "invalid-fetch-message"
}
{
  "type": "error",
  "code": "invalid-fetch-inbox"
}
{
  "type": "error",
  "code": "invalid-publish-notice"
}
{
  "type": "error",
  "code": "unknown-sender-key"
}
{
  "type": "error",
  "code": "unknown-message-id"
}
{
  "type": "error",
  "code": "invalid-direct-message-signature"
}
{
  "type": "error",
  "code": "read"
}
```

Fields:

- `type` — required; frame kind, here `error`
- `code` — required; stable machine-readable error code
- `error` — optional; human-readable detail when available

### Current desktop flow

1. load/create identity
2. load/create trust store
3. start embedded local node
4. sync peers and contacts
5. fetch contacts
6. fetch topic traffic
7. fetch and decrypt readable direct messages
8. fetch Gazeta notices

---

## Русский

Связанная криптографическая документация:

- [encryption.md](encryption.md)

### Статус

- версия: `v8`
- стабильность: experimental
- транспорт: plain TCP
- фрейминг: line-based UTF-8 с окончанием `\n`
- основной формат кадра: JSON-объект на строку
- identity: `ed25519`
- шифрование сообщений: `X25519 + AES-GCM`
- подписи direct messages: `ed25519`

### Сетевой конфиг

Основные переменные окружения:

- `CORSA_LISTEN_ADDRESS`
- `CORSA_ADVERTISE_ADDRESS`
- `CORSA_BOOTSTRAP_PEER`
- `CORSA_BOOTSTRAP_PEERS`
- `CORSA_IDENTITY_PATH`
- `CORSA_TRUST_STORE_PATH`
- `CORSA_NODE_TYPE`
- `CORSA_MAX_CLOCK_DRIFT_SECONDS`

Правила:

- `CORSA_BOOTSTRAP_PEERS` имеет приоритет над `CORSA_BOOTSTRAP_PEER`
- если ничего не задано, используется seed по умолчанию: `65.108.204.190:64646`
- `CORSA_NODE_TYPE=full` включает relay/forwarding
- `CORSA_NODE_TYPE=client` отключает forwarding, но оставляет sync, inbox и локальное хранение
- допустимый drift времени сообщений по умолчанию: `600` секунд

### Handshake

Основной JSON-запрос от desktop:

```json
{
  "type": "hello",
  "version": 1,
  "client": "desktop",
  "client_version": "0.3-alpha"
}
```

Поля:

- `type` — обязательное; тип кадра, здесь всегда `hello`
- `version` — обязательное; версия формата кадра
- `client` — обязательное; тип вызывающей стороны, например `desktop` или `node`
- `client_version` — опциональное; версия приложения в wire-форме

Основной JSON-запрос node-to-node:

```json
{
  "type": "hello",
  "version": 1,
  "client": "node",
  "listen": "203.0.113.10:64646",
  "node_type": "full",
  "client_version": "0.3-alpha",
  "services": [
    "identity",
    "contacts",
    "messages",
    "gazeta",
    "relay"
  ],
  "address": "<fingerprint>",
  "pubkey": "<base64-ed25519-pubkey>",
  "boxkey": "<base64-x25519-pubkey>",
  "boxsig": "<base64url-ed25519-signature>"
}
```

Поля:

- `type` — обязательное; тип кадра, здесь `hello`
- `version` — обязательное; версия формата кадра
- `client` — обязательное; тип вызывающей стороны, здесь `node`
- `listen` — опциональное; адрес, который узел рекламирует пирам
- `node_type` — опциональное; роль узла, сейчас `full` или `client`
- `client_version` — опциональное; версия ПО узла
- `services` — опциональное; список capabilities, которые поддерживает узел
- `address` — опциональное; fingerprint identity этого узла
- `pubkey` — опциональное; base64 `ed25519` identity key
- `boxkey` — опциональное; base64 `X25519` ключ для шифрования
- `boxsig` — опциональное; подпись, связывающая `boxkey` с identity key

Ответ:

```json
{
  "type": "welcome",
  "version": 1,
  "node": "corsa",
  "network": "gazeta-devnet",
  "node_type": "full",
  "client_version": "0.3-alpha",
  "services": [
    "identity",
    "contacts",
    "messages",
    "gazeta",
    "relay"
  ],
  "address": "<fingerprint>",
  "pubkey": "<base64-ed25519-pubkey>",
  "boxkey": "<base64-x25519-pubkey>",
  "boxsig": "<base64url-ed25519-signature>"
}
```

Поля:

- `type` — обязательное; тип кадра, здесь `welcome`
- `version` — обязательное; версия формата кадра
- `node` — обязательное; имя серверной реализации
- `network` — обязательное; логическое имя сети
- `node_type` — опциональное; роль отвечающего узла
- `client_version` — опциональное; версия ПО отвечающего узла
- `services` — опциональное; capabilities отвечающего узла
- `address` — опциональное; fingerprint identity отвечающего узла
- `pubkey` — опциональное; identity key узла
- `boxkey` — опциональное; encryption key узла
- `boxsig` — опциональное; подпись, подтверждающая принадлежность encryption key этому identity

Правила ролей:

- `full`-узлы форвардят mesh-трафик
- `client`-узлы не ретранслируют трафик дальше
- `corsa-desktop` и `corsa-node` по умолчанию запускаются как `full`
- будущий mobile/light client должен использовать `client`
- текущая версия клиента: `0.3 alpha`
- wire-форма в handshake: `0.3-alpha`

### Peer sync

Запрос:

```json
{
  "type": "get_peers"
}
```

Ответ:

```json
{
  "type": "peers",
  "count": 2,
  "peers": [
    "127.0.0.1:64646",
    "127.0.0.1:64647"
  ]
}
```

Поля:

- `type` — обязательное; тип кадра, здесь `peers`
- `count` — обязательное; число адресов в `peers`
- `peers` — обязательное; список peer endpoints

### Contacts

Запрос:

```json
{
  "type": "fetch_contacts"
}
```

Ответ:

```json
{
  "type": "contacts",
  "count": 2,
  "contacts": [
    {
      "address": "<address1>",
      "boxkey": "<boxkey1>",
      "pubkey": "<pubkey1>",
      "boxsig": "<boxsig1>"
    },
    {
      "address": "<address2>",
      "boxkey": "<boxkey2>",
      "pubkey": "<pubkey2>",
      "boxsig": "<boxsig2>"
    }
  ]
}
```

Поля:

- `type` — обязательное; тип кадра, здесь `contacts`
- `count` — обязательное; число contact records
- `contacts` — обязательное; массив известных identity
- `contacts[].address` — обязательное; fingerprint контакта
- `contacts[].pubkey` — обязательное; `ed25519` identity key контакта
- `contacts[].boxkey` — обязательное; `X25519` encryption key контакта
- `contacts[].boxsig` — обязательное; подпись, связывающая encryption key с identity key контакта

Правила доверия:

- `pubkey` должен хэшироваться в объявленный fingerprint-адрес
- `boxkey` должен быть подписан этим identity key
- первый валидный набор ключей pin-ится локально
- последующие конфликтующие ключи игнорируются

### Direct messages

Основной JSON-запрос:

```json
{
  "type": "send_message",
  "topic": "dm",
  "id": "550e8400-e29b-41d4-a716-446655440001",
  "address": "a1b2c3",
  "recipient": "d4e5f6",
  "flag": "sender-delete",
  "created_at": "2026-03-19T12:00:05Z",
  "ttl_seconds": 0,
  "body": "<ciphertext-token>"
}
```

Поля:

- `type` — обязательное; тип кадра, здесь `send_message`
- `topic` — обязательное; логический канал сообщений, например `global` или `dm`
- `id` — обязательное; UUID сообщения
- `address` — обязательное; fingerprint отправителя
- `recipient` — обязательное; fingerprint получателя или `*` для broadcast
- `flag` — обязательное; правило удаления/хранения сообщения
- `created_at` — обязательное; timestamp отправителя в RFC3339 UTC
- `ttl_seconds` — опциональное; TTL для `auto-delete-ttl`, иначе `0`
- `body` — обязательное; plaintext для публичных тем или ciphertext для `dm`

Ответы:

```json
{
  "type": "message_stored",
  "topic": "dm",
  "count": 1,
  "id": "550e8400-e29b-41d4-a716-446655440001"
}
{
  "type": "message_known",
  "topic": "dm",
  "count": 1,
  "id": "550e8400-e29b-41d4-a716-446655440001"
}
{
  "type": "error",
  "code": "message-timestamp-out-of-range"
}
```

Запрос полного лога:

```json
{
  "type": "fetch_messages",
  "topic": "dm"
}
```

Ответ:

```json
{
  "type": "messages",
  "topic": "dm",
  "count": 1,
  "messages": [
    {
      "id": "<id>",
      "flag": "sender-delete",
      "created_at": "2026-03-19T12:00:05Z",
      "ttl_seconds": 0,
      "sender": "a1b2c3",
      "recipient": "d4e5f6",
      "body": "<ciphertext-token>"
    }
  ]
}
```

Поля:

- `type` — обязательное; тип кадра, здесь `messages`
- `topic` — обязательное; запрошенная тема
- `count` — обязательное; число возвращенных сообщений
- `messages` — обязательное; массив message objects
- `messages[].id` — обязательное; UUID сообщения
- `messages[].sender` — обязательное; fingerprint отправителя
- `messages[].recipient` — обязательное; fingerprint получателя или `*`
- `messages[].flag` — обязательное; правило удаления/хранения
- `messages[].created_at` — обязательное; исходный timestamp отправителя
- `messages[].ttl_seconds` — опциональное; TTL для автоудаления, если используется
- `messages[].body` — обязательное; payload сообщения или ciphertext

Запрос индекса UUID:

```json
{
  "type": "fetch_message_ids",
  "topic": "dm"
}
```

Ответ:

```json
{
  "type": "message_ids",
  "topic": "dm",
  "count": 2,
  "ids": [
    "uuid1",
    "uuid2"
  ]
}
```

Поля:

- `type` — обязательное; тип кадра, здесь `message_ids`
- `topic` — обязательное; запрошенная тема
- `count` — обязательное; число UUID в списке
- `ids` — обязательное; список UUID без полной загрузки сообщений

Запрос одного сообщения:

```json
{
  "type": "fetch_message",
  "topic": "dm",
  "id": "uuid1"
}
```

Ответ:

```json
{
  "type": "message",
  "topic": "dm",
  "id": "uuid1",
  "item": {
    "id": "uuid1",
    "flag": "sender-delete",
    "created_at": "2026-03-19T12:00:05Z",
    "ttl_seconds": 0,
    "sender": "a1b2c3",
    "recipient": "d4e5f6",
    "body": "<ciphertext-token>"
  }
}
```

Поля:

- `type` — обязательное; тип кадра, здесь `message`
- `topic` — обязательное; запрошенная тема
- `id` — обязательное; UUID, который запрашивали
- `item` — опциональное; полный объект сообщения для этого UUID

Запрос inbox:

```json
{
  "type": "fetch_inbox",
  "topic": "dm",
  "recipient": "d4e5f6"
}
```

Ответ:

```json
{
  "type": "inbox",
  "topic": "dm",
  "recipient": "d4e5f6",
  "count": 1,
  "messages": [
    {
      "id": "<id>",
      "flag": "sender-delete",
      "created_at": "2026-03-19T12:00:05Z",
      "ttl_seconds": 0,
      "sender": "a1b2c3",
      "recipient": "d4e5f6",
      "body": "<ciphertext-token>"
    }
  ]
}
```

Поля:

- `type` — обязательное; тип кадра, здесь `inbox`
- `topic` — обязательное; запрошенная тема
- `recipient` — обязательное; identity, для которой фильтруется inbox
- `count` — обязательное; число видимых сообщений
- `messages` — обязательное; отфильтрованный массив сообщений для этого inbox-запроса

Примечания:

- для `dm` поле `<body>` содержит ciphertext
- ноды проверяют подпись `dm` до хранения и relay
- desktop повторно проверяет подпись перед расшифровкой и показом
- сообщения вне допустимого time drift отклоняются и не форвардятся
- сообщения с `auto-delete-ttl` удаляются после `ttl-seconds`
- `fetch_message_ids` можно использовать как легкий индекс direct messages
- `fetch_message` позволяет загрузить одно direct message по UUID

Флаги сообщений:

- `immutable` — сообщение нельзя удалить никому
- `sender-delete` — удалить сообщение может только отправитель
- `any-delete` — удалить сообщение может любой участник
- `auto-delete-ttl` — сообщение автоматически удаляется по `ttl-seconds`

### Gazeta

Запрос публикации:

```json
{
  "type": "publish_notice",
  "ttl_seconds": 30,
  "ciphertext": "<ciphertext-token>"
}
```

Поля:

- `type` — обязательное; тип кадра, здесь `publish_notice`
- `ttl_seconds` — обязательное; время жизни объявления в секундах
- `ciphertext` — обязательное; зашифрованный payload Gazeta

Ответы:

```json
{
  "type": "notice_stored",
  "id": "<notice-id>",
  "expires_at": 1234567890
}
{
  "type": "notice_known",
  "id": "<notice-id>",
  "expires_at": 1234567890
}
```

Запрос получения:

```json
{
  "type": "fetch_notices"
}
```

Ответ:

```json
{
  "type": "notices",
  "count": 1,
  "notices": [
    {
      "id": "<id>",
      "expires_at": 1234567890,
      "ciphertext": "<ciphertext-token>"
    }
  ]
}
```

Поля:

- `type` — обязательное; тип кадра, здесь `notices`
- `count` — обязательное; число активных notices
- `notices` — обязательное; массив активных зашифрованных notices
- `notices[].id` — обязательное; идентификатор notice, вычисленный нодой
- `notices[].expires_at` — обязательное; время истечения в Unix seconds
- `notices[].ciphertext` — обязательное; зашифрованный payload notice

### Ошибки

Возможные JSON error codes:

```json
{
  "type": "error",
  "code": "unknown-command"
}
{
  "type": "error",
  "code": "invalid-send-message"
}
{
  "type": "error",
  "code": "invalid-fetch-messages"
}
{
  "type": "error",
  "code": "invalid-fetch-message-ids"
}
{
  "type": "error",
  "code": "invalid-fetch-message"
}
{
  "type": "error",
  "code": "invalid-fetch-inbox"
}
{
  "type": "error",
  "code": "invalid-publish-notice"
}
{
  "type": "error",
  "code": "unknown-sender-key"
}
{
  "type": "error",
  "code": "unknown-message-id"
}
{
  "type": "error",
  "code": "invalid-direct-message-signature"
}
{
  "type": "error",
  "code": "read"
}
```

Поля:

- `type` — обязательное; тип кадра, здесь `error`
- `code` — обязательное; стабильный machine-readable код ошибки
- `error` — опциональное; человекочитаемое пояснение, если оно передается

### Текущий desktop flow

1. загрузка/создание identity
2. загрузка/создание trust store
3. запуск встроенной локальной ноды
4. синк peers и contacts
5. получение списка contacts
6. получение topic traffic
7. получение и локальная расшифровка direct messages
8. получение notices из Gazeta
