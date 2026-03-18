# CORSA Protocol

## English

Related crypto documentation:

- [encryption.md](encryption.md)

### Status

- version: `v7`
- stability: experimental
- transport: plain TCP
- framing: line-based UTF-8 terminated by `\n`
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

Rules:

- `CORSA_BOOTSTRAP_PEERS` overrides `CORSA_BOOTSTRAP_PEER`
- default seed if nothing is set: `65.108.204.190:64646`
- `CORSA_NODE_TYPE=full` enables relay/forwarding
- `CORSA_NODE_TYPE=client` disables forwarding but keeps sync, inbox, and local storage

### Handshake

Desktop request:

```text
HELLO version=1 client=desktop client_version=<wire-version>\n
```

Node-to-node request:

```text
HELLO version=1 client=node listen=<advertise> node_type=<full|client> client_version=<wire-version> services=<csv> address=<fingerprint> pubkey=<base64-ed25519-pubkey> boxkey=<base64-x25519-pubkey> boxsig=<base64url-ed25519-signature>\n
```

Response:

```text
WELCOME version=1 node=corsa network=gazeta-devnet node_type=<full|client> client_version=<wire-version> services=<csv> address=<fingerprint> pubkey=<base64-ed25519-pubkey> boxkey=<base64-x25519-pubkey> boxsig=<base64url-ed25519-signature>\n
```

Node-to-node request fields:

```text
HELLO version=1 client=node listen=<advertise> node_type=<full|client> client_version=<wire-version> services=<csv> address=<fingerprint> pubkey=<base64-ed25519-pubkey> boxkey=<base64-x25519-pubkey> boxsig=<base64url-ed25519-signature>\n
```

Role rules:

- `full` nodes forward mesh traffic
- `client` nodes do not relay traffic onward
- desktop and standalone console node default to `full`
- future mobile/light clients should use `client`
- current client version: `0.1 alpha`
- wire form used in handshake: `0.1-alpha`

### Peer sync

Request:

```text
GET_PEERS\n
```

Response:

```text
PEERS count=<n> list=<addr1>,<addr2>,...\n
```

### Contacts

Request:

```text
FETCH_CONTACTS\n
```

Response:

```text
CONTACTS count=<n> list=<address1>@<boxkey1>@<pubkey1>@<boxsig1>,<address2>@<boxkey2>@<pubkey2>@<boxsig2>,...\n
```

Trust rules:

- `pubkey` must hash to the advertised fingerprint address
- `boxkey` must be signed by that identity key
- the first valid key set is pinned locally
- later conflicting keys are ignored

### Direct messages

Request:

```text
SEND_MESSAGE <topic> <from-address> <to-address> <body>\n
```

Examples:

```text
SEND_MESSAGE global a1b2c3 * hello-all\n
SEND_MESSAGE dm a1b2c3 d4e5f6 <ciphertext-token>\n
```

Responses:

```text
MESSAGE_STORED topic=<topic> count=<n> id=<message-id>\n
MESSAGE_KNOWN topic=<topic> count=<n>\n
```

Message log request:

```text
FETCH_MESSAGES <topic>\n
```

Response:

```text
MESSAGES topic=<topic> count=<n> list=<from>><to>><body>|...\n
```

Inbox request:

```text
FETCH_INBOX <topic> <recipient-address>\n
```

Response:

```text
INBOX topic=<topic> recipient=<address> count=<n> list=<from>><to>><body>|...\n
```

Notes:

- `list=` consumes the rest of the line
- items are separated by `|`
- for `dm`, `<body>` is ciphertext
- nodes verify `dm` signatures before store/relay
- desktops verify signatures again before decrypt/render

### Gazeta

Publish request:

```text
PUBLISH_NOTICE <ttl-seconds> <ciphertext-token>\n
```

Responses:

```text
NOTICE_STORED id=<notice-id> expires=<unix>\n
NOTICE_KNOWN id=<notice-id> expires=<unix>\n
```

Fetch request:

```text
FETCH_NOTICES\n
```

Response:

```text
NOTICES count=<n> list=<id>@<expires>@<ciphertext>,...\n
```

### Errors

Possible errors:

```text
ERR unknown-command\n
ERR invalid-send-message\n
ERR invalid-fetch-messages\n
ERR invalid-fetch-inbox\n
ERR invalid-publish-notice\n
ERR unknown-sender-key\n
ERR invalid-direct-message-signature\n
ERR read\n
```

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

- версия: `v7`
- стабильность: experimental
- транспорт: plain TCP
- фрейминг: line-based UTF-8 с окончанием `\n`
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

Правила:

- `CORSA_BOOTSTRAP_PEERS` имеет приоритет над `CORSA_BOOTSTRAP_PEER`
- если ничего не задано, используется seed по умолчанию: `65.108.204.190:64646`
- `CORSA_NODE_TYPE=full` включает relay/forwarding
- `CORSA_NODE_TYPE=client` отключает forwarding, но оставляет sync, inbox и локальное хранение

### Handshake

Запрос от desktop:

```text
HELLO version=1 client=desktop client_version=<wire-version>\n
```

Запрос node-to-node:

```text
HELLO version=1 client=node listen=<advertise> node_type=<full|client> client_version=<wire-version> services=<csv> address=<fingerprint> pubkey=<base64-ed25519-pubkey> boxkey=<base64-x25519-pubkey> boxsig=<base64url-ed25519-signature>\n
```

Ответ:

```text
WELCOME version=1 node=corsa network=gazeta-devnet node_type=<full|client> client_version=<wire-version> services=<csv> address=<fingerprint> pubkey=<base64-ed25519-pubkey> boxkey=<base64-x25519-pubkey> boxsig=<base64url-ed25519-signature>\n
```

Поля node-to-node handshake:

```text
HELLO version=1 client=node listen=<advertise> node_type=<full|client> client_version=<wire-version> services=<csv> address=<fingerprint> pubkey=<base64-ed25519-pubkey> boxkey=<base64-x25519-pubkey> boxsig=<base64url-ed25519-signature>\n
```

Правила ролей:

- `full`-узлы форвардят mesh-трафик
- `client`-узлы не ретранслируют трафик дальше
- `corsa-desktop` и `corsa-node` по умолчанию запускаются как `full`
- будущий mobile/light client должен использовать `client`
- текущая версия клиента: `0.1 alpha`
- wire-форма в handshake: `0.1-alpha`

### Peer sync

Запрос:

```text
GET_PEERS\n
```

Ответ:

```text
PEERS count=<n> list=<addr1>,<addr2>,...\n
```

### Contacts

Запрос:

```text
FETCH_CONTACTS\n
```

Ответ:

```text
CONTACTS count=<n> list=<address1>@<boxkey1>@<pubkey1>@<boxsig1>,<address2>@<boxkey2>@<pubkey2>@<boxsig2>,...\n
```

Правила доверия:

- `pubkey` должен хэшироваться в объявленный fingerprint-адрес
- `boxkey` должен быть подписан этим identity key
- первый валидный набор ключей pin-ится локально
- последующие конфликтующие ключи игнорируются

### Direct messages

Запрос:

```text
SEND_MESSAGE <topic> <from-address> <to-address> <body>\n
```

Примеры:

```text
SEND_MESSAGE global a1b2c3 * hello-all\n
SEND_MESSAGE dm a1b2c3 d4e5f6 <ciphertext-token>\n
```

Ответы:

```text
MESSAGE_STORED topic=<topic> count=<n> id=<message-id>\n
MESSAGE_KNOWN topic=<topic> count=<n>\n
```

Запрос полного лога:

```text
FETCH_MESSAGES <topic>\n
```

Ответ:

```text
MESSAGES topic=<topic> count=<n> list=<from>><to>><body>|...\n
```

Запрос inbox:

```text
FETCH_INBOX <topic> <recipient-address>\n
```

Ответ:

```text
INBOX topic=<topic> recipient=<address> count=<n> list=<from>><to>><body>|...\n
```

Примечания:

- `list=` забирает остаток строки
- элементы разделяются через `|`
- для `dm` поле `<body>` содержит ciphertext
- ноды проверяют подпись `dm` до хранения и relay
- desktop повторно проверяет подпись перед расшифровкой и показом

### Gazeta

Запрос публикации:

```text
PUBLISH_NOTICE <ttl-seconds> <ciphertext-token>\n
```

Ответы:

```text
NOTICE_STORED id=<notice-id> expires=<unix>\n
NOTICE_KNOWN id=<notice-id> expires=<unix>\n
```

Запрос получения:

```text
FETCH_NOTICES\n
```

Ответ:

```text
NOTICES count=<n> list=<id>@<expires>@<ciphertext>,...\n
```

### Ошибки

Возможные ошибки:

```text
ERR unknown-command\n
ERR invalid-send-message\n
ERR invalid-fetch-messages\n
ERR invalid-fetch-inbox\n
ERR invalid-publish-notice\n
ERR unknown-sender-key\n
ERR invalid-direct-message-signature\n
ERR read\n
```

### Текущий desktop flow

1. загрузка/создание identity
2. загрузка/создание trust store
3. запуск встроенной локальной ноды
4. синк peers и contacts
5. получение списка contacts
6. получение topic traffic
7. получение и локальная расшифровка direct messages
8. получение notices из Gazeta
