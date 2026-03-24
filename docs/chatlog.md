# Chat Log Persistence

## English

### Overview

The `chatlog` package provides append-only, file-backed storage for chat messages.
The desktop client does **not** keep all conversations in memory. Messages are
written to JSONL files on disk as they arrive and read back on demand when the
UI switches to a conversation. Only lightweight metadata (message headers and
previews) is kept in memory for the sidebar.

### Architecture diagram

```mermaid
flowchart TB
    subgraph MEMORY["Memory (runtime)"]
        TOPICS["s.topics[dm]\n(in-memory buffer\nfor gossip/relay)"]
        SEEN["s.seen map\n(dedup IDs)"]
        HEADERS["DMHeaders\n(id, sender, recipient, ts)\nno body — lightweight"]
        PREVIEWS["Cached Previews\n(last msg per peer)\nmap[peer]→body"]
        CONV["conversationMessages\n(active chat only)\ndecrypted on demand"]
    end

    subgraph DISK["Disk (.corsa/)"]
        FILE_A["dm-abc12345-peer_A-64646.jsonl"]
        FILE_B["dm-abc12345-peer_B-64646.jsonl"]
        FILE_G["global-abc12345-64646.jsonl"]
        QUEUE["queue-64646.json\n(relay state + retry)"]
    end

    TOPICS -->|"chatLog.Append()\nonly if isLocalMessage()"| FILE_A
    TOPICS -->|"chatLog.Append()\nonly if isLocalMessage()"| FILE_B
    TOPICS -->|"trackRelayMessage()\ntransit DMs only"| QUEUE
    FILE_A -->|"fetch_chatlog\n(on demand)"| CONV
    FILE_B -->|"fetch_chatlog\n(on demand)"| CONV
    FILE_A -->|"ReadLastEntryPerPeer()\n(on startup)"| PREVIEWS
    FILE_B -->|"ReadLastEntryPerPeer()\n(on startup)"| PREVIEWS
    TOPICS -->|"fetch_dm_headers\n(every 2s, no body)"| HEADERS
```

### What is stored where

```mermaid
flowchart LR
    subgraph MEM["In Memory"]
        direction TB
        M1["DMHeaders — lightweight\nid + sender + recipient + ts\nused for unread detection"]
        M2["Previews — last message\nper peer, decrypted body\nused for sidebar"]
        M3["Active conversation —\nfull decrypted messages\nonly for selected peer"]
        M4["topics[dm] — encrypted\nenvelopes for gossip/relay\n(node layer)"]
    end

    subgraph DSK["On Disk"]
        direction TB
        D1["Chatlog (JSONL)\nsealed envelopes\nper-peer files"]
        D2["Incoming DMs:\nstored as-is"]
        D3["Outgoing DMs:\nsealed envelope with\nsender-readable part"]
        D4["queue-port.json\ntransit relay state\n+ retry metadata"]
    end

    MEM -->|"chatLog.Append()\nlocal messages only"| D1
    MEM -->|"trackRelayMessage()\ntransit DMs only"| D4
    D1 -->|"read on demand"| MEM
```

### Message arrival flow

```mermaid
sequenceDiagram
    participant NET as Network peer
    participant NODE as Local node
    participant LOG as Chatlog (disk)
    participant QUEUE as queue-port.json
    participant UI as Desktop UI

    NET->>NODE: relay DM (encrypted envelope)
    NODE->>NODE: validate timestamp
    NODE->>NODE: verify ed25519 signature
    NODE->>NODE: check dedup (s.seen)
    NODE->>NODE: store in s.topics[dm]
    alt local message (sender or recipient is this node)
        NODE->>LOG: chatLog.Append(entry)
        Note over LOG: JSONL file: O_APPEND<br/>sealed envelope stored as-is<br/>no extra encryption
        NODE-->>UI: SubscribeLocalChanges event
        NODE->>NODE: push to DM subscribers
        UI->>NODE: fetch_dm_headers
        Note over UI: lightweight: id + sender<br/>+ recipient + timestamp<br/>NO body transferred
        UI->>UI: detect new header IDs
        UI->>UI: update unread counts
        UI->>LOG: FetchSinglePreview(peer)
        Note over UI: decrypt only 1 message<br/>for sidebar preview
    else transit message (neither party is this node)
        Note over NODE: NOT written to chatlog
        NODE->>NODE: trackRelayMessage()
        NODE->>NODE: gossip to other peers
        NODE->>QUEUE: persist relay state on save
    end
```

### Sending a message

```mermaid
sequenceDiagram
    participant UI as Desktop UI
    participant SVC as DesktopClient
    participant NODE as Local node
    participant LOG as Chatlog (disk)

    UI->>SVC: SendDirectMessage(peer, body)
    SVC->>SVC: encrypt: EncryptForParticipants()
    Note over SVC: creates sealed envelope<br/>with recipient-part + sender-part<br/>both encrypted, ed25519 signed
    SVC->>NODE: send_message(dm, envelope)
    NODE->>NODE: validate + store in s.topics[dm]
    NODE->>LOG: chatLog.Append(entry)
    Note over LOG: body = base64(sealed envelope)<br/>sender can decrypt own msgs<br/>via sender-part
    NODE-->>UI: local change event
    NODE->>NODE: gossip to peers
    UI->>UI: reload active conversation from disk
    UI->>UI: refresh preview for peer
```

### Loading a conversation (on demand)

```mermaid
sequenceDiagram
    participant UI as Desktop Window
    participant SVC as DesktopClient
    participant NODE as Local node
    participant LOG as Chatlog (disk)

    UI->>UI: user clicks on peer in sidebar
    UI->>SVC: FetchConversation(peerAddress)
    SVC->>NODE: fetch_chatlog(topic=dm, address=peer)
    NODE->>LOG: chatLog.Read(dm, peerAddress)
    Note over LOG: read JSONL file for this peer<br/>all entries (encrypted bodies)
    LOG-->>NODE: []Entry
    NODE-->>SVC: []ChatEntryFrame
    SVC->>SVC: fetch trusted contacts
    SVC->>SVC: decryptDirectMessages()
    Note over SVC: decrypt each sealed envelope<br/>using local identity key
    SVC-->>UI: []DirectMessage (decrypted)
    UI->>UI: display conversation
    Note over UI: only THIS conversation<br/>is in memory, others are not
```

### Loading previews (on startup, with retry)

```mermaid
sequenceDiagram
    participant UI as Desktop Window
    participant SVC as DesktopClient
    participant NODE as Local node
    participant LOG as Chatlog (disk)

    UI->>SVC: FetchConversationPreviews()
    SVC->>NODE: fetch_chatlog_previews
    NODE->>LOG: ReadLastEntryPerPeer()
    Note over LOG: scan all dm-*.jsonl files<br/>read last entry from each
    LOG-->>NODE: map[peer]→Entry
    NODE-->>SVC: []ChatPreviewFrame
    SVC->>SVC: decrypt each preview
    Note over SVC: only 1 message per peer<br/>= minimal decryption work
    SVC-->>UI: []ConversationPreview
    alt success
        UI->>UI: cache in previews map
        UI->>UI: render sidebar with last message text
    else error or empty (node still starting)
        UI->>UI: retry with exponential backoff (1s, 2s, 4s)
        UI->>SVC: FetchConversationPreviews() (retry)
    end
```

### File naming

Each conversation partner gets its own file in the chatlog directory
(defaults to `.corsa/`, configurable via `CORSA_CHATLOG_DIR`):

```
dm-<identity_short>-<peer_address>-<port>.jsonl   # DM with a specific peer
global-<identity_short>-<port>.jsonl               # broadcast / public messages
```

- `identity_short` — first 8 characters of the node's identity address (40-char hex SHA256 fingerprint)
- `peer_address` — full 40-char hex address of the conversation partner
- `port` — TCP listen port (same suffix used for identity, trust, queue, and peers files)

This naming scheme ensures:
- Multiple identities on the same machine don't collide
- Multiple node instances on different ports don't collide
- Each conversation is in its own file for independent access

### File format

Files use JSON Lines format — one JSON object per line:

```json
{"id":"550e8400-e29b-41d4-a716-446655440000","sender":"aabb...","recipient":"ccdd...","body":"<sealed_envelope_base64>","created_at":"2026-03-23T12:00:00.000Z","flag":"immutable"}
{"id":"660e8400-e29b-41d4-a716-446655440001","sender":"ccdd...","recipient":"aabb...","body":"<sealed_envelope_base64>","created_at":"2026-03-23T12:01:00.000Z","flag":"immutable"}
```

Each line is a `chatlog.Entry`:

| Field       | Type   | Description                                          |
|-------------|--------|------------------------------------------------------|
| `id`        | string | Message UUID                                         |
| `sender`    | string | Sender's identity address (40-char hex)              |
| `recipient` | string | Recipient's identity address or `*` for broadcast    |
| `body`      | string | Raw message body (sealed envelope for DMs, plaintext for global) |
| `created_at`| string | RFC3339Nano timestamp                                |
| `flag`      | string | Message flag (immutable, sender-delete, etc.)        |

### Body encoding

```mermaid
flowchart LR
    subgraph INCOMING["Incoming DM"]
        I1["Peer sends sealed envelope"] --> I2["Node receives base64 ciphertext"]
        I2 --> I3["chatLog.Append(body as-is)"]
    end

    subgraph OUTGOING["Outgoing DM"]
        O1["Desktop encrypts message"] --> O2["Creates sealed envelope:\n• recipient-part (Bob's key)\n• sender-part (own key)"]
        O2 --> O3["Node stores envelope"]
        O3 --> O4["chatLog.Append(body as-is)"]
    end

    subgraph ONDISK["On Disk"]
        D1["body = base64(sealed_envelope)\n\nrecipient-part: encrypted with recipient X25519\nsender-part: encrypted with sender X25519\nsignature: ed25519 of sender\n\nNo additional encryption layer"]
    end

    I3 --> D1
    O4 --> D1
```

- **Incoming DMs**: stored as-is. The body is already a base64-encoded sealed envelope
  that can only be decrypted by the recipient's or sender's identity key via
  `directmsg.DecryptForIdentity()`.
- **Outgoing DMs**: the body is the same sealed envelope, which includes a sender-part
  encrypted with the sender's own box key — so the sender can always decrypt their own
  messages.
- **Global/broadcast messages**: stored as-is (plaintext body).

No additional encryption layer is applied. The sealed envelope itself provides
end-to-end encryption for DMs.

### Write flow

```
storeIncomingMessage()
  ├── validate timestamp and signatures
  ├── store in-memory (s.topics[topic])
  ├── if isLocalMessage():
  │     ├── chatLog.Append(topic, selfAddress, entry)
  │     │     └── open file O_APPEND → write JSON line → close
  │     ├── emitLocalChange() → notify UI
  │     └── push to DM subscribers + delivery receipt
  ├── gossip to peers (if routing, via shouldRouteStoredMessage)
  └── trackRelayMessage() (transit DMs only)
```

The append happens synchronously after the in-memory store, before gossip.
The chatlog directory is auto-created (`MkdirAll` with `0700`) if it does not exist.
File writes use `O_CREATE|O_APPEND|O_WRONLY` with `0600` permissions.
Errors are logged but do not fail the message store — the in-memory store
and network propagation always proceed.

**Transit messages are NOT written to chatlog.** When a full node relays a DM
where neither sender nor recipient is the local identity, the message is stored
only in-memory (`s.topics[dm]`) for gossip/relay purposes. Transit persistence
is handled separately via `queue-<port>.json` and the `relayRetry` mechanism.
This ensures the local chat history only contains conversations this node
actually participates in.

### Read flow

Desktop client uses three read strategies depending on context:

| Strategy                | When                          | What is read                    | Decryption |
|-------------------------|-------------------------------|---------------------------------|------------|
| `fetch_dm_headers`      | Every 2s poll                 | ID + sender + recipient + ts (local only) | None       |
| `fetch_chatlog_previews`| App startup (with retry)      | Last entry per conversation     | 1 msg/peer |
| `fetch_chatlog`         | User opens a conversation     | All entries for one peer        | Full       |

```
# Lightweight poll (every 2 seconds)
HandleLocalFrame("fetch_dm_headers")
  └── return message headers from s.topics[dm] — local only (sender/recipient = this node), no body, no disk I/O

# Preview load (on startup with retry + on new message)
HandleLocalFrame("fetch_chatlog_previews")
  ├── chatLog.ReadLastEntryPerPeer()
  │     └── scan all dm-*.jsonl files → read last line from each
  └── return []ChatPreviewFrame with encrypted bodies
# Startup: retries up to 4 times (backoff 1s→2s→4s) if node is not ready

# Full conversation load (on demand)
HandleLocalFrame("fetch_chatlog")
  ├── chatLog.Read(topic, peerAddress)   [or ReadLast with limit]
  │     └── open file → scan JSON lines → return []Entry
  └── convert to []ChatEntryFrame → return Frame
```

### Console commands

| Command                                    | Description                              |
|--------------------------------------------|------------------------------------------|
| `fetch_chatlog [topic] <peer_address>`     | Read chat history for a peer             |
| `fetch_chatlog_previews`                   | Last message for each conversation       |
| `fetch_dm_headers`                         | Lightweight DM headers (no body, local only — transit filtered out) |
| `fetch_conversations`                      | List all conversations with counts       |

### Config

| Environment Variable  | Config Field          | Default   | Description                  |
|-----------------------|-----------------------|-----------|------------------------------|
| `CORSA_CHATLOG_DIR`   | `Node.ChatLogDir`     | `.corsa`  | Directory for chatlog files (auto-created if missing) |

### Deduplication

`HasEntryID()` can check whether a message ID already exists in the file.
Currently, the in-memory `seen` map in the node service handles deduplication
before the chatlog append, so duplicate writes don't normally occur.

### Conversation listing

`ListConversations()` scans the chatlog directory for DM files matching the
current identity and port, reads each file to extract the last message timestamp
and total count, and returns results sorted by most recent message first.

### Memory optimization

The desktop client minimizes memory usage by following these principles:

1. **No bulk DM decryption in poll loop** — `ProbeNode()` fetches only lightweight
   `DMHeaders` (no message bodies) every 2 seconds.
2. **Previews loaded once** — on startup (with retry up to 4 attempts, exponential
   backoff), one message per conversation is decrypted for the sidebar; updated
   incrementally when new messages arrive.
3. **Deduplicated preview refresh** — when new headers arrive,
   `refreshPreviewForPeer()` is called once per unique peer, not once per message.
4. **Conversation loaded on demand** — full chat history is read from disk and
   decrypted only when the user switches to a specific peer.
5. **Only active conversation in memory** — switching to another peer replaces
   the previous conversation data.
6. **Transit messages excluded from chatlog** — DMs relayed through a full node
   (where neither party is local) are only stored in-memory for gossip; their
   persistence is handled by `queue-<port>.json`.
7. **Transit DMs filtered from `fetch_dm_headers`** — the poll loop returns only
   headers where the local node is sender or recipient; `seenIncoming` map
   records only local headers to avoid unbounded memory growth from transit traffic.

---

## Русский

### Обзор

Пакет `chatlog` обеспечивает append-only хранение сообщений на диске.
Desktop-клиент **не** хранит все чаты в памяти. Сообщения записываются
в JSONL-файлы по мере поступления и читаются обратно по запросу, когда
пользователь переключается на диалог. В памяти хранятся только легковесные
метаданные (заголовки сообщений и превью) для боковой панели.

### Диаграмма архитектуры

```mermaid
flowchart TB
    subgraph MEMORY["Память (runtime)"]
        TOPICS["s.topics[dm]\n(буфер для gossip/relay)"]
        SEEN["s.seen map\n(ID для дедупликации)"]
        HEADERS["DMHeaders\n(id, sender, recipient, ts)\nбез тела — легковесно"]
        PREVIEWS["Кэш превью\n(последнее сообщение для каждого peer)\nmap[peer]→body"]
        CONV["conversationMessages\n(только активный чат)\nрасшифровывается по запросу"]
    end

    subgraph DISK["Диск (.corsa/)"]
        FILE_A["dm-abc12345-peer_A-64646.jsonl"]
        FILE_B["dm-abc12345-peer_B-64646.jsonl"]
        FILE_G["global-abc12345-64646.jsonl"]
        QUEUE["queue-64646.json\n(состояние relay + retry)"]
    end

    TOPICS -->|"chatLog.Append()\nтолько если isLocalMessage()"| FILE_A
    TOPICS -->|"chatLog.Append()\nтолько если isLocalMessage()"| FILE_B
    TOPICS -->|"trackRelayMessage()\nтолько транзитные DM"| QUEUE
    FILE_A -->|"fetch_chatlog\n(по запросу)"| CONV
    FILE_B -->|"fetch_chatlog\n(по запросу)"| CONV
    FILE_A -->|"ReadLastEntryPerPeer()\n(при запуске)"| PREVIEWS
    FILE_B -->|"ReadLastEntryPerPeer()\n(при запуске)"| PREVIEWS
    TOPICS -->|"fetch_dm_headers\n(каждые 2с, без тела)"| HEADERS
```

### Что где хранится

```mermaid
flowchart LR
    subgraph MEM["В памяти"]
        direction TB
        M1["DMHeaders — легковесные\nid + sender + recipient + ts\nдля отслеживания непрочитанных"]
        M2["Превью — последнее сообщение\nдля каждого peer, расшифрованное\nдля боковой панели"]
        M3["Активный диалог —\nполные расшифрованные сообщения\nтолько для выбранного peer"]
        M4["topics[dm] — зашифрованные\nконверты для gossip/relay\n(уровень ноды)"]
    end

    subgraph DSK["На диске"]
        direction TB
        D1["Chatlog (JSONL)\nsealed envelopes\nпо файлу на peer"]
        D2["Входящие ЛС:\nхранятся как есть"]
        D3["Исходящие ЛС:\nsealed envelope с\nsender-readable частью"]
        D4["queue-port.json\nсостояние relay транзита\n+ метаданные retry"]
    end

    MEM -->|"chatLog.Append()\nтолько локальные сообщения"| D1
    MEM -->|"trackRelayMessage()\nтолько транзитные DM"| D4
    D1 -->|"чтение по запросу"| MEM
```

### Flow поступления сообщения

```mermaid
sequenceDiagram
    participant NET as Сетевой peer
    participant NODE as Локальная нода
    participant LOG as Chatlog (диск)
    participant QUEUE as queue-port.json
    participant UI as Desktop UI

    NET->>NODE: relay DM (зашифрованный конверт)
    NODE->>NODE: валидация timestamp
    NODE->>NODE: проверка подписи ed25519
    NODE->>NODE: проверка дедупликации (s.seen)
    NODE->>NODE: запись в s.topics[dm]
    alt локальное сообщение (sender или recipient — эта нода)
        NODE->>LOG: chatLog.Append(entry)
        Note over LOG: JSONL файл: O_APPEND<br/>sealed envelope хранится как есть<br/>без доп. шифрования
        NODE-->>UI: событие SubscribeLocalChanges
        NODE->>NODE: push подписчикам DM
        UI->>NODE: fetch_dm_headers
        Note over UI: легковесно: id + sender<br/>+ recipient + timestamp<br/>БЕЗ тела сообщения
        UI->>UI: обнаружение новых ID
        UI->>UI: обновление счётчика непрочитанных
        UI->>LOG: FetchSinglePreview(peer)
        Note over UI: расшифровка только 1 сообщения<br/>для превью в боковой панели
    else транзитное сообщение (ни одна сторона — не эта нода)
        Note over NODE: НЕ записывается в chatlog
        NODE->>NODE: trackRelayMessage()
        NODE->>NODE: gossip другим peers
        NODE->>QUEUE: сохранение состояния relay
    end
```

### Flow отправки сообщения

```mermaid
sequenceDiagram
    participant UI as Desktop UI
    participant SVC as DesktopClient
    participant NODE as Локальная нода
    participant LOG as Chatlog (диск)

    UI->>SVC: SendDirectMessage(peer, body)
    SVC->>SVC: шифрование: EncryptForParticipants()
    Note over SVC: создаёт sealed envelope<br/>с recipient-part + sender-part<br/>оба зашифрованы, ed25519 подписан
    SVC->>NODE: send_message(dm, envelope)
    NODE->>NODE: валидация + запись в s.topics[dm]
    NODE->>LOG: chatLog.Append(entry)
    Note over LOG: body = base64(sealed envelope)<br/>отправитель может расшифровать свои<br/>сообщения через sender-part
    NODE-->>UI: событие локального изменения
    NODE->>NODE: gossip к peers
    UI->>UI: перезагрузка активного диалога с диска
    UI->>UI: обновление превью для peer
```

### Flow загрузки диалога (по запросу)

```mermaid
sequenceDiagram
    participant UI as Desktop Window
    participant SVC as DesktopClient
    participant NODE as Локальная нода
    participant LOG as Chatlog (диск)

    UI->>UI: пользователь нажимает на peer в боковой панели
    UI->>SVC: FetchConversation(peerAddress)
    SVC->>NODE: fetch_chatlog(topic=dm, address=peer)
    NODE->>LOG: chatLog.Read(dm, peerAddress)
    Note over LOG: чтение JSONL файла для этого peer<br/>все записи (зашифрованные тела)
    LOG-->>NODE: []Entry
    NODE-->>SVC: []ChatEntryFrame
    SVC->>SVC: получение trusted contacts
    SVC->>SVC: decryptDirectMessages()
    Note over SVC: расшифровка каждого sealed envelope<br/>используя локальный ключ identity
    SVC-->>UI: []DirectMessage (расшифрованные)
    UI->>UI: отображение диалога
    Note over UI: только ЭТОТ диалог<br/>в памяти, остальные — нет
```

### Flow загрузки превью (при запуске, с retry)

```mermaid
sequenceDiagram
    participant UI as Desktop Window
    participant SVC as DesktopClient
    participant NODE as Локальная нода
    participant LOG as Chatlog (диск)

    UI->>SVC: FetchConversationPreviews()
    SVC->>NODE: fetch_chatlog_previews
    NODE->>LOG: ReadLastEntryPerPeer()
    Note over LOG: сканирование всех dm-*.jsonl<br/>чтение последней записи из каждого
    LOG-->>NODE: map[peer]→Entry
    NODE-->>SVC: []ChatPreviewFrame
    SVC->>SVC: расшифровка каждого превью
    Note over SVC: только 1 сообщение на peer<br/>= минимальная работа дешифрации
    SVC-->>UI: []ConversationPreview
    alt успех
        UI->>UI: кэширование в previews map
        UI->>UI: рендеринг боковой панели с текстом последнего сообщения
    else ошибка или пусто (нода ещё запускается)
        UI->>UI: retry с экспоненциальным backoff (1s, 2s, 4s)
        UI->>SVC: FetchConversationPreviews() (retry)
    end
```

### Именование файлов

Каждый собеседник получает свой файл в директории chatlog
(по умолчанию `.corsa/`, настраивается через `CORSA_CHATLOG_DIR`):

```
dm-<identity_short>-<peer_address>-<port>.jsonl   # ЛС с конкретным собеседником
global-<identity_short>-<port>.jsonl               # broadcast / публичные сообщения
```

- `identity_short` — первые 8 символов адреса identity ноды (40-символьный hex SHA256 fingerprint)
- `peer_address` — полный 40-символьный hex адрес собеседника
- `port` — TCP порт (тот же суффикс, что для identity, trust, queue и peers файлов)

Эта схема гарантирует:
- Разные identity на одной машине не пересекаются
- Разные инстансы ноды на разных портах не пересекаются
- Каждый диалог в своём файле для независимого доступа

### Формат файла

Файлы используют JSON Lines — один JSON-объект на строку:

```json
{"id":"550e8400-...","sender":"aabb...","recipient":"ccdd...","body":"<sealed_envelope>","created_at":"2026-03-23T12:00:00.000Z","flag":"immutable"}
```

### Кодирование тела сообщения

```mermaid
flowchart LR
    subgraph INCOMING["Входящее ЛС"]
        I1["Peer отправляет sealed envelope"] --> I2["Нода получает base64 шифротекст"]
        I2 --> I3["chatLog.Append(body как есть)"]
    end

    subgraph OUTGOING["Исходящее ЛС"]
        O1["Desktop шифрует сообщение"] --> O2["Создаёт sealed envelope:\n• recipient-part (ключ Bob)\n• sender-part (свой ключ)"]
        O2 --> O3["Нода сохраняет конверт"]
        O3 --> O4["chatLog.Append(body как есть)"]
    end

    subgraph ONDISK["На диске"]
        D1["body = base64(sealed_envelope)\n\nrecipient-part: зашифрован X25519 получателя\nsender-part: зашифрован X25519 отправителя\nподпись: ed25519 отправителя\n\nНикакого доп. слоя шифрования"]
    end

    I3 --> D1
    O4 --> D1
```

- **Входящие ЛС**: хранятся как есть. Body — это base64-encoded sealed envelope,
  который может быть расшифрован только ключом получателя или отправителя через
  `directmsg.DecryptForIdentity()`.
- **Исходящие ЛС**: тот же sealed envelope, который содержит sender-part,
  зашифрованный box-ключом отправителя — поэтому отправитель всегда может расшифровать
  свои сообщения.
- **Global/broadcast**: хранятся как есть (plaintext body).

Никакой дополнительный слой шифрования не применяется. Sealed envelope сам по себе
обеспечивает end-to-end шифрование для ЛС.

### Flow записи

```
storeIncomingMessage()
  ├── валидация timestamp и подписей
  ├── запись в память (s.topics[topic])
  ├── если isLocalMessage():
  │     ├── chatLog.Append(topic, selfAddress, entry)
  │     │     └── open O_APPEND → запись JSON-строки → close
  │     ├── emitLocalChange() → уведомление UI
  │     └── push подписчикам DM + delivery receipt
  ├── gossip к peers (если relay, через shouldRouteStoredMessage)
  └── trackRelayMessage() (только транзитные DM)
```

Append происходит синхронно после in-memory записи, до gossip.
Директория chatlog создаётся автоматически (`MkdirAll` с правами `0700`), если не существует.
Файл открывается с `O_CREATE|O_APPEND|O_WRONLY` и правами `0600`.
Ошибки логируются, но не блокируют сохранение сообщения.

**Транзитные сообщения НЕ записываются в chatlog.** Когда полная нода
пересылает ЛС, где ни отправитель, ни получатель не являются локальной
identity, сообщение хранится только в памяти (`s.topics[dm]`) для gossip/relay.
Персистентность транзита обеспечивается отдельно через `queue-<port>.json`
и механизм `relayRetry`. Это гарантирует, что локальная история чата содержит
только те диалоги, в которых эта нода реально участвует.

### Flow чтения

Стратегии чтения в зависимости от контекста:

| Стратегия                | Когда                             | Что читается                       | Дешифрация      |
|--------------------------|-----------------------------------|------------------------------------|-----------------|
| `fetch_dm_headers`       | Каждые 2с (poll)                  | ID + sender + recipient + ts (только локальные) | Нет             |
| `fetch_chatlog_previews` | При запуске (с retry)             | Последняя запись каждого диалога   | 1 сообщ./peer   |
| `fetch_chatlog`          | При открытии диалога              | Все записи для одного peer         | Полная          |

### Консольные команды

| Команда                                    | Описание                                   |
|--------------------------------------------|--------------------------------------------|
| `fetch_chatlog [topic] <peer_address>`     | Прочитать историю чата с peer              |
| `fetch_chatlog_previews`                   | Последнее сообщение для каждого диалога    |
| `fetch_dm_headers`                         | Легковесные заголовки DM (без тела, только локальные — транзитные отфильтрованы) |
| `fetch_conversations`                      | Список всех диалогов со счётчиками         |

### Конфигурация

| Переменная окружения  | Поле конфига         | По умолчанию | Описание                    |
|-----------------------|----------------------|--------------|-----------------------------|
| `CORSA_CHATLOG_DIR`   | `Node.ChatLogDir`    | `.corsa`     | Директория для chatlog файлов (создаётся автоматически) |

### Оптимизация памяти

Desktop-клиент минимизирует использование памяти, следуя этим принципам:

1. **Нет массовой расшифровки DM в цикле опроса** — `ProbeNode()` получает только
   легковесные `DMHeaders` (без тел сообщений) каждые 2 секунды.
2. **Превью загружаются один раз** — при запуске (с retry до 4 попыток, экспоненциальный
   backoff) расшифровывается по одному сообщению на диалог для боковой панели;
   обновляется инкрементально при поступлении новых.
3. **Дедупликация обновления превью** — при получении новых заголовков
   `refreshPreviewForPeer()` вызывается один раз на уникального собеседника, а не
   на каждое сообщение.
4. **Диалог загружается по запросу** — полная история читается с диска и расшифровывается
   только когда пользователь переключается на конкретного собеседника.
5. **Только активный диалог в памяти** — переключение на другого собеседника заменяет
   предыдущие данные диалога.
6. **Транзитные сообщения исключены из chatlog** — ЛС, пересылаемые через полную ноду
   (где ни одна из сторон не является локальной), хранятся только в памяти для gossip;
   их персистентность обеспечивается через `queue-<port>.json`.
7. **Транзитные DM отфильтрованы из `fetch_dm_headers`** — цикл опроса возвращает только
   заголовки, где локальная нода является отправителем или получателем; карта `seenIncoming`
   записывает только локальные заголовки, чтобы избежать неограниченного роста памяти от
   транзитного трафика.
