# File Transfer Commands

## English

### POST /rpc/v1/file/sendFileAnnounce

Send a `file_announce` DM to a recipient. The file must already exist in the transmit directory (`<dataDir>/transmit/<sha256>.<ext>`). Pre-send validation (transmit file existence, sender quota) runs synchronously — if it fails, the RPC returns HTTP 500 with an error message. On success the response returns `"status": "pending"` indicating that pre-validation passed and the announce is queued for asynchronous delivery. The actual DM send and sender mapping registration happen in a background goroutine — delivery failures are reported via the UI status event stream. The sender-side mapping is registered automatically using the real DM message ID, so chunk_request frames from the receiver are guaranteed to match.

Request:
```json
{
  "to": "peer-identity-hex",
  "file_name": "report.pdf",
  "file_size": 1048576,
  "file_hash": "a1b2c3...",
  "content_type": "application/pdf",
  "body": "Quarterly report"
}
```

Required: `to`, `file_name`, `file_size`, `file_hash`.
Optional: `content_type` (defaults to `application/octet-stream`), `body` (defaults to `[file]` sentinel).

Error responses: HTTP 400 for missing/invalid parameters, HTTP 500 if pre-send validation fails (e.g. transmit file not found), HTTP 503 if DM routing is unavailable.

#### CLI

```bash
corsa-cli sendFileAnnounce to=<peer> file_name=report.pdf file_size=1048576 file_hash=a1b2c3...
```

#### Console

```
sendFileAnnounce <to> <file_name> <file_size> <file_hash> [content_type] [body]
```

### POST /rpc/v1/file/transfers

List active and pending file transfers (both sender and receiver sides). Terminal states (completed, tombstone, failed) are excluded. No arguments.

Use `fetchAllFileTransfers` (below) when terminal entries (history) are needed — for example to render the desktop file tab. The two endpoints share the same response schema; the only difference is whether terminal mappings are filtered.

#### CLI

```bash
corsa-cli fetchFileTransfers
```

#### Console

```
fetchFileTransfers
```

### POST /rpc/v1/file/transfers_all

List **all** file transfers (both sender and receiver sides), **including** terminal states (`completed`, `tombstone`, `failed`). Powers the desktop file tab's history view and any client that needs to display past transfers alongside active ones. No arguments.

The `state` field on each entry is the discriminator clients filter on (`downloading`, `verifying`, `waiting_ack`, `completed`, `failed`, etc.). The `file_id` field equals the originating DM `MessageID`, so clients can correlate a transfer entry with its chat message and use `delete_dm` to remove the announce on both sides — see [`docs/dm-commands.md`](../dm-commands.md). `TransmitPath` is never exposed.

Use `fetchFileTransfers` (above) when only active/pending transfers are required.

#### CLI

```bash
corsa-cli fetchAllFileTransfers
```

#### Console

```
fetchAllFileTransfers
```

### POST /rpc/v1/file/mapping

Show the sender FileMapping table. TransmitPath is never exposed through RPC. No arguments.

#### CLI

```bash
corsa-cli fetchFileMapping
```

#### Console

```
fetchFileMapping
```

### POST /rpc/v1/file/retry_chunk

Force retry of the current pending chunk request for a stalled or paused download. Accepts transfers in `downloading` or `waiting_route` state. When the transfer is paused in `waiting_route` and the sender is reachable again, the mapping transitions back to `downloading` and the next chunk is requested immediately — mirroring the automatic resume logic. If the sender is unreachable, the transfer transitions to (or stays in) `waiting_route` and the call returns an error.

Request:
```json
{
  "file_id": "uuid-of-file-transfer"
}
```

Required: `file_id`.

#### CLI

```bash
corsa-cli retryFileChunk file_id=<uuid>
```

#### Console

```
retryFileChunk <file_id>
```

### POST /rpc/v1/file/start_download

Start downloading a previously announced file. The file must have been registered via an incoming `file_announce` DM and be in `available` or `waiting_route` state.

Request:
```json
{
  "file_id": "uuid-of-file-transfer"
}
```

Required: `file_id`.

#### CLI

```bash
corsa-cli startFileDownload file_id=<uuid>
```

#### Console

```
startFileDownload <file_id>
```

### POST /rpc/v1/file/cancel_download

Cancel an active download, delete partial data, and reset the receiver mapping to available state.

Request:
```json
{
  "file_id": "uuid-of-file-transfer"
}
```

Required: `file_id`.

#### CLI

```bash
corsa-cli cancelFileDownload file_id=<uuid>
```

#### Console

```
cancelFileDownload <file_id>
```

### POST /rpc/v1/file/explain_route

Diagnostic command. Reports the ranked next-hop plan the file router would actually use when sending a file command to `<identity>`. Read-only — never enqueues, dials, or mutates state. Shape and ranking semantics are documented in [`docs/protocol/file_transfer.md`](../protocol/file_transfer.md#diagnostic-command-explainfileroute); the wire schema is duplicated below for quick reference.

The output mirrors `SendFileCommand`'s two-step strategy: a direct session to `<identity>`, when usable, is promoted to the head of the plan as a synthetic `next_hop == <identity>`, `hops == 1` entry and marked `best: true` unconditionally — even when a relay route advertises a higher `protocol_version`. Subsequent entries are the route-table candidates sorted by `protocol_version` DESC → `hops` ASC → `connected_at` ASC → `next_hop`. Self-routes, withdrawn / expired entries and stalled next-hops are dropped before ranking.

`protocol_version` in the response is the **normalized ranking value**, not the raw handshake-reported field. The wire source depends on direction — outbound-backed candidates carry `welcome.Version`, inbound-backed candidates carry `hello.Version` — but in either case a peer reporting a version higher than this build's `config.ProtocolVersion` is demoted by the inflated-version defence: its entry shows `protocol_version: 0` and falls to the bottom of the plan even though its real claim was higher. Treat `0` here as "ranking-untrusted" rather than "unknown"; the node-side meta logs at WARN level whenever the clamp fires, so the actual reported value is recoverable from the journal. See [Inflated-version defence](../protocol/file_transfer.md#inflated-version-defence) for the full rationale.

Request:
```json
{"identity": "peer-identity-hex"}
```

Required: `identity` (40-hex-character peer identity).

Response: a JSON array, one entry per next-hop in selection order. Empty array means no usable next-hop.

```jsonc
[
  {
    "next_hop": "<peer identity>",
    "hops": 1,
    "protocol_version": 7,
    "connected_at": "2025-01-01T12:34:56Z",  // omitted when unknown
    "uptime_seconds": 3600.5,                // 0 when connected_at omitted
    "best": true                             // true only on the first entry
  }
]
```

Error responses: HTTP 400 for missing or malformed `identity`, HTTP 500 if the file subsystem returns an error (e.g. not initialized).

#### CLI

```bash
corsa-cli explainFileRoute <identity>
```

#### Console

```
explainFileRoute <identity>
```

---

## Русский

### POST /rpc/v1/file/sendFileAnnounce

Отправка `file_announce` DM получателю. Файл должен уже существовать в директории transmit (`<dataDir>/transmit/<sha256>.<ext>`). Предотправочная валидация (наличие файла, квота отправителя) выполняется синхронно — при неудаче RPC возвращает HTTP 500 с сообщением об ошибке. При успехе ответ содержит `"status": "pending"`, что означает: предвалидация пройдена, announce поставлен в очередь на асинхронную доставку. Фактическая отправка DM и регистрация маппинга отправителя происходят в фоновой goroutine — ошибки доставки сообщаются через UI status event stream. Маппинг отправителя регистрируется автоматически с использованием реального ID DM-сообщения, поэтому chunk_request фреймы от получателя гарантированно совпадают.

Запрос:
```json
{
  "to": "peer-identity-hex",
  "file_name": "report.pdf",
  "file_size": 1048576,
  "file_hash": "a1b2c3...",
  "content_type": "application/pdf",
  "body": "Квартальный отчёт"
}
```

Обязательные: `to`, `file_name`, `file_size`, `file_hash`.
Опциональные: `content_type` (по умолчанию `application/octet-stream`), `body` (по умолчанию `[file]` sentinel).

Ответы об ошибках: HTTP 400 при отсутствующих/невалидных параметрах, HTTP 500 при неудаче предотправочной валидации (например, файл не найден в transmit), HTTP 503 если DM-маршрутизация недоступна.

#### CLI

```bash
corsa-cli sendFileAnnounce to=<peer> file_name=report.pdf file_size=1048576 file_hash=a1b2c3...
```

#### Консоль

```
sendFileAnnounce <to> <file_name> <file_size> <file_hash> [content_type] [body]
```

### POST /rpc/v1/file/transfers

Список активных и ожидающих файловых трансферов (стороны отправителя и получателя). Терминальные состояния (completed, tombstone, failed) исключаются. Без аргументов.

Используйте `fetchAllFileTransfers` (ниже), когда нужны терминальные записи (история) — например, для отрисовки file-вкладки в десктопе. Обе ручки используют одинаковую схему ответа; разница только в том, фильтруются ли терминальные маппинги.

#### CLI

```bash
corsa-cli fetchFileTransfers
```

#### Консоль

```
fetchFileTransfers
```

### POST /rpc/v1/file/transfers_all

Список **всех** файловых трансферов (стороны отправителя и получателя), **включая** терминальные состояния (`completed`, `tombstone`, `failed`). Используется file-вкладкой десктопа для истории и любым клиентом, которому нужно показывать завершённые трансферы рядом с активными. Без аргументов.

Поле `state` каждой записи — дискриминатор, по которому клиент фильтрует (`downloading`, `verifying`, `waiting_ack`, `completed`, `failed` и т.д.). Поле `file_id` равно `MessageID` исходного DM, поэтому клиенты могут связать запись трансфера с её чат-сообщением и удалить announce у обеих сторон через `delete_dm` — см. [`docs/dm-commands.md`](../dm-commands.md). `TransmitPath` никогда не раскрывается.

Используйте `fetchFileTransfers` (выше), если нужны только активные/ожидающие трансферы.

#### CLI

```bash
corsa-cli fetchAllFileTransfers
```

#### Консоль

```
fetchAllFileTransfers
```

### POST /rpc/v1/file/mapping

Показать таблицу FileMapping отправителя. TransmitPath никогда не раскрывается через RPC. Без аргументов.

#### CLI

```bash
corsa-cli fetchFileMapping
```

#### Консоль

```
fetchFileMapping
```

### POST /rpc/v1/file/retry_chunk

Принудительный retry текущего ожидающего chunk-запроса для зависшей или приостановленной загрузки. Принимает трансферы в состоянии `downloading` или `waiting_route`. Когда трансфер приостановлен в `waiting_route` и отправитель снова доступен, маппинг переходит обратно в `downloading` и следующий chunk запрашивается немедленно — аналогично автоматической логике возобновления. Если отправитель недоступен, трансфер переходит в (или остаётся в) `waiting_route` и вызов возвращает ошибку.

Запрос:
```json
{
  "file_id": "uuid-файлового-трансфера"
}
```

Обязательные: `file_id`.

#### CLI

```bash
corsa-cli retryFileChunk file_id=<uuid>
```

#### Консоль

```
retryFileChunk <file_id>
```

### POST /rpc/v1/file/start_download

Начать скачивание ранее объявленного файла. Файл должен быть зарегистрирован через входящий `file_announce` DM и находиться в состоянии `available` или `waiting_route`.

Запрос:
```json
{
  "file_id": "uuid-файлового-трансфера"
}
```

Обязательные: `file_id`.

#### CLI

```bash
corsa-cli startFileDownload file_id=<uuid>
```

#### Консоль

```
startFileDownload <file_id>
```

### POST /rpc/v1/file/cancel_download

Отмена активной загрузки, удаление частичных данных и возврат маппинга получателя в состояние available.

Запрос:
```json
{
  "file_id": "uuid-файлового-трансфера"
}
```

Обязательные: `file_id`.

#### CLI

```bash
corsa-cli cancelFileDownload file_id=<uuid>
```

#### Консоль

```
cancelFileDownload <file_id>
```

### POST /rpc/v1/file/explain_route

Диагностическая команда. Возвращает ранжированный план next-hop, который file router реально использовал бы при отправке файловой команды на `<identity>`. Read-only — ничего не ставит в очередь, не дозванивается и не меняет состояние. Семантика и формат описаны в [`docs/protocol/file_transfer.md`](../protocol/file_transfer.md#диагностическая-команда-explainfileroute); wire-схема продублирована ниже для быстрой справки.

Вывод зеркалит двухшаговую стратегию `SendFileCommand`: direct-сессия к `<identity>`, если пригодна, попадает в head плана как синтетическая запись `next_hop == <identity>`, `hops == 1` и помечается `best: true` безусловно — даже если у relay-маршрута объявлена более высокая `protocol_version`. Следующие записи — кандидаты из таблицы маршрутов, отсортированные по `protocol_version` DESC → `hops` ASC → `connected_at` ASC → `next_hop`. Self-маршруты, withdrawn/expired записи и stalled next-hop-ы выкидываются до ранжирования.

`protocol_version` в ответе — это **нормализованный ranking-key**, а не сырое значение из handshake-фрейма. Источник зависит от направления — outbound-кандидаты несут `welcome.Version`, inbound-кандидаты несут `hello.Version` — но в любом случае пир, заявивший версию выше нашей `config.ProtocolVersion`, демотируется защитой от inflated-version: его запись показывает `protocol_version: 0` и уезжает в самый низ плана, даже если реальное заявленное значение было больше. Считай `0` здесь «ranking-untrusted», а не «неизвестно»; meta на node-стороне пишет WARN-лог при каждом срабатывании клампа, так что оригинальное значение восстанавливается из журнала. Подробности см. [Защита от завышенной версии](../protocol/file_transfer.md#защита-от-завышенной-версии).

Запрос:
```json
{"identity": "peer-identity-hex"}
```

Обязательные: `identity` (40-символьный hex peer identity).

Ответ: JSON-массив, по одной записи на next-hop в порядке выбора. Пустой массив — нет пригодного next-hop.

```jsonc
[
  {
    "next_hop": "<peer identity>",
    "hops": 1,
    "protocol_version": 7,
    "connected_at": "2025-01-01T12:34:56Z",  // отсутствует, если неизвестен
    "uptime_seconds": 3600.5,                // 0, если connected_at отсутствует
    "best": true                             // true только у первой записи
  }
]
```

Ответы об ошибках: HTTP 400 при отсутствующем или некорректном `identity`, HTTP 500 если файловая подсистема вернула ошибку (например, не инициализирована).

#### CLI

```bash
corsa-cli explainFileRoute <identity>
```

#### Консоль

```
explainFileRoute <identity>
```
