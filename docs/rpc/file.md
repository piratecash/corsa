# File Transfer Commands

## English

### POST /rpc/v1/file/send_file_announce

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
corsa-cli send_file_announce to=<peer> file_name=report.pdf file_size=1048576 file_hash=a1b2c3...
```

#### Console

```
send_file_announce <to> <file_name> <file_size> <file_hash> [content_type] [body]
```

### POST /rpc/v1/file/transfers

List active and pending file transfers (both sender and receiver sides). Terminal states (completed, tombstone, failed) are excluded. No arguments.

#### CLI

```bash
corsa-cli fetch_file_transfers
```

#### Console

```
fetch_file_transfers
```

### POST /rpc/v1/file/mapping

Show the sender FileMapping table. TransmitPath is never exposed through RPC. No arguments.

#### CLI

```bash
corsa-cli fetch_file_mapping
```

#### Console

```
fetch_file_mapping
```

### POST /rpc/v1/file/retry_chunk

Force retry of the current pending chunk request for a stalled or paused download. Accepts transfers in `downloading` or `waiting_route` state. When the transfer is paused in `waiting_route` and the sender is reachable again, the mapping transitions back to `downloading` and the next chunk is requested immediately — mirroring the automatic resume logic. If the sender is unreachable, the transfer transitions to (or stays in) `waiting_route` and the call returns an error. Also respects the concurrent download limit when resuming from `waiting_route`.

Request:
```json
{
  "file_id": "uuid-of-file-transfer"
}
```

Required: `file_id`.

#### CLI

```bash
corsa-cli retry_file_chunk file_id=<uuid>
```

#### Console

```
retry_file_chunk <file_id>
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
corsa-cli start_file_download file_id=<uuid>
```

#### Console

```
start_file_download <file_id>
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
corsa-cli cancel_file_download file_id=<uuid>
```

#### Console

```
cancel_file_download <file_id>
```

---

## Русский

### POST /rpc/v1/file/send_file_announce

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
corsa-cli send_file_announce to=<peer> file_name=report.pdf file_size=1048576 file_hash=a1b2c3...
```

#### Консоль

```
send_file_announce <to> <file_name> <file_size> <file_hash> [content_type] [body]
```

### POST /rpc/v1/file/transfers

Список активных и ожидающих файловых трансферов (стороны отправителя и получателя). Терминальные состояния (completed, tombstone, failed) исключаются. Без аргументов.

#### CLI

```bash
corsa-cli fetch_file_transfers
```

#### Консоль

```
fetch_file_transfers
```

### POST /rpc/v1/file/mapping

Показать таблицу FileMapping отправителя. TransmitPath никогда не раскрывается через RPC. Без аргументов.

#### CLI

```bash
corsa-cli fetch_file_mapping
```

#### Консоль

```
fetch_file_mapping
```

### POST /rpc/v1/file/retry_chunk

Принудительный retry текущего ожидающего chunk-запроса для зависшей или приостановленной загрузки. Принимает трансферы в состоянии `downloading` или `waiting_route`. Когда трансфер приостановлен в `waiting_route` и отправитель снова доступен, маппинг переходит обратно в `downloading` и следующий chunk запрашивается немедленно — аналогично автоматической логике возобновления. Если отправитель недоступен, трансфер переходит в (или остаётся в) `waiting_route` и вызов возвращает ошибку. Также учитывает лимит одновременных загрузок при возобновлении из `waiting_route`.

Запрос:
```json
{
  "file_id": "uuid-файлового-трансфера"
}
```

Обязательные: `file_id`.

#### CLI

```bash
corsa-cli retry_file_chunk file_id=<uuid>
```

#### Консоль

```
retry_file_chunk <file_id>
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
corsa-cli start_file_download file_id=<uuid>
```

#### Консоль

```
start_file_download <file_id>
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
corsa-cli cancel_file_download file_id=<uuid>
```

#### Консоль

```
cancel_file_download <file_id>
```
