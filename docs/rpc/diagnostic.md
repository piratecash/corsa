# Diagnostic Commands

## English

Diagnostic commands provide runtime inspection and traffic capture for peer connections. These commands are available when the node is running with capture support enabled (desktop mode). On standalone nodes without capture support, these commands return 503 Service Unavailable.

All commands belong to the `diagnostic` category and are visible in `help` output.

### recordPeerTrafficByConnID

Start recording wire traffic for specific connection IDs.

Usage: `recordPeerTrafficByConnID conn_ids=41,42 [format=compact|pretty]`

Request:
```json
{"command": "recordPeerTrafficByConnID", "args": {"conn_ids": "41,42", "format": "compact"}}
```

Arguments:

- `conn_ids` (required) — comma-separated list of `conn_id` values (uint64). Only currently live connections are matched; missing IDs appear in `not_found`.
- `format` (optional) — capture file format: `compact` (default) or `pretty`. Compact writes one line per event (`<ts> <dir>: <json>`). Pretty indents JSON payloads for human reading.

Response:
```json
{
  "started": [{"conn_id": 41, "remote_ip": "203.0.113.10", "peer_direction": "outbound", "format": "compact", "file_path": "/data/debug/traffic-captures/..."}],
  "already_active": [],
  "not_found": ["conn_id=42"],
  "conflicts": [],
  "errors": []
}
```

Idempotency: if recording is already active for a `conn_id` with the same format, it appears in `already_active`. If the format differs, it appears in `conflicts`. To change format, stop then restart.

### recordPeerTrafficByIP

Start recording wire traffic for all current (and future) connections matching the given remote IPs. Installs a standing rule so that new connections from the same IP are automatically captured.

Usage: `recordPeerTrafficByIP ips=203.0.113.10,198.51.100.7 [format=compact|pretty]`

Request:
```json
{"command": "recordPeerTrafficByIP", "args": {"ips": "203.0.113.10", "format": "compact"}}
```

Arguments:

- `ips` (required) — comma-separated list of remote IP addresses.
- `format` (optional) — `compact` (default) or `pretty`.

Response includes `started`, `already_active`, `installed_rules`, `not_found`, `conflicts`, `errors`. The `installed_rules` array describes each rule with its `scope`, `target`, `format`, `created_at`, and `matched_conn_ids`.

### recordAllPeerTraffic

Start recording wire traffic for all current and future peer connections. Installs a global standing rule.

Usage: `recordAllPeerTraffic [format=compact|pretty]`

Request:
```json
{"command": "recordAllPeerTraffic", "args": {"format": "pretty"}}
```

Arguments:

- `format` (optional) — `compact` (default) or `pretty`.

Response includes `started`, `installed_rules`, `conflicts`, `errors`.

### stopPeerTrafficRecording

Stop active recordings. At least one argument is required; calling without arguments returns a validation error.

Usage: `stopPeerTrafficRecording conn_ids=41,42 | ips=203.0.113.10 | scope=all`

Request (stop by conn_ids):
```json
{"command": "stopPeerTrafficRecording", "args": {"conn_ids": "41,42"}}
```

Request (stop by IP — also removes the standing rule):
```json
{"command": "stopPeerTrafficRecording", "args": {"ips": "203.0.113.10"}}
```

Request (stop everything — all sessions and all rules):
```json
{"command": "stopPeerTrafficRecording", "args": {"scope": "all"}}
```

Arguments (one required):

- `conn_ids` — comma-separated list of `conn_id` values to stop.
- `ips` — comma-separated list of IPs whose sessions and rules to stop/remove.
- `scope=all` — stops all active sessions and removes all standing rules.

Response:
```json
{
  "stopped": [{"conn_id": 41, "remote_ip": "203.0.113.10", "peer_direction": "outbound", "format": "compact", "file_path": "..."}],
  "removed_rules": [{"scope": "ip", "target": "203.0.113.10", "format": "compact", "created_at": "...", "matched_conn_ids": [41]}],
  "not_found": []
}
```

### Aliases

All four commands support snake_case aliases for console and automation convenience:

| Canonical | Snake_case alias |
|---|---|
| `recordPeerTrafficByConnID` | `record_peer_traffic_by_conn_id` |
| `recordPeerTrafficByIP` | `record_peer_traffic_by_ip` |
| `recordAllPeerTraffic` | `record_all_peer_traffic` |
| `stopPeerTrafficRecording` | `stop_peer_traffic_recording` |

### Capture File Format

Each `conn_id` produces a separate `.jsonl` file in `<data_dir>/debug/traffic-captures/<session-timestamp>/`.

File name pattern: `<started_at>__ip=<remote-ip>__conn=<conn-id>__peerdir=<inbound|outbound>.jsonl`

Compact format (one event per line):
```
2026-04-14T12:44:03.412Z out: {"type":"ping"}
2026-04-14T12:44:03.981Z in: {"type":"pong"}
2026-04-14T12:44:04.100Z out [write_failed]: {"type":"ping"}
```

Pretty format (indented JSON, blank line between events):
```
2026-04-14T12:44:03.412Z out:
{
  "type": "ping"
}

2026-04-14T12:44:03.981Z in:
{
  "type": "pong"
}
```

Non-JSON and malformed payloads are recorded as-is with the same timestamp + direction header.

### Limits

- Maximum 1024 concurrent capture sessions per process.
- Per-session ring buffer: 256 events. On overflow the oldest event is dropped (not the newest), and `recording_dropped_events` is incremented.
- Capture files are not rotated or compressed in the first version.
- Capture state is runtime-only and does not survive application restart.

---

## Русский

Диагностические команды предоставляют runtime-инспекцию и запись трафика peer-соединений. Эти команды доступны когда нода работает с поддержкой capture (desktop-режим). На standalone-нодах без поддержки capture команды возвращают 503 Service Unavailable.

Все команды принадлежат категории `diagnostic` и видны в выводе `help`.

### recordPeerTrafficByConnID

Запуск записи wire-трафика для конкретных connection ID.

Использование: `recordPeerTrafficByConnID conn_ids=41,42 [format=compact|pretty]`

Запрос:
```json
{"command": "recordPeerTrafficByConnID", "args": {"conn_ids": "41,42", "format": "compact"}}
```

Аргументы:

- `conn_ids` (обязательный) — список `conn_id` через запятую (uint64). Сопоставляются только текущие live-соединения; отсутствующие ID попадают в `not_found`.
- `format` (необязательный) — формат файла: `compact` (по умолчанию) или `pretty`. Compact пишет одну строку на событие (`<ts> <dir>: <json>`). Pretty форматирует JSON с отступами для чтения глазами.

Ответ:
```json
{
  "started": [{"conn_id": 41, "remote_ip": "203.0.113.10", "peer_direction": "outbound", "format": "compact", "file_path": "/data/debug/traffic-captures/..."}],
  "already_active": [],
  "not_found": ["conn_id=42"],
  "conflicts": [],
  "errors": []
}
```

Идемпотентность: если запись уже активна для `conn_id` с тем же форматом, он появляется в `already_active`. Если формат отличается — в `conflicts`. Для смены формата нужно сначала остановить, потом перезапустить.

### recordPeerTrafficByIP

Запуск записи wire-трафика для всех текущих (и будущих) соединений с указанными remote IP. Устанавливает постоянное правило, чтобы новые соединения с того же IP автоматически записывались.

Использование: `recordPeerTrafficByIP ips=203.0.113.10,198.51.100.7 [format=compact|pretty]`

Аргументы:

- `ips` (обязательный) — список remote IP через запятую.
- `format` (необязательный) — `compact` (по умолчанию) или `pretty`.

Ответ включает `started`, `already_active`, `installed_rules`, `not_found`, `conflicts`, `errors`. Массив `installed_rules` описывает каждое правило с полями `scope`, `target`, `format`, `created_at`, `matched_conn_ids`.

### recordAllPeerTraffic

Запуск записи wire-трафика для всех текущих и будущих peer-соединений. Устанавливает глобальное постоянное правило.

Использование: `recordAllPeerTraffic [format=compact|pretty]`

Аргументы:

- `format` (необязательный) — `compact` (по умолчанию) или `pretty`.

### stopPeerTrafficRecording

Остановка активных записей. Требуется хотя бы один аргумент; вызов без аргументов возвращает ошибку валидации.

Использование: `stopPeerTrafficRecording conn_ids=41,42 | ips=203.0.113.10 | scope=all`

Аргументы (один обязательный):

- `conn_ids` — список `conn_id` через запятую для остановки.
- `ips` — список IP, чьи сессии и правила нужно остановить/удалить.
- `scope=all` — останавливает все активные сессии и удаляет все постоянные правила.

### Псевдонимы (aliases)

| Каноническое имя | Snake_case alias |
|---|---|
| `recordPeerTrafficByConnID` | `record_peer_traffic_by_conn_id` |
| `recordPeerTrafficByIP` | `record_peer_traffic_by_ip` |
| `recordAllPeerTraffic` | `record_all_peer_traffic` |
| `stopPeerTrafficRecording` | `stop_peer_traffic_recording` |

### Формат файлов записи

Каждый `conn_id` создаёт отдельный `.jsonl` файл в `<data_dir>/debug/traffic-captures/<session-timestamp>/`.

Шаблон имени файла: `<started_at>__ip=<remote-ip>__conn=<conn-id>__peerdir=<inbound|outbound>.jsonl`

Compact-формат (одно событие на строку):
```
2026-04-14T12:44:03.412Z out: {"type":"ping"}
2026-04-14T12:44:03.981Z in: {"type":"pong"}
2026-04-14T12:44:04.100Z out [write_failed]: {"type":"ping"}
```

Pretty-формат (JSON с отступами, пустая строка между событиями):
```
2026-04-14T12:44:03.412Z out:
{
  "type": "ping"
}

2026-04-14T12:44:03.981Z in:
{
  "type": "pong"
}
```

Не-JSON и некорректные payload'ы записываются как есть с тем же заголовком timestamp + direction.

### Ограничения

- Максимум 1024 одновременных capture-сессий на процесс.
- Ring buffer на сессию: 256 событий. При переполнении удаляется самое старое событие (не новейшее), `recording_dropped_events` инкрементируется.
- Файлы записи не ротируются и не сжимаются в первой версии.
- Состояние capture хранится только в runtime и не переживает перезапуск приложения.
