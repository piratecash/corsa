# Metrics Commands

## English

### POST /rpc/v1/metrics/traffic_history

Rolling traffic history (1 sample/sec, 1 hour window).

Response contains an array of per-second samples with bytes sent/received deltas and cumulative totals. The buffer holds up to 3600 entries (1 hour). Samples are ordered oldest-first.

Optional `since` argument (RFC3339 timestamp): returns only samples strictly newer than the given timestamp. Used by the desktop traffic chart as an incremental cursor — each tick fetches just the tail recorded since the last applied sample. Over HTTP, pass it in the JSON body: `{"since": "2026-06-11T10:00:00Z"}`; a malformed timestamp returns 400.

Unavailable (503) when MetricsProvider is nil (standalone node without metrics collection).

#### CLI

```bash
corsa-cli fetchTrafficHistory
corsa-cli fetchTrafficHistory since=2026-06-11T10:00:00Z
```

#### Console

```
fetchTrafficHistory
fetchTrafficHistory since=2026-06-11T10:00:00Z
```

---

## Русский

### POST /rpc/v1/metrics/traffic_history

История трафика (1 семпл/сек, окно 1 час).

Ответ содержит массив посекундных семплов с дельтами байт отправленных/полученных и кумулятивными итогами. Буфер вмещает до 3600 записей (1 час). Семплы отсортированы от старых к новым.

Необязательный аргумент `since` (RFC3339): возвращает только семплы строго новее указанной метки времени. Используется графиком трафика в десктоп-консоли как инкрементальный курсор — каждый тик забирает лишь хвост, записанный после последнего применённого семпла. По HTTP передаётся в JSON-теле: `{"since": "2026-06-11T10:00:00Z"}`; некорректная метка времени вернёт 400.

Недоступна (503) когда MetricsProvider равен nil (standalone-нода без сбора метрик).

#### CLI

```bash
corsa-cli fetchTrafficHistory
corsa-cli fetchTrafficHistory since=2026-06-11T10:00:00Z
```

#### Консоль

```
fetchTrafficHistory
fetchTrafficHistory since=2026-06-11T10:00:00Z
```
