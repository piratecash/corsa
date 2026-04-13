# Metrics Commands

## English

### POST /rpc/v1/metrics/traffic_history

Rolling traffic history (1 sample/sec, 1 hour window).

Response contains an array of per-second samples with bytes sent/received deltas and cumulative totals. The buffer holds up to 3600 entries (1 hour). Samples are ordered oldest-first.

Unavailable (503) when MetricsProvider is nil (standalone node without metrics collection).

#### CLI

```bash
corsa-cli fetchTrafficHistory
```

#### Console

```
fetchTrafficHistory
```

---

## Русский

### POST /rpc/v1/metrics/traffic_history

История трафика (1 семпл/сек, окно 1 час).

Ответ содержит массив посекундных семплов с дельтами байт отправленных/полученных и кумулятивными итогами. Буфер вмещает до 3600 записей (1 час). Семплы отсортированы от старых к новым.

Недоступна (503) когда MetricsProvider равен nil (standalone-нода без сбора метрик).

#### CLI

```bash
corsa-cli fetchTrafficHistory
```

#### Консоль

```
fetchTrafficHistory
```
