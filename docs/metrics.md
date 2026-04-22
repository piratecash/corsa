# Metrics Layer

## English

### Overview

The metrics layer (`internal/core/metrics/`) is a standalone data collection service that periodically samples node statistics and maintains rolling history buffers. It operates independently from the network service and accesses data through the `TrafficSource` interface.

### Architecture

```mermaid
graph TB
    subgraph Node["node.Service"]
        MC["MeteredConn<br/>(wraps net.Conn)"]
        PH["peerHealth<br/>.BytesSent / .BytesReceived"]
        LT["liveTrafficLocked()<br/>(active connections)"]
        NSF["networkStatsFrame()<br/>aggregates per-peer + live"]
        MC -->|"accumulate on close"| PH
        MC -->|"live read"| LT
        PH --> NSF
        LT --> NSF
    end

    subgraph Metrics["metrics.Collector"]
        TH["TrafficHistory<br/>(ring buffer, 3600 slots)"]
        TICK["Ticker<br/>(1 sample/sec)"]
        SNAP["TrafficSnapshot()"]
    end

    subgraph RPC["RPC Layer"]
        CMD["fetch_traffic_history"]
        CMD_NS["fetch_network_stats"]
        CMD_PH["fetch_peer_health"]
        CMD_GP["get_peers"]
    end

    TICK -->|"every 1s"| NSF
    NSF -->|"total_sent, total_received"| TH
    SNAP -->|"Snapshot()"| TH
    CMD -->|"calls"| SNAP
    CMD_NS -->|"atomic snapshot load"| NSF
    CMD_PH -->|"atomic snapshot load"| Node
    CMD_GP -->|"atomic snapshot load"| Node
```

*Metrics data collection flow*

### Data Flow

1. Every TCP connection (inbound and outbound) is wrapped in `MeteredConn`, which atomically counts bytes read/written
2. When a connection closes, its final byte counts are accumulated into the peer's `peerHealth.BytesSent` / `peerHealth.BytesReceived`
3. While connections are active, `liveTrafficLocked()` reads current counters directly from `MeteredConn` instances
4. Three hot local RPCs (`fetch_network_stats`, `fetch_peer_health`, `get_peers`) each return a pre-built snapshot via a single `atomic.Pointer` load — `networkStatsSnapshot`, `peerHealthSnapshot`, `peersExchangeSnapshot`, and `cmSlotsSnapshot` respectively. The fourth snapshot caches `ConnectionManager.Slots()` so `peerHealthFrames` and `buildPeerExchangeResponse` never call `Slots()` on the RPC path — `Slots()` takes `cm.mu.RLock`, and Go's writer-preferring `sync.RWMutex` would otherwise serialise these RPC readers behind any queued CM-writer just like `s.mu` used to. Snapshots are primed **synchronously** by `primeHotReadSnapshots()` in `Run()` before the TCP listener opens, then refreshed every 500 ms by `hotReadsRefreshLoop` — the only path that takes `s.mu.RLock` (first three snapshots) or `cm.mu.RLock` (fourth) to read `health` / `peers` / `persistedMeta` / live counters / slot state. The RPC handlers themselves never touch `s.mu` or `cm.mu` and **do not fall back to a synchronous rebuild** on a snapshot miss — the previous fallback re-coupled the hot path to the very locks the snapshot infrastructure was meant to bypass. With the prime step in place, every hot-path handler observes a non-nil snapshot on its first load (see `bug-self-loopback-reconnect-storm.md` §B.6)
5. The `metrics.Collector` calls `fetch_network_stats` every second and records the totals into a ring buffer (`TrafficHistory`)
6. RPC clients call `fetch_traffic_history` to get the full 1-hour rolling window

Snapshot staleness is bounded by `networkStatsSnapshotInterval` (500 ms) for all four snapshots. Even if a writer holds `s.mu.Lock` or `cm.mu.Lock` for many seconds, the RPCs keep serving the last successfully-built snapshot instead of blocking — clients prefer bounded-stale data over a frozen UI. The four rebuilds run in independent per-snapshot goroutines inside `hotReadsRefreshLoop`, each on its own 500 ms ticker. The fan-out isolates slow rebuilds (notably `peersExchangeSnapshot`, which re-acquires `s.mu.RLock` via `peerProvider.Candidates()` callbacks, and `cmSlotsSnapshot`, which takes `cm.mu.RLock`) so they do not delay the other three snapshots' refreshes or widen their staleness windows.

### TrafficHistory Ring Buffer

The ring buffer stores up to 3600 samples (1 hour at 1 sample/second). Each sample contains:

| Field | Description |
|---|---|
| `timestamp` | UTC time when the sample was recorded |
| `bytes_sent_ps` | Delta: bytes sent since the previous sample |
| `bytes_recv_ps` | Delta: bytes received since the previous sample |
| `total_sent` | Cumulative bytes sent at this moment |
| `total_received` | Cumulative bytes received at this moment |

When the buffer is full, new samples overwrite the oldest entries. `Snapshot()` returns samples in chronological order (oldest first).

#### Baseline seeding

Delta computation requires a baseline (`prevSent` / `prevRecv`). The baseline starts at zero and is updated on every `Record`. This default works when the collector starts together with the source — the first `Record` reports the bytes that flowed since startup.

When the collector attaches to a source whose counters are already non-zero, callers must invoke `Collector.Seed()` once before the first ticker tick. `Seed` reads current totals and sets them as baseline without recording a sample. Without `Seed`, the first sample would dump the entire pre-attach cumulative as a single-second spike.

`desktop`, `node`, and `sdk` runtimes call `Seed` between collector creation and the first `Run` tick so the bootstrap handshake traffic appears as a real per-second delta on the first chart bar instead of being lost (the previous skip-on-first behavior) or distorted (delta == totals).

### RPC Commands

| Command | Category | Description |
|---|---|---|
| `fetch_traffic_history` | metrics | Rolling 1-hour traffic history (per-second samples) |
| `fetch_network_stats` | network | Current aggregated traffic per peer and total |
| `fetch_peer_health` | network | Per-peer health including traffic counters |

### Desktop UI — Traffic Tab

The console window includes a Traffic tab that visualizes network activity in real time. On tab open, the full history is loaded from `fetch_traffic_history` (up to 3600 samples). While the tab is active, a 1-second ticker appends new samples via `fetch_network_stats`.

The graph contains three visual elements:

- **In bars (green)** — received bytes per second, drawn as vertical bars on the left side of each sample slot
- **Out bars (blue)** — sent bytes per second, drawn as vertical bars on the right side of each sample slot
- **Total line (yellow)** — sum of In + Out, drawn as a continuous line connecting all non-zero sample points. The line bridges zero-traffic gaps to maintain visual continuity across bar clusters

All samples are spread across the full graph width. The Y-axis auto-scales to the maximum total value (with 10% headroom) and displays 4 grid lines with value labels. Inside the graph (top-right corner), cumulative totals (Total In / Total Out) are shown as badges.

The metrics collector runs independently from the UI — it always samples data in the background. Opening the Traffic tab loads the existing history rather than starting from zero.

---

## Русский

### Обзор

Слой метрик (`internal/core/metrics/`) — это автономный сервис сбора данных, который периодически снимает статистику ноды и хранит её в кольцевых буферах. Он работает независимо от сетевого сервиса и обращается к данным через интерфейс `TrafficSource`.

### Архитектура

```mermaid
graph TB
    subgraph Node["node.Service"]
        MC["MeteredConn<br/>(обёртка над net.Conn)"]
        PH["peerHealth<br/>.BytesSent / .BytesReceived"]
        LT["liveTrafficLocked()<br/>(активные соединения)"]
        NSF["networkStatsFrame()<br/>агрегация по пирам + live"]
        MC -->|"аккумуляция при закрытии"| PH
        MC -->|"live чтение"| LT
        PH --> NSF
        LT --> NSF
    end

    subgraph Metrics["metrics.Collector"]
        TH["TrafficHistory<br/>(кольцевой буфер, 3600 слотов)"]
        TICK["Тикер<br/>(1 семпл/сек)"]
        SNAP["TrafficSnapshot()"]
    end

    subgraph RPC["RPC слой"]
        CMD["fetch_traffic_history"]
        CMD_NS["fetch_network_stats"]
        CMD_PH["fetch_peer_health"]
        CMD_GP["get_peers"]
    end

    TICK -->|"каждую 1 сек"| NSF
    NSF -->|"total_sent, total_received"| TH
    SNAP -->|"Snapshot()"| TH
    CMD -->|"вызывает"| SNAP
    CMD_NS -->|"atomic snapshot load"| NSF
    CMD_PH -->|"atomic snapshot load"| Node
    CMD_GP -->|"atomic snapshot load"| Node
```

*Диаграмма сбора данных метрик*

### Поток данных

1. Каждое TCP-соединение (входящее и исходящее) оборачивается в `MeteredConn`, который атомарно считает прочитанные/записанные байты
2. При закрытии соединения финальные счётчики аккумулируются в `peerHealth.BytesSent` / `peerHealth.BytesReceived`
3. Пока соединения активны, `liveTrafficLocked()` читает текущие счётчики напрямую из экземпляров `MeteredConn`
4. Три hot local RPC (`fetch_network_stats`, `fetch_peer_health`, `get_peers`) отдают заранее подготовленный snapshot одним `atomic.Pointer`-load'ом — `networkStatsSnapshot`, `peerHealthSnapshot`, `peersExchangeSnapshot` и `cmSlotsSnapshot` соответственно. Четвёртый snapshot кэширует `ConnectionManager.Slots()`, чтобы `peerHealthFrames` и `buildPeerExchangeResponse` не звали `Slots()` на RPC-пути — `Slots()` берёт `cm.mu.RLock`, и writer-preferring `sync.RWMutex` в Go иначе сериализовал бы этих RPC-читателей за любым queued CM-writer'ом ровно той же формы, что `s.mu` раньше. Snapshot'ы **синхронно** инициализируются в `Run()` вызовом `primeHotReadSnapshots()` ДО открытия TCP-listener'а, и далее пересобираются каждые 500 мс в `hotReadsRefreshLoop` — это единственный путь, который берёт `s.mu.RLock` (первые три snapshot'а) или `cm.mu.RLock` (четвёртый) для чтения `health` / `peers` / `persistedMeta` / live-счётчиков / slot state. Сами RPC-handler'ы не касаются ни `s.mu`, ни `cm.mu`, и **не делают синхронный rebuild на miss** — прежний fallback как раз возвращал hot-path обратно на те самые локи, от которых snapshot-инфраструктура должна была его отвязать. С prime-шагом каждый handler на первом же load'е видит непустой snapshot (см. `bug-self-loopback-reconnect-storm.md` §B.6)
5. `metrics.Collector` вызывает `fetch_network_stats` каждую секунду и записывает итоги в кольцевой буфер (`TrafficHistory`)
6. RPC-клиенты вызывают `fetch_traffic_history` для получения полного часового окна

Максимальная устаревшесть каждого snapshot'а ограничена `networkStatsSnapshotInterval` (500 мс) для всех четырёх snapshot'ов. Даже если writer держит `s.mu.Lock` или `cm.mu.Lock` много секунд, RPC продолжает отдавать последний успешно построенный snapshot вместо того, чтобы блокироваться — клиент предпочитает bounded-stale данные замёрзшему UI. Четыре rebuild'а выполняются в независимых под-горутинах внутри `hotReadsRefreshLoop`, по одной на snapshot, каждая со своим 500 мс тикером. Fan-out изолирует медленные rebuild'ы (особенно `peersExchangeSnapshot`, который повторно берёт `s.mu.RLock` через callback'и `peerProvider.Candidates()`, и `cmSlotsSnapshot`, который берёт `cm.mu.RLock`), так что они не задерживают refresh остальных трёх snapshot'ов и не расширяют их окна staleness.

### Кольцевой буфер TrafficHistory

Буфер хранит до 3600 семплов (1 час при 1 семпл/секунду). Каждый семпл содержит:

| Поле | Описание |
|---|---|
| `timestamp` | UTC-время записи семпла |
| `bytes_sent_ps` | Дельта: байты отправленные с предыдущего семпла |
| `bytes_recv_ps` | Дельта: байты полученные с предыдущего семпла |
| `total_sent` | Кумулятивные байты отправленные на этот момент |
| `total_received` | Кумулятивные байты полученные на этот момент |

Когда буфер заполнен, новые семплы перезаписывают самые старые. `Snapshot()` возвращает семплы в хронологическом порядке (от старых к новым).

#### Сидирование baseline

Расчёт дельты требует baseline (`prevSent` / `prevRecv`). Baseline начинается с нуля и обновляется при каждом `Record`. Этот вариант корректен когда коллектор стартует одновременно с источником — первый `Record` показывает байты, прошедшие с момента старта.

Когда коллектор подключается к источнику, в котором счётчики уже ненулевые, вызывающий обязан выполнить `Collector.Seed()` один раз до первого tick'а тикера. `Seed` читает текущие totals и записывает их как baseline без добавления семпла. Без `Seed` первый семпл сбросит весь pre-attach кумулятив одним выбросом за секунду.

Runtimes `desktop`, `node` и `sdk` вызывают `Seed` между созданием коллектора и первым `Run`-тиком, чтобы bootstrap-трафик handshake'ов появился реальной посекундной дельтой на первом баре графика, а не потерялся (старое поведение skip-on-first) и не исказился (delta == totals).

### RPC-команды

| Команда | Категория | Описание |
|---|---|---|
| `fetch_traffic_history` | metrics | Часовая история трафика (посекундные семплы) |
| `fetch_network_stats` | network | Текущий агрегированный трафик по пирам и общий |
| `fetch_peer_health` | network | Здоровье пиров включая счётчики трафика |

### Desktop UI — вкладка Traffic

Окно консоли содержит вкладку Traffic для визуализации сетевой активности в реальном времени. При открытии вкладки загружается полная история из `fetch_traffic_history` (до 3600 семплов). Пока вкладка активна, тикер раз в секунду добавляет новые семплы через `fetch_network_stats`.

График содержит три визуальных элемента:

- **Бары In (зелёные)** — полученные байты в секунду, рисуются как вертикальные столбцы слева в каждом слоте сэмпла
- **Бары Out (синие)** — отправленные байты в секунду, рисуются как вертикальные столбцы справа в каждом слоте сэмпла
- **Линия Total (жёлтая)** — сумма In + Out, рисуется как непрерывная линия, соединяющая все ненулевые точки. Линия перекрывает участки с нулевым трафиком для визуальной непрерывности между кластерами баров

Все семплы распределяются на всю ширину графика. Ось Y автоматически масштабируется до максимального значения total (с запасом 10%) и отображает 4 линии сетки с подписями значений. Внутри графика (правый верхний угол) показаны кумулятивные итоги (Всего вх / Всего исх) в виде бейджей.

Коллектор метрик работает независимо от UI — он всегда собирает данные в фоне. Открытие вкладки Traffic загружает существующую историю, а не начинает с нуля.
