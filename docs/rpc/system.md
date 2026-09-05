# System Commands

## English

### POST /rpc/v1/system/help

List all available commands.

Response:
```json
{
  "version": "1.0",
  "commands": [
    {"name": "help", "description": "List all available RPC commands", "category": "system"},
    {"name": "getPeers", "description": "Get list of connected peers", "category": "network"}
  ]
}
```

`version` is the help response schema version (currently `"1.0"`). It tracks the structure of the help response itself — not the protocol or client version. Clients can use it to detect format changes (e.g. new fields in command metadata). Bump when the help response structure changes.

### POST /rpc/v1/system/ping

Send local ping, returns pong response. The same semantic result regardless of transport (HTTP RPC or in-process desktop).

### POST /rpc/v1/system/hello

Send hello frame for identification. The handler populates `Version`, `MinimumProtocolVersion`, `Client`, `ClientVersion`, and `ClientBuild` before forwarding to the node — without these fields the handshake is rejected. The standalone node handler identifies as `Client: "rpc"`; in desktop mode, `RegisterDesktopOverrides` registers a transport-level override that identifies as `Client: "desktop"` with the desktop application version. This is not a semantic fork — it changes only transport identity metadata. `ClientBuild` is a monotonically increasing integer that peers compare to detect newer software releases.

### POST /rpc/v1/system/version

Client and protocol version.

Response:
```json
{
  "client_version": "0.0.43",
  "protocol_version": 14,
  "node_address": "a1b2c3d4..."
}
```

### POST /rpc/v1/system/node_status

PIP-0001 integration surface — single authenticated probe consumed by
PirateCash Core at masternode startup (Stage 1) and during the v20
liveness checks (Stage 2). The `public_key` field is the long-term
identifier the v21 proof-of-service will sign challenges with.

Equivalent dispatch: `POST /rpc/v1/exec` with `{"command": "getNodeStatus"}`.
Snake_case aliases `node_status` and `get_node_status` resolve to the
same handler; case-insensitive matching additionally accepts spellings
like `nodestatus`. Route-based callers may use either
`/rpc/v1/system/node_status` or `/rpc/v1/system/nodestatus`.

Response:
```json
{
  "identity": "a1b2c3d4...",
  "address": "a1b2c3d4...",
  "public_key": "Q...=",
  "box_public_key": "B...=",
  "protocol_version": 14,
  "minimum_protocol_version": 14,
  "client_version": "0.0.43",
  "client_build": 43,
  "connected_peers": 4,
  "started_at": "2026-04-30T12:00:00Z",
  "uptime_seconds": 3742,
  "current_time": "2026-04-30T13:02:22Z"
}
```

Field reference:

- `identity` — ed25519 fingerprint, the node's stable routing identifier
  and the primary field PirateCash Core should consume.
- `address` — ed25519 fingerprint, the node's stable routing identifier.
- `public_key` — base64-encoded ed25519 public key behind `address`.
- `box_public_key` — base64-encoded curve25519 public key for E2E DM
  encryption.
- `protocol_version` / `minimum_protocol_version` — wire protocol the
  node currently speaks and the floor it accepts on inbound peers.
- `client_version` / `client_build` — implementation identifiers.
  `client_build` is a monotonically increasing integer suitable for
  policy gates.
- `connected_peers` — number of **distinct peer identities** the node
  currently has at least one live connection with, counting both
  outbound and inbound sessions. Multiple sockets from the same peer
  identity collapse to one (the field measures relay reach, not socket
  count; use `getActiveConnections` for per-connection detail). A
  masternode reporting zero usable peers is unlikely to be relaying
  messenger traffic (Stage 2 service-health hint).
- `started_at` / `uptime_seconds` / `current_time` — RFC3339Nano UTC
  timestamps plus a whole-seconds convenience field. PirateCash Core
  uses these to detect clock drift between its own host and the
  masternode.

Security guarantees enforced by tests:

- Only public key material crosses the wire. The endpoint MUST NOT
  carry private keys, seeds, RPC credentials, or other secrets.
- Authenticated localhost RPC only. Same auth gate as every other
  command in this document — see [../rpc.md](../rpc.md).
- No signature attached. PIP-0001 stages 1 and 2 do not require one;
  the v21 proof-of-service will be exposed as a separate signed
  artifact rather than retrofitted onto this snapshot, so health-check
  loops stay cheap.

### POST /rpc/v1/exec — `getResourceUsage`

Process memory footprint, cgroup (container) memory, live connection
count, and uptime. Both machine-readable integers and human-formatted strings are
returned so dashboards consume the raw numbers while operators reading
the JSON get sensible units. Snake_case aliases `resource_usage` and
`get_resource_usage` resolve to the same handler.

Response:
```json
{
  "mem_sys_bytes": 62390272,
  "mem_sys_human": "59.50 MB",
  "mem_heap_alloc_bytes": 41943040,
  "mem_heap_alloc_human": "40.00 MB",
  "heap_inuse_bytes": 45088768,
  "heap_inuse_human": "43.00 MB",
  "heap_idle_bytes": 12582912,
  "heap_idle_human": "12.00 MB",
  "heap_released_bytes": 8388608,
  "heap_released_human": "8.00 MB",
  "gc_sys_bytes": 4194304,
  "gc_sys_human": "4.00 MB",
  "cgroup_mem_limit_bytes": 536870912,
  "cgroup_mem_limit_human": "512.00 MB",
  "cgroup_mem_usage_bytes": 157286400,
  "cgroup_mem_usage_human": "150.00 MB",
  "connection_count": 12,
  "uptime_seconds": 192600,
  "uptime_human": "2.23 d",
  "sampled_at": "2026-06-06T05:15:47.123456789Z"
}
```

Field notes:

- `mem_sys_bytes` / `mem_sys_human` — total memory obtained from the OS
  (`runtime.MemStats.Sys`), the closest runtime-visible proxy for the
  process footprint / RSS. The headline "memory used" figure.
- `mem_heap_alloc_bytes` / `mem_heap_alloc_human` — live (in-use) heap
  (`runtime.MemStats.HeapAlloc`). This is the figure that climbs
  steadily under a memory leak — watch it across calls.
- `heap_inuse_bytes` / `heap_inuse_human` — bytes in in-use heap spans
  (`HeapInuse`, ≥ HeapAlloc). The gap over HeapAlloc is heap
  fragmentation.
- `heap_idle_bytes` / `heap_idle_human` — idle (unused) heap spans
  (`HeapIdle`) available for reuse or release. Large idle after a spike =
  the runtime holding reclaimed memory it hasn't returned yet.
- `heap_released_bytes` / `heap_released_human` — idle heap returned to
  the OS (`HeapReleased`). `heap_idle - heap_released` is
  reclaimed-but-still-held.
- `gc_sys_bytes` / `gc_sys_human` — memory used by the GC's own metadata
  (`GCSys`); grows with heap size and churn.
- `cgroup_mem_limit_bytes` / `cgroup_mem_limit_human` — memory limit read
  from the **root of the mounted cgroup hierarchy** (cgroup v2
  `memory.max`, v1 `memory.limit_in_bytes`); the process's own cgroup is
  not resolved via `/proc/self/cgroup`. `0` / `"unlimited"` when no cgroup
  memory controller is mounted or the limit is `max`. An unlimited limit
  zeroes only this field — usage is still reported.
- `cgroup_mem_usage_bytes` / `cgroup_mem_usage_human` — current usage of
  that same cgroup-hierarchy root (`memory.current` /
  `memory.usage_in_bytes`). **Scope caveat:** accurate for Docker/k8s
  private cgroup namespaces, where the mount root IS the container's
  cgroup (its memory and the limit the OOM killer enforces). On a bare
  host / systemd service / non-private cgroup namespace the mount root is
  a broad / machine-root cgroup, so the figure describes that wider scope,
  **not** this process or service — do not read it as the corsa process's
  memory there. `0` off-cgroup. Pair with the limit to see headroom.
- `connection_count` — number of live peer connections (inbound +
  outbound), the same liveness set as `getActiveConnections`. A footprint
  growing in lock-step with this is working set, not a leak.
- `uptime_seconds` / `uptime_human` — seconds since process start, plus
  a human string in the largest of three tiers: seconds (< 1 h), hours
  (< 1 day), or days.
- `sampled_at` — RFC3339Nano UTC instant the sample was taken.

The desktop console **Info** tab shows the headline `Memory` / `Uptime`
rows from this command.

---

### POST /rpc/v1/exec — `getResourceBreakdown`

**Who** is holding the long-lived state, as opposed to how much the
process holds. Snake_case aliases `resource_breakdown` and
`get_resource_breakdown` resolve to the same handler.

It is a separate command from `getResourceUsage` rather than an
extension of it, because the desktop client samples that one **once per
second** to draw the Info tab: folding a per-subsystem breakdown into it
would make every node with a UI attached pay for a dozen domain-lock
acquisitions per second to produce numbers nothing renders. Call this
one when investigating growth, not on a timer.

Response:
```json
{
  "sampled_at": "2026-09-05T12:00:00.123456789Z",
  "floor_bytes": 47458816,
  "floor_human": "45.26 MB",
  "dominant": "route_plane",
  "subsystems": [
    {
      "subsystem": "route_plane",
      "floor_bytes": 41000000,
      "floor_human": "39.10 MB",
      "gauges": [
        {
          "name": "outbound_peer_seq",
          "kind": "memory",
          "count": 644032,
          "entry_bytes": 56,
          "floor_bytes": 36065792,
          "floor_human": "34.40 MB"
        }
      ]
    }
  ]
}
```

Field notes:

- `subsystems[].subsystem` — one of `route_plane`, `announce`,
  `datagram`, `delivery`, `sessions`, `knowledge`, `bans`. Every one is
  always present; a subsystem this build did not wire (the datagram
  plane on a node without it) reports an empty gauge list rather than
  disappearing, so "nothing is holding it there" stays distinguishable
  from "this build cannot answer".
- `subsystems[].gauges[].count` — **exact**: the live cardinality of one
  container, read as a `len` under the lock its owner already holds. No
  container is walked, because an accounting pass that scanned the
  routing table under its mutex would stall the announce loop it shares
  that mutex with.
- `subsystems[].gauges[].entry_bytes` — what one entry of that container
  costs: its key plus its value, and **nothing they point at**.
- `subsystems[].gauges[].kind` — `memory` or `saturation`. A **saturation**
  gauge reports how full a quota is and contributes **no bytes** to any
  total: the entries behind it are a subset of ones a memory gauge has
  already counted, so adding them would report the same records twice
  and leave the "floor" above the truth. Its `floor_bytes` is therefore
  `0` beside a non-zero `count`, which is deliberate rather than a bug.
- `*_floor_bytes` / `*_floor_human` — `count × entry_bytes`, summed
  upwards. **A floor, never a measurement.** It excludes Go's own
  per-bucket map overhead and every byte a stored value merely
  references (a signature, an opaque `Extra` blob, a nested slice).
  Measured retention runs roughly 2.0–2.65× the floor depending on the
  container — see `docs/refactoring/dht/13-measurements.md` §8.2 for the
  measured ratios. Do not expect these figures to add up to
  `getResourceUsage`'s process numbers.
- `dominant` — the subsystem with the largest floor. **Omitted** when
  the node holds nothing at all: on a freshly started node every
  subsystem is equally the largest, and naming one would be an answer
  with no content behind it.
- `sampled_at` — RFC3339Nano UTC instant the pass began. Subsystems are
  **not** sampled under one lock: each is read where its own owner holds
  its own, so the picture is consistent only to within the pass. A
  global lock across every domain would make one timestamp tidier at the
  cost of stalling the node being measured.

One gauge is not a memory figure at all and is documented here because
it is easy to misread as one: `datagram/reverse_local_slots` is a
**saturation** figure — how many of the shared local-request slots are
occupied. Read it against `limits.reverse.per_upstream_cap` from
`fetchDatagramSummary`, whose `reverse.LocalRefusals` says which dtype
that quota has been turning away.

---

## Русский

### POST /rpc/v1/system/help

Список всех доступных команд.

Ответ:
```json
{
  "version": "1.0",
  "commands": [
    {"name": "help", "description": "Список всех доступных RPC команд", "category": "system"},
    {"name": "getPeers", "description": "Получить список подключённых пиров", "category": "network"}
  ]
}
```

`version` — версия схемы ответа help (сейчас `"1.0"`). Отслеживает структуру самого ответа help, а не версию протокола или клиента. Клиенты могут использовать это поле для обнаружения изменений формата (например, новых полей в метаданных команд). Инкрементируется при изменении структуры ответа help.

### POST /rpc/v1/system/ping

Локальный пинг, возвращает pong-ответ. Одинаковый семантический результат вне зависимости от транспорта (HTTP RPC или in-process desktop).

### POST /rpc/v1/system/hello

Отправка hello-фрейма для идентификации. Обработчик заполняет `Version`, `MinimumProtocolVersion`, `Client`, `ClientVersion` и `ClientBuild` перед отправкой ноде — без этих полей handshake отклоняется. Standalone-нода идентифицируется как `Client: "rpc"`; в desktop-режиме `RegisterDesktopOverrides` регистрирует transport-level override, который идентифицируется как `Client: "desktop"` с версией desktop-приложения. Это не семантический форк — меняются только transport identity метаданные. `ClientBuild` — монотонно возрастающий целочисленный номер сборки, по которому пиры определяют наличие новых версий ПО.

### POST /rpc/v1/system/version

Версия клиента и протокола.

Ответ:
```json
{
  "client_version": "0.0.43",
  "protocol_version": 14,
  "node_address": "a1b2c3d4..."
}
```

### POST /rpc/v1/system/node_status

Точка интеграции из PIP-0001 — один аутентифицированный probe, который
PirateCash Core вызывает на старте мастерноды (Этап 1) и при проверках
доступности в v20 (Этап 2). Поле `public_key` — это долговременный
идентификатор, которым v21 proof-of-service будет подписывать
challenge-и.

Эквивалентный вызов: `POST /rpc/v1/exec` с
`{"command": "getNodeStatus"}`. Алиасы `node_status` и `get_node_status`
ведут к тому же обработчику; case-insensitive поиск дополнительно
принимает варианты вроде `nodestatus`. Для route-based вызова работают
оба пути: `/rpc/v1/system/node_status` и `/rpc/v1/system/nodestatus`.

Ответ:
```json
{
  "identity": "a1b2c3d4...",
  "address": "a1b2c3d4...",
  "public_key": "Q...=",
  "box_public_key": "B...=",
  "protocol_version": 14,
  "minimum_protocol_version": 14,
  "client_version": "0.0.43",
  "client_build": 43,
  "connected_peers": 4,
  "started_at": "2026-04-30T12:00:00Z",
  "uptime_seconds": 3742,
  "current_time": "2026-04-30T13:02:22Z"
}
```

Описание полей:

- `identity` — ed25519-fingerprint, стабильный идентификатор ноды
  в маршрутизации и основное поле для PirateCash Core.
- `address` — ed25519-fingerprint, стабильный идентификатор ноды
  в маршрутизации.
- `public_key` — base64-кодированный публичный ed25519-ключ,
  соответствующий `address`.
- `box_public_key` — base64-кодированный публичный curve25519-ключ для
  end-to-end шифрования DM.
- `protocol_version` / `minimum_protocol_version` — текущая версия
  wire-протокола ноды и минимальная допустимая для входящих пиров.
- `client_version` / `client_build` — идентификаторы реализации.
  `client_build` — монотонно возрастающее число, удобно для policy
  gate-ов.
- `connected_peers` — количество **различных peer identity**, с
  которыми у ноды сейчас есть хотя бы одно живое соединение, считая
  и outbound, и inbound сессии. Несколько сокетов от одного и того
  же peer identity схлопываются в один (поле описывает охват
  ретрансляции, а не число сокетов; для пер-коннекшен данных есть
  `getActiveConnections`). Мастернода с нулём полезных пиров вряд
  ли ретранслирует мессенджерный трафик (service-health сигнал
  Этапа 2).
- `started_at` / `uptime_seconds` / `current_time` — RFC3339Nano UTC
  timestamps плюс convenience-поле в целых секундах. PirateCash Core
  использует их для детекции дрейфа часов между своим хостом и
  мастернодой.

Гарантии безопасности, защищённые тестами:

- Через wire идёт только публичный ключевой материал. Эндпоинт НЕ
  должен возвращать приватные ключи, seed-ы, RPC-credentials и любые
  другие секреты.
- Только аутентифицированный localhost-RPC. Тот же gate, что и у всех
  остальных команд в этом документе — см. [../rpc.md](../rpc.md).
- Подпись не прикладывается. Этапы 1 и 2 PIP-0001 её не требуют;
  v21 proof-of-service будет отдельным подписанным артефактом, а не
  довеском к этому снапшоту, чтобы health-check loop оставался
  дешёвым.

### POST /rpc/v1/exec — `getResourceUsage`

Потребление памяти процессом, память cgroup (контейнера), число живых
соединений и аптайм. Возвращаются и машинночитаемые целые, и человекочитаемые
строки — дашборды берут сырые числа, а оператор, читающий JSON, видит
удобные единицы. Snake_case алиасы `resource_usage` и
`get_resource_usage` ведут к тому же обработчику.

Ответ:
```json
{
  "mem_sys_bytes": 62390272,
  "mem_sys_human": "59.50 MB",
  "mem_heap_alloc_bytes": 41943040,
  "mem_heap_alloc_human": "40.00 MB",
  "heap_inuse_bytes": 45088768,
  "heap_inuse_human": "43.00 MB",
  "heap_idle_bytes": 12582912,
  "heap_idle_human": "12.00 MB",
  "heap_released_bytes": 8388608,
  "heap_released_human": "8.00 MB",
  "gc_sys_bytes": 4194304,
  "gc_sys_human": "4.00 MB",
  "cgroup_mem_limit_bytes": 536870912,
  "cgroup_mem_limit_human": "512.00 MB",
  "cgroup_mem_usage_bytes": 157286400,
  "cgroup_mem_usage_human": "150.00 MB",
  "connection_count": 12,
  "uptime_seconds": 192600,
  "uptime_human": "2.23 d",
  "sampled_at": "2026-06-06T05:15:47.123456789Z"
}
```

Описание полей:

- `mem_sys_bytes` / `mem_sys_human` — всего памяти, взятой у ОС
  (`runtime.MemStats.Sys`), ближайший к RSS показатель из рантайма.
  Заголовочная цифра «сколько памяти юзает приложение».
- `mem_heap_alloc_bytes` / `mem_heap_alloc_human` — живая (in-use) куча
  (`runtime.MemStats.HeapAlloc`). Именно она монотонно растёт при
  утечке — её и стоит отслеживать между вызовами.
- `heap_inuse_bytes` / `heap_inuse_human` — байты в in-use heap-спанах
  (`HeapInuse`, ≥ HeapAlloc). Зазор над HeapAlloc — фрагментация кучи.
- `heap_idle_bytes` / `heap_idle_human` — простаивающие heap-спаны
  (`HeapIdle`), доступные для переиспользования или возврата ОС. Большой
  idle после пика = рантайм держит reclaimed-память, ещё не вернув её.
- `heap_released_bytes` / `heap_released_human` — idle-куча, возвращённая
  ОС (`HeapReleased`). `heap_idle - heap_released` — reclaimed, но ещё
  удерживается.
- `gc_sys_bytes` / `gc_sys_human` — память под собственные метаданные GC
  (`GCSys`); растёт с размером кучи и churn.
- `cgroup_mem_limit_bytes` / `cgroup_mem_limit_human` — лимит памяти,
  прочитанный из **корня смонтированной cgroup-иерархии** (cgroup v2
  `memory.max`, v1 `memory.limit_in_bytes`); собственная cgroup процесса
  через `/proc/self/cgroup` не резолвится. `0` / `"unlimited"` когда
  cgroup memory-контроллер не смонтирован или лимит `max`. Безлимитный
  лимит обнуляет только это поле — usage всё равно отдаётся.
- `cgroup_mem_usage_bytes` / `cgroup_mem_usage_human` — текущее
  потребление того же корня cgroup-иерархии (`memory.current` /
  `memory.usage_in_bytes`). **Оговорка по области видимости:** точно для
  private cgroup namespace в Docker/k8s, где mount-root = cgroup
  контейнера (его память и лимит, который форсит OOM-killer). На голом
  хосте / systemd-сервисе / non-private namespace mount-root — широкая /
  root cgroup машины, и цифра описывает этот более широкий scope, **а не**
  процесс/сервис corsa — не принимайте её там за память процесса. `0` вне
  cgroup. В паре с лимитом показывает запас.
- `connection_count` — число живых peer-соединений (входящие + исходящие),
  тот же набор, что у `getActiveConnections`. Footprint, растущий в такт
  с этим числом, — рабочий набор, а не утечка.
- `uptime_seconds` / `uptime_human` — секунды с момента старта плюс
  человекочитаемая строка в наибольшем из трёх ярусов: секунды (< 1 ч),
  часы (< 1 суток) или дни.
- `sampled_at` — момент снятия сэмпла, RFC3339Nano UTC.

В десктоп-консоли на вкладке **Инфо** показываются заголовочные строки
`Память` / `Аптайм` из этой команды.

---

### POST /rpc/v1/exec — `getResourceBreakdown`

**Кто** держит долгоживущее состояние — в отличие от того, сколько держит
процесс целиком. Snake_case алиасы `resource_breakdown` и
`get_resource_breakdown` ведут к тому же обработчику.

Это отдельная команда, а не расширение `getResourceUsage`, по одной
причине: ту сэмплит desktop-клиент **раз в секунду** ради Info-таба, и
вложенная в неё разбивка заставила бы каждый узел с UI платить десятком
захватов доменных мьютексов в секунду за числа, которых никто не рисует.
Эту команду зовут при разборе роста, а не по таймеру.

Ответ:
```json
{
  "sampled_at": "2026-09-05T12:00:00.123456789Z",
  "floor_bytes": 47458816,
  "floor_human": "45.26 MB",
  "dominant": "route_plane",
  "subsystems": [
    {
      "subsystem": "route_plane",
      "floor_bytes": 41000000,
      "floor_human": "39.10 MB",
      "gauges": [
        {
          "name": "outbound_peer_seq",
          "kind": "memory",
          "count": 644032,
          "entry_bytes": 56,
          "floor_bytes": 36065792,
          "floor_human": "34.40 MB"
        }
      ]
    }
  ]
}
```

Описание полей:

- `subsystems[].subsystem` — одно из `route_plane`, `announce`,
  `datagram`, `delivery`, `sessions`, `knowledge`, `bans`. Присутствуют
  всегда все; подсистема, которой в этой сборке нет (плоскость датаграмм
  на узле без неё), отдаёт пустой список gauge-ей, а не исчезает — чтобы
  «там ничего не лежит» осталось отличимо от «эта сборка не умеет
  отвечать».
- `subsystems[].gauges[].count` — **точное** число: живая мощность одного
  контейнера, снятая как `len` под уже удерживаемой владельцем
  блокировкой. Ни один контейнер не обходится: проход, сканирующий
  таблицу маршрутов под её мьютексом, застопорил бы announce-цикл, с
  которым он этот мьютекс делит.
- `subsystems[].gauges[].entry_bytes` — во что обходится одна запись:
  ключ плюс значение и **ничего из того, на что они ссылаются**.
- `subsystems[].gauges[].kind` — `memory` или `saturation`. Gauge вида
  **saturation** показывает, насколько заполнена квота, и **не даёт байтов**
  ни в одну сумму: записи за ним — подмножество тех, что уже посчитал
  memory-gauge, и сложить их значило бы посчитать одни и те же записи дважды
  и увести «пол» выше истины. Поэтому у него `floor_bytes` равен `0` при
  ненулевом `count` — это намеренно, а не ошибка.
- `*_floor_bytes` / `*_floor_human` — `count × entry_bytes`, суммируется
  вверх. **Это ПОЛ, а не измерение.** Не учитывает накладные расходы
  Go-шной map и все байты, на которые хранимое значение лишь ссылается
  (подпись, непрозрачный `Extra`, вложенный слайс). Измеренное удержание
  идёт примерно ×2.0–2.65 от пола в зависимости от контейнера — замеры в
  `docs/refactoring/dht/13-measurements.md` §8.2. Не ждите, что эти числа
  сойдутся с процессными числами `getResourceUsage`.
- `dominant` — подсистема с наибольшим полом. **Опускается**, когда узел
  не держит вообще ничего: на свежезапущенном узле все подсистемы
  одинаково «наибольшие», и назвать одну значило бы дать ответ без
  содержания.
- `sampled_at` — момент начала прохода, RFC3339Nano UTC. Подсистемы
  **не** снимаются под одной блокировкой: каждая читается там, где её
  владелец держит свою, поэтому картина согласована лишь с точностью до
  прохода. Глобальная блокировка по всем доменам сделала бы одну метку
  времени опрятнее ценой остановки измеряемого узла.

Один gauge вообще не про память и вынесен сюда, потому что его легко
принять за память: `datagram/reverse_local_slots` — это **насыщение**,
сколько общих слотов локальных запросов занято. Читать его надо против
`limits.reverse.per_upstream_cap` из `fetchDatagramSummary`, где
`reverse.LocalRefusals` говорит, какому dtype эта квота отказывала.
