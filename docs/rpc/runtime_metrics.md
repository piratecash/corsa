# Runtime metrics and ban diagnostics

## English

### `getRuntimeMetrics` (A4)

This read-only command reports the **Go process**, not a node's share when several nodes share one
process. It is `TransportLoopbackOnly`: HTTP requires a real loopback socket and configured RPC
authentication; proxy headers do not establish locality. The in-process console is allowed. The
policy is also declared when the provider is unavailable. `getResourceUsage` keeps its existing
interface and broader policy; A4 does not add a `ReadMemStats` or forced-GC operation.

The response has `version: 1`, `started_at`, `read_at`, numeric fields below and an `unsupported`
array of runtime metric names. Unsupported fields are absent, never fabricated zeros. A supported
counter before its first event may be zero. Readings use one `runtime/metrics.Read`, with no node
domain mutex held.

| JSON field | runtime metric | Unit / interpretation |
|---|---|---|
| `heap_live_bytes` | `/gc/heap/live:bytes` | Bytes live at the preceding GC |
| `heap_objects_bytes` | `/memory/classes/heap/objects:bytes` | Current objects, including unswept garbage |
| `heap_allocs_bytes` | `/gc/heap/allocs:bytes` | Cumulative allocated bytes |
| `go_memory_bytes` | `/memory/classes/total:bytes` | Runtime-managed memory; overlaps MemStats.Sys, do not add them |
| `gc_cycles` | `/gc/cycles/total:gc-cycles` | Cumulative cycles |
| `goroutines` | `/sched/goroutines:goroutines` | Current count |
| `gomaxprocs` | `/sched/gomaxprocs:threads` | Current runtime parallelism limit |
| `cpu_total_seconds` | `/cpu/classes/total:cpu-seconds` | Cumulative available CPU time, not spent CPU |
| `cpu_user_seconds` | `/cpu/classes/user:cpu-seconds` | Cumulative Go user-code CPU estimate |
| `cpu_gc_seconds` | `/cpu/classes/gc/total:cpu-seconds` | Cumulative GC CPU estimate |
| `cpu_idle_seconds` | `/cpu/classes/idle:cpu-seconds` | Cumulative idle CPU estimate |

CPU classes are runtime estimates updated with GC; they are not kernel accounting. External
`/proc` and final `wait4` rusage are the process CPU/RSS/fd source, with separate columns. Never add
runtime and kernel CPU or memory figures. Compare cumulative deltas only within the same
`started_at`, with increasing read time and equal bench instrumentation/settings. A process restart
begins a new period. An older binary without this command has an unavailable family; a missing
command or field is not zero.

### `listBanned` (R1)

The command and its `list_banned` alias are now `TransportLoopbackOnly` with mandatory HTTP
authentication. The original `banned_ips` and `count` fields retain their meaning (the
PeerProvider's propagated IP bans). Version 2 adds independent source families:

| Field | Scope |
|---|---|
| `local_bans` | Local IP score and active blacklist expiry; includes subthreshold scores |
| `local_ip_bans` | Node's retained IP-wide bans, even without a provider |
| `peer_bans` | Active local per-endpoint bans; `self_identity_cooldown` distinguishes a self-route cooldown |
| `remote_peer_bans` | Active `RemoteBannedUntil` by exact endpoint |
| `remote_ip_bans` | Active remote IP-wide suppression |
| `loopback_identities_scored`, `loopback_identities_banned` | Aggregate counts from proven-identity penalties of the loopback class |
| `loopback_identity_penalties_available` | Whether that class was available for this observation |

These families overlap; do not add them as a count of distinct offenders. A nonzero local score
already invalidates the normal ban-free bench policy. Self-route cooldown is a separately declared
bench-policy choice. Loopback identity counts may conservatively retain expired entries until their
normal sweep. False emptiness must not be inferred from their absence on older releases.

`ready: false` means no initial observation exists. `started_at` identifies the period;
`observation_started_at` and `generated_at` delimit collection. Sources are read separately, not as
one atomic transaction. RPC uses a **fresh read barrier**: it queues a bounded request to the
background refresher and waits with the request context (at most five seconds); the RPC goroutine takes no node domain
locks. Only that request's completed collection is returned with `fresh: true`. A blocked refresher,
shutdown or cancelled request fails the call; it never turns a previous clean snapshot into a
successful fresh observation. An immutable worker lifecycle identifies the active collection epoch.
Before the worker starts and after it stops, fresh reads are immediately unavailable. Shutdown
interrupts both queueing and response waiting even while collection is blocked; the lifecycle is
checked again after collection and before returning the result. The direct cached `ListBannedJSON` reader returns `fresh: false` and
must not be used as a measurement admission check. During ordinary operation the reader-gated
background snapshot also refreshes on the existing 500 ms cadence.

The refresher completes PeerProvider callbacks before entering node domains, reads `peerMu` and
`ipStateMu` separately, then publishes an immutable atomic snapshot. Missing version, readiness,
freshness or loopback-family availability makes a complete ban-free measurement check unavailable,
not passed. Older adapters keep their original JSON and therefore cannot establish complete R1
coverage. No ban, score, connection limit or release decision is changed by either diagnostic.

## Русский

### `getRuntimeMetrics` (A4)

Команда только читает **процесс Go**, а не долю узла, если несколько узлов работают в одном процессе.
Политика `TransportLoopbackOnly`: HTTP требует настоящий loopback-сокет и настроенную RPC-
аутентификацию; proxy-заголовки локальность не доказывают. Внутрипроцессная консоль разрешена.
Политика действует и при недоступном provider. Интерфейс и более широкая политика `getResourceUsage`
не меняются; A4 не добавляет `ReadMemStats` или принудительный GC.

Ответ: `version: 1`, `started_at`, `read_at`, численные поля таблицы английской секции и массив
`unsupported` с именами метрик runtime. Неподдерживаемые поля отсутствуют, а не превращаются в ноль.
Поддерживаемый счётчик до первого события может быть нулём. Сборка — один `runtime/metrics.Read`
без доменных мьютексов узла.

`heap_live_bytes` — байты живого на предыдущем GC; `heap_objects_bytes` — текущие объекты вместе с
ещё не собранным мусором; `heap_allocs_bytes` — накопительные аллокации. `go_memory_bytes` перекрывается
с MemStats.Sys: складывать их нельзя. `gc_cycles` — накопительные циклы, `goroutines` — текущее число
горутин, `gomaxprocs` — текущий предел параллелизма runtime.

Поля `cpu_*_seconds` — накопительные оценки runtime в секундах, обновляемые с GC, а не учёт ОС.
`total` — доступное время CPU, не потраченное; `user` — пользовательский Go-код, `gc` — сборка мусора,
`idle` — простой. Внешние `/proc` и итоговый `wait4` rusage дают CPU/RSS/fd процесса отдельными
колонками. CPU и память runtime и ядра не суммируются. Дельты накопительных сравниваются только при
одинаковом `started_at`, возрастающем времени и одинаковых инструментализации/настройках стенда.
Рестарт — новый период. У старого бинаря без команды семейство недоступно; пропущенная команда или
поле не равны нулю.

### `listBanned` (R1)

Команда и alias `list_banned` теперь `TransportLoopbackOnly` с обязательной HTTP-аутентификацией.
Прежние `banned_ips` и `count` сохраняют смысл: распространённые IP-баны PeerProvider. Версия 2
добавляет независимые семейства: `local_bans` (локальный IP score, включая подпороговый, и активный
blacklist); `local_ip_bans` (сохранённые IP-баны узла); `peer_bans` (баны endpoint с отдельным флагом
`self_identity_cooldown`); `remote_peer_bans` (`RemoteBannedUntil` точного endpoint);
`remote_ip_bans` (удалённое подавление по IP); `loopback_identities_scored` и
`loopback_identities_banned` (числа доказанных identity с наказаниями loopback-класса),
`loopback_identity_penalties_available` (доступность класса для наблюдения).

Семейства перекрываются, их нельзя складывать как число разных нарушителей. Любой ненулевой
локальный score нарушает обычную политику стенда «без банов»; self-route cooldown — отдельный выбор
политики. Числа loopback identity могут консервативно удерживать истёкшие записи до штатной очистки.
Отсутствие полей у старого выпуска не означает пустоту.

`ready: false` — начального наблюдения ещё нет. `started_at` задаёт период, `observation_started_at`
и `generated_at` — окно сборки. Источники читаются порознь, не одной атомарной транзакцией. RPC
использует **барьер свежего чтения**: ограниченный запрос отправляется фоновой горутине, ожидание
отменяется context запроса и ограничено пятью секундами; горутина RPC не берёт доменные мьютексы. Только завершённая сборка для
этого запроса возвращается как `fresh: true`. Зависший refresher, остановка или отмена завершают
вызов ошибкой; прежний чистый снимок не становится новым успешным наблюдением. Неизменяемый lifecycle
worker определяет активную эпоху сборки. До запуска worker и после его остановки свежее чтение
немедленно недоступно. Остановка прерывает и постановку в очередь, и ожидание ответа даже при
заблокированной сборке; lifecycle перепроверяется после сборки и перед возвратом результата. Прямой кешированный
`ListBannedJSON` отдаёт `fresh: false` и не подходит для допуска измерения. При обычной работе
фоновый снимок также обновляется по активности читателя с прежним периодом 500 мс.

Refresher заканчивает callbacks PeerProvider до входа в домены, читает `peerMu` и `ipStateMu`
отдельно, затем публикует неизменяемый атомарный снимок. Нет версии, готовности, свежести или
доступности loopback-семейства — полная проверка «без банов» недоступна, а не пройдена. Старые
адаптеры сохраняют прежний JSON и полной R1-проверки не доказывают. Диагностика не меняет баны,
score, лимиты соединений и решения о выпуске.
