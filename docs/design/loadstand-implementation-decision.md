# Process load stand: implementation decision

## English

### Owner decision, 2026-10-18

The owner approved the process-stand design choices T5 1–4 and 6–8, with all review corrections to
that design. The corrections take precedence over its earlier implementation sketches. Development
and light functional smoke checks are authorised. This approves implementation, not completion of
T5 or admission of any load measurement or release. The approximately **19 person-days** estimate is
accepted as an estimate of work, not a deadline.

The separate client profile must use a real `DesktopClient` and SQLite through
[`sdk.NewWithContext`](../../sdk/runtime.go). The core/headless-node profile measures the cost of the
node core. The client profile excludes the UI and its notification coalescer; it does not represent
the full GUI application. Results must name the profile they describe. The intended limited client
profile is 16 nodes, including eight SDK-edge processes.

Heavy runs, their number, resource configurations and limits, acceptance thresholds, the final D1
budget and final-candidate measurements require separate approval. The implementation decision does
not authorise E0–E4, T7 baseline runs or 13b-D1 merely because a script is ready. Tor measurements are
a separate task with different network access. Release admission remains governed by
[Releases](../release.md) and the [manifest](../../packaging/release-admission.json).

### Accepted execution model

Use one Docker container with `--network none`, one Linux network namespace per node and a private
veth/bridge plan inside the container. The acceptance target is linux/arm64 in a Docker Desktop VM
of 12 vCPU and 8 GiB. Do not use `--privileged`: drop all capabilities, add only `NET_ADMIN`,
`SYS_ADMIN`, `SETUID`, `SETGID`, use `no-new-privileges`, default seccomp, a read-only root filesystem
with required tmpfs mounts, explicit memory and PID limits, and `--init`. Nodes run under a nonzero
UID. The Docker socket is not mounted and source trees are read-only.

The scripts never stop unrelated containers. A failed free-memory/resource check refuses to start;
the owner manages other workloads. Numeric run allocations still need the separate approval above.

Primary addresses come from `240.0.0.0/4`, distributed across the production source groups. A
shadowed public /8 is a fallback only after all isolation checks pass. Production classification and
actual observed source addresses must establish group diversity separately for **each binary label**;
different loopback addresses alone are insufficient. Retain production connection limits.

Nodes and SDK-edge processes create their own identities. The orchestrator reads public identities;
it does not hold another node's `*identity.Identity` or sign on its behalf. DM and gossip originate
on SDK-edge through their normal send paths (`sendDm` for DM). Full/headless measured nodes do not
accept DM as final recipients; edge nodes do. Submitting a frame under somebody else's sender
identity is not a substitute for local DM origination.

RPC listens on `127.0.0.1` inside each node's namespace. The orchestrator locks an OS thread, enters
that namespace with `setns`, and dials its loopback listener; the opened socket remains in that
namespace. Other nodes cannot reach this RPC port. Each node has separate random credentials. R1
(local bans and peer `RemoteBannedUntil`) and A4 (`runtime/metrics`) require
`TransportLoopbackOnly` with authentication. Optional R2 may echo effective public configuration.
The policy is described in [RPC](../rpc.md).

### Acceptance conditions for implementation

These are requirements for the implementation and its review, not statements that they have passed.

| Area | Required behaviour and evidence |
|---|---|
| Host isolation | Inspect the container and require network mode `none` before admitting a run. |
| Container preflight | Before the first node, only `lo` is present; all IPv4/IPv6 route tables have no default or non-loopback route. Connections to the production bootstrap endpoint, the fallback public prefix and a public IPv6 address must immediately fail with `ENETUNREACH` or `EHOSTUNREACH`; a timeout is a failed preflight. |
| Node namespaces | Only planned prefixes are routed over the private bridge, with no default route. An outgoing nftables allowlist drops destinations outside the plan and counts drops; any such count invalidates the run. Disable IPv6 until explicitly included in the plan. |
| Bootstrap and postflight | Every node, including hubs, gets a nonempty bootstrap list wholly inside the plan; refuse empty, whitespace-only or foreign values. After a run, peer lists and persisted peer files contain no address outside the plan. |
| Isolation mutations | Empty bootstrap, foreign bootstrap, wrong Docker network, an added default route, and a connect that does not fail immediately must each cause refusal before measured work starts. |
| Address diversity | Static classification uses each binary label's own source tree; dynamic tests check actual incoming addresses and groups for every label. A namespace-to-namespace preflight catches SNAT or wrong source selection. A hub observes more distinct source IPs than the per-IP connection cap. |
| Least privilege | Preflight finds no `cap_sys_module`/`cap_sys_rawio`, no block devices in `/dev`, and no root node processes. Start children by `setns` on a locked thread, then `exec.Cmd` with explicit nonroot credentials. |
| Readiness | Verify `getNodeStatus` and identity, successful dial to the planned node listener, and nonempty `fetchPeerHealth`. An RPC reply alone is insufficient because RPC may start before the node listener. Record the old release's known startup capture race as a limitation. |
| Secrets | Credentials appear only in the child's environment, never argv, URLs, logs, error text or journal. Authenticate in headers; do not log client requests. A formatter redacts every formatting verb. Scan journal, node logs and launch specification for credentials and node private keys as raw, base64 standard/URL, hex and decimal encodings. Same-UID access to `/proc/<pid>/environ` is within the documented container trust boundary. Set `RLIMIT_CORE=0` for orchestrator and children. |
| Process identity | Bind every PID to `/proc` start time, incarnation metadata and SHA-256 of `/proc/<pid>/exe`; journal the actual running binary, not just a version string. Restart reuses the node's namespace, address and data directory. |
| Shutdown | Collect final samples, send SIGTERM, wait within the configured stop budget and journal completion. SIGKILL of a node marks a defective run. The orchestrator handles SIGTERM and stops its own children. Verify the 60-second stop grace against 64 nodes under the eventual approved resource limits. |
| Shared heavy-run lock | Extend the common [heavy-run wrapper](../../scripts/heavy-run.sh), including preflight and holder detection, to treat a live `corsa.loadstand` container as a live job even if its Docker CLI died. Stop then kill its own labelled container before releasing the lock. Run attached with `--rm --init --sig-proxy=true --stop-timeout` and the label. After killing the CLI with SIGKILL while its container lives, the next heavy-run attempt must return 75. |
| Sampling | Linux `/proc`, final `wait4` rusage and authenticated RPC report explicit units and availability. Missing old-release families are undefined, not zero; old-release ban checks without R1 remain incomplete. Explicit test timeouts and Linux-specific lint are required; Linux-only properties run on Linux. |
| Journal and host context | Journal storage is writable only by the effective UID and root. Record only public parameters, image digest, kernel, VM memory/vCPU, Docker version, VM steal, `nf_conntrack_max`, file descriptor limit, sampling schedule, Go settings and source/binary stamps. Keep the macOS host awake during admitted runs. |

### Comparable builds and baseline

Define A2–A5 accounting units and boundaries before implementation, avoiding double counting. A2–A5
and R1 must precede the baseline commit; alternatively apply exactly the same instrumentation to
both measured trees and record its source stamps. Baseline and candidate have identical plans,
address classification, role configuration, sampling families/frequency, runtime settings and
instrumentation. Compare only matching plan/configuration digests, changing the binary label;
use the planned ABAB ordering. Measure the previous release with its available families plus
external metrics and report its capability limitations.

Build from identified revisions with the same toolchain and release flags (`-trimpath`, `-ldflags
"-s -w"`), recording commit/tree, Go environment (including `GOARM64` and `GOEXPERIMENT`), binary
SHA-256, build ID, `go version -m` and reported version. Build twice in separate directories with
separate caches and require identical hashes. The wrapper supplies commit metadata when extracting
with `git archive`. SDK-edge source and any build overlay are part of the recorded provenance.

Start from an environment allowlist, with explicit node paths and public settings; reject ambient
`CORSA_*` settings except the previously accepted `CORSA_VERSION` exception, after verifying it does
not change runtime behaviour. Do not pass that ambient override to children. Allow loopback-only
`CORSA_PPROF_ADDR` for profiling and document the same-namespace access path.

### Work remaining

Approval removes design-choice blockers for the accepted implementation. Missing process execution,
namespace isolation, production-group checks, R1/A4, external sampling, secret scanning, build
provenance, DesktopClient profile, lock lifecycle tests and their validation remain **not done**
until implemented and independently checked. This decision does not mark T5, T6 controls/mutations,
T7 baseline measurements, 13b-D1, G2 or release admission complete. Later status changes require
evidence for the actual candidate; the development estimate or a light loopback smoke test cannot
serve as resource acceptance evidence.

## Русский

### Решение владельца, 2026-10-18

Владелец утвердил решения процессного стенда T5 1–4 и 6–8 вместе со всеми правками по ревью этого
дизайна. Правки имеют приоритет над прежними набросками реализации. Разработка и лёгкие
функциональные smoke-проверки разрешены. Это утверждение реализации, а не завершение T5 или допуск
нагрузочного измерения либо выпуска. Оценка **около 19 человеко-дней** принята как оценка объёма,
а не срок выполнения.

Отдельный клиентский профиль использует настоящий `DesktopClient` и SQLite через
[`sdk.NewWithContext`](../../sdk/runtime.go). Профиль ядра/headless-узла измеряет стоимость ядра
узла. Клиентский профиль не включает UI и коалесцер уведомлений и не представляет полное GUI-
приложение. Результаты должны явно называть профиль. Плановый ограниченный клиентский профиль —
16 узлов, включая восемь процессов SDK-edge.

Тяжёлые прогоны, их число, ресурсные конфигурации и лимиты, пороги приёмки, итоговый бюджет D1 и
измерения финального кандидата требуют отдельного утверждения. Решение о реализации не разрешает
E0–E4, базовые прогоны T7 или 13b-D1 только потому, что скрипт готов. Замеры Tor — отдельная задача с
другим доступом к сети. Допуск выпуска определяется [порядком выпуска](../release.md) и
[манифестом](../../packaging/release-admission.json).

### Принятая модель исполнения

Один контейнер Docker с `--network none`, отдельный Linux netns на узел и приватный план
veth/bridge внутри контейнера. Платформа приёмки — linux/arm64 в VM Docker Desktop с 12 vCPU и
8 GiB. Без `--privileged`: сбросить все capabilities, добавить только `NET_ADMIN`, `SYS_ADMIN`,
`SETUID`, `SETGID`, использовать `no-new-privileges`, стандартный seccomp, корневую файловую систему
только для чтения с нужными tmpfs, явные лимиты памяти и PID, `--init`. Узлы работают с ненулевым UID.
Docker socket не монтируется, деревья исходников доступны только для чтения.

Скрипты никогда не останавливают посторонние контейнеры. Непройденная проверка свободной
памяти/ресурсов запрещает старт; другими нагрузками управляет владелец. Численные выделения на
прогон требуют отдельного утверждения, указанного выше.

Основные адреса — из `240.0.0.0/4`, с распределением по боевым группам источников. Затенённый публичный
/8 — запасной вариант только после всех проверок изоляции. Боевая классификация и фактически
наблюдаемые исходные адреса должны подтвердить разнообразие групп отдельно для **каждой метки
бинаря**; разных loopback-адресов недостаточно. Боевые лимиты соединений сохраняются.

Узлы и процессы SDK-edge создают свои identity сами. Оркестратор читает публичные identity, не
держит чужие `*identity.Identity` и не подписывает от их имени. DM и gossip рождаются на SDK-edge
через обычные пути отправки (`sendDm` для DM). Измеряемые full/headless-узлы не принимают DM как
конечные адресаты; edge принимают. Передача кадра с чужой identity отправителя не заменяет
локальную отправку DM.

RPC слушает `127.0.0.1` внутри netns каждого узла. Оркестратор закрепляет поток ОС, входит в этот
namespace через `setns`, затем соединяется с loopback-слушателем; открытый сокет остаётся в этом
namespace. Другие узлы не видят этот RPC-порт. У каждого узла свои случайные credentials. R1
(локальные баны и `RemoteBannedUntil` пира) и A4 (`runtime/metrics`) требуют
`TransportLoopbackOnly` с аутентификацией. Необязательный R2 может отражать эффективную публичную
конфигурацию. Политика описана в [RPC](../rpc.md).

### Условия приёмки реализации

Это требования к реализации и её проверке, а не заявление, что они уже выполнены.

| Область | Требуемое поведение и доказательство |
|---|---|
| Изоляция хоста | До допуска прогона проверить контейнер и потребовать режим сети `none`. |
| Проверка контейнера | До первого узла присутствует только `lo`; во всех таблицах IPv4/IPv6 нет default и маршрутов вне loopback. Соединения к боевому bootstrap, запасному публичному префиксу и публичному IPv6 немедленно завершаются `ENETUNREACH` или `EHOSTUNREACH`; таймаут — отказ проверки. |
| Namespace узлов | Через приватный мост маршрутизируются только префиксы плана, default нет. Исходящая nftables-allowlist отбрасывает адресатов вне плана и считает отбрасывания; любое такое срабатывание делает прогон дефектным. IPv6 выключен до явного включения в план. |
| Bootstrap и проверка после | Каждому узлу, включая хабы, задан непустой bootstrap целиком из плана; пустое значение, пробелы или чужие адреса запрещают старт. После прогона в списках пиров и сохранённых файлах пиров нет адресов вне плана. |
| Мутации изоляции | Пустой bootstrap, чужой bootstrap, неверная сеть Docker, добавленный default route и connect без немедленного отказа по отдельности запрещают запуск измеряемой работы. |
| Разнообразие адресов | Статическая классификация использует дерево исходников каждой метки бинаря; динамические проверки наблюдают входящие адреса и группы каждой метки. Предварительный дозвон между netns ловит SNAT и неверный исходящий адрес. Хаб видит больше разных исходных IP, чем per-IP-потолок соединений. |
| Минимальные полномочия | До старта нет `cap_sys_module`/`cap_sys_rawio`, блочных устройств в `/dev` и процессов узлов под root. Дети запускаются через `setns` на закреплённом потоке, затем `exec.Cmd` с явными nonroot credentials. |
| Готовность | Проверены `getNodeStatus` и identity, успешный дозвон на слушатель узла из плана, непустой `fetchPeerHealth`. Одного ответа RPC мало: RPC может запуститься раньше слушателя узла. Известная стартовая гонка capture старого выпуска записана как ограничение. |
| Секреты | Credentials только в окружении ребёнка, не в argv, URL, логах, ошибках или журнале. Аутентификация — заголовками; запросы клиента не логируются. Formatter скрывает значение для всех глаголов форматирования. Сканер проверяет журнал, логи узлов и спецификацию запуска на credentials и приватные ключи узлов в raw, base64 standard/URL, hex и десятичном виде. Доступ того же UID к `/proc/<pid>/environ` входит в описанную границу доверия контейнера. У оркестратора и детей `RLIMIT_CORE=0`. |
| Идентификация процесса | Каждый PID связан со временем старта `/proc`, метаданными инкарнации и SHA-256 `/proc/<pid>/exe`; журнал фиксирует фактический бинарь, а не только строку версии. Рестарт сохраняет namespace, адрес и каталог узла. |
| Остановка | Снять финальные метрики, послать SIGTERM, дождаться остановки в заданном бюджете и дописать журнал. SIGKILL узла делает прогон дефектным. Оркестратор обрабатывает SIGTERM и останавливает своих детей. Проверить grace 60 секунд на 64 узлах с будущими утверждёнными ресурсными лимитами. |
| Общий замок тяжёлых прогонов | В общем [heavy-run wrapper](../../scripts/heavy-run.sh), включая проверку до старта и живости владельца, живой контейнер `corsa.loadstand` считается живым заданием даже после смерти Docker CLI. Свой контейнер с меткой остановить, затем принудительно завершить до снятия замка. Запуск присоединённый: `--rm --init --sig-proxy=true --stop-timeout` и метка. После SIGKILL CLI при живом контейнере следующая попытка heavy-run возвращает 75. |
| Метрики | Linux `/proc`, итоговый `wait4` rusage и аутентифицированный RPC имеют явные единицы и доступность. Недоступные семейства старого выпуска — «не определено», не ноль; проверка его банов без R1 неполна. Нужны явный timeout тестов и отдельный Linux lint; Linux-свойства исполняются на Linux. |
| Журнал и окружение | В том журнала пишут только effective UID и root. Записываются только публичные параметры, digest образа, ядро, память/vCPU VM, версия Docker, VM steal, `nf_conntrack_max`, лимит файловых дескрипторов, расписание опроса, настройки Go и отпечатки исходников/бинарей. Во время допущенных прогонов macOS-хост не засыпает. |

### Сопоставимые сборки и baseline

До реализации определить единицы и границы учёта A2–A5, исключив двойной счёт. A2–A5 и R1 должны
предшествовать baseline-коммиту; запасной путь — ровно та же инструментализация обоих измеряемых
деревьев с отпечатками исходников. У baseline и кандидата одинаковы планы, классификация адресов,
конфигурация ролей, семейства/частота опроса, настройки runtime и инструментализация. Сравнивать
только совпадающие digest плана/конфигурации с заменой метки бинаря; использовать плановый порядок
ABAB. Предыдущий выпуск измеряется доступными ему семействами и внешними метриками с явным
описанием ограничений возможностей.

Собирать определённые ревизии одним toolchain с релизными флагами (`-trimpath`, `-ldflags "-s -w"`),
фиксируя commit/дерево, окружение Go (включая `GOARM64`, `GOEXPERIMENT`), SHA-256 бинаря, build ID,
`go version -m` и сообщаемую версию. Две сборки в разных каталогах с раздельными cache должны иметь
одинаковые отпечатки. При извлечении через `git archive` commit фиксирует обёртка. Исходники SDK-edge
и любой build overlay входят в сведения о происхождении.

Запускать с белым списком окружения, явными путями узла и публичными настройками; запрещать
унаследованные `CORSA_*`, кроме ранее принятого исключения `CORSA_VERSION` после проверки, что оно
не меняет runtime. Этот внешний override детям не передавать. Разрешить loopback-only
`CORSA_PPROF_ADDR` для профилирования и описать доступ внутри namespace.

### Оставшаяся работа

Утверждение снимает блокеры выбора дизайна принятой реализации. Отсутствующие процессный
исполнитель, изоляция namespace, проверки боевых групп, R1/A4, внешний сэмплер, сканер секретов,
происхождение сборок, профиль DesktopClient, проверки жизни замка и их валидация остаются
**не выполнены** до реализации и независимой проверки. Это решение не закрывает T5, контроли и
мутации T6, базовые измерения T7, 13b-D1, G2 или допуск выпуска. Следующий статус требует
доказательств на фактическом кандидате; оценка разработки и лёгкий loopback-smoke не служат
доказательством ресурсной приёмки.
