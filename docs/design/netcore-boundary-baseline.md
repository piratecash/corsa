# NetCore lexical boundary baseline

## English

The boundary guard records existing technical debt at commit `251b2a2`. It does
not claim that the NetCore migration or removal of raw socket dependencies is
complete. The former script expected 12 declarations and eight importers while
that commit already contained 23 matching declarations, 18 importers, four
registry reads, four `entry.core` metrics reads and one `uint64` identity helper.
D1 separately adds one declaration/import for degree observation and one import
used only for the readiness close sentinel: 24 declaration matches and 20
reviewed `"net"` import occurrences in total. The 23/18 legacy counts do not
include these additions.

`make enforce-netcore-boundary` runs
[`scripts/enforce-netcore-boundary.sh`](../../scripts/enforce-netcore-boundary.sh).
Its static review contract is
[`scripts/netcore-boundary-baseline.tsv`](../../scripts/netcore-boundary-baseline.tsv).
No expectation is inferred from HEAD at runtime. Declarations are compared as
sorted **multisets** of path and normalized signature. Legacy access sites also
include the enclosing signature and the complete normalized matching source.
Line numbers and whitespace are ignored; multiplicity is retained. An addition,
removal, duplicate or same-count replacement fails and needs explicit review of
the contract. These are exceptions for individual sites, never whole-file
exclusions for forbidden accesses.

### Frozen declaration matches

The existing parameter-pattern check has these 23 matches. It is a lexical
check, not a claim to enumerate every semantic use of `net.Conn`.

| Node file | Frozen declarations |
|---|---|
| `accept_inbound.go` | `acceptInbound` |
| `conn_registry.go` | `connIDForLocked`, `connIDFor`, `registerInboundConnLocked`, `attachOutboundCoreLocked`, `unregisterConnLocked` |
| `listener_refusal.go` | `refuse`, `closeRefusedSecure`, `sendRefusalAlert`, `closeUnregisteredInbound` |
| `peer_management.go` | `enableTCPKeepAlive` |
| `secure_session_store.go` | `outboundDialOf` |
| `service.go` | `handleConn`, `ConnAuthState`, `SetConnAuthState`, `isBlacklistedConn`, `registerInboundConn`, `unregisterInboundConn`, `closeRegisteredInboundCore`, `closeRegistryCore` |
| `session_liveness.go` | `newInboundFrameDeadline` |
| `session_probe.go` | `probeLegacy`, `armLegacyProbeDeadline` |

### D1 degree observation addition

The exact new declaration is
`internal/core/node/find_node_degree.go | func (s *Service) publishDegreeInbound(conn net.Conn)`.
Its exact import record is `internal/core/node/find_node_degree.go | "net"`.
The function is a no-op without a degree observer; otherwise it type-asserts the
existing `*peekedConn`, reads `wire.common.Load()` and delegates to the existing
transport-proof publication checks. This declaration introduces no socket
read/write/close or new registry. The records acknowledge its concrete lexical
dependency; they do not complete the NetCore migration or grant this file a
blanket network-I/O exception. Compilation, behavior and SOURCE guards remain
separate checks.

### Frozen access debt

| Location | Retained matching sites |
|---|---|
| `resource_breakdown.go`, `sessionsUsage` | `len(s.conns)`, the `s.conns` loop and `entry.core.WriterQueue()` |
| `rollout_metrics.go`, `collectNeighbourComposition` | the `s.conns` loop, `entry.core.Auth().ProvenIdentity()` and `entry.core.Declarations()` |
| `session_kind_metrics.go`, `refreshLiveSessionKinds` | the `s.conns` loop and `entry.core.Auth()` |
| `peer_management.go`, `isActiveConnectionState` | the existing `connID uint64` parameter |

The TSV freezes full source expressions, including assignments and surrounding
calls. These metrics/helper sites are recorded debt; the guard repair changes
no production Go code and performs no architectural cleanup.

### Import review scope

The 18 legacy importers remain explicit debt with their existing purposes:

| Node file | Existing purpose |
|---|---|
| `accept_inbound.go` | accepted socket admission and source address |
| `conn_budget_dial.go` | auxiliary socket reservation wrapper and dial result |
| `conn_registry.go` | lifecycle registration and socket/ConnID binding |
| `connect_only.go` | target host/port and IP handling |
| `contact_verify_budget.go` | canonical IP and IPv6 prefix accounting |
| `inbound_handshakes.go` | address/IP source groups and handshake budgets |
| `listener_refusal.go` | pre-registration refusal and socket close |
| `loopback_inbound.go` | loopback address classification |
| `netgroup.go` | IP grouping and reachability |
| `peer_management.go` | dial/socket helpers and TCP keepalive |
| `peer_provider.go` | peer address and CIDR policy |
| `routing_relay.go` | notice dial bootstrap edges |
| `secure_session_store.go` | actual outbound socket endpoint attribution |
| `service.go` | existing socket entry/lifecycle surface |
| `session_liveness.go` | existing inbound frame deadlines |
| `session_probe.go` | legacy probe socket and deadline |
| `session_secure.go` | session-kind selection and pre-registration TLS |
| `socks5.go` | SOCKS5 handshake before NetCore |

The added D1 `find_node_degree.go` import is the nineteenth record in the
existing `legacy_import` matching category. That category name is retained for
the strict matcher, not a claim that the addition existed at `251b2a2`. The
readiness import is a separate twentieth record; neither addition replaces or
removes any of the 18 historical import records.

`peer_session_readiness.go` is separate: its only allowed `net` selector is
`net.ErrClosed`, in `(*peerSessionReadiness).stop`. A new selector, relocation,
aliased/dot import or additional importer fails. The import check conservatively
reviews all non-comment source lines containing the exact quoted `"net"` literal;
it is not an import AST parser.

All other existing zero-forbidden-pattern gates remain, including parallel
socket registries, raw session writes, unacknowledged write wrappers, removed
lookup/mutation helpers and old walker signatures. Existing direct-write checks
retain their previous three notice-dial and two SOCKS5 matches. Compilation,
tests and lint remain independent requirements. Sources must follow gofmt's
column-zero function declarations/closing braces for enclosing-function lookup.

`bash scripts/test-enforce-netcore-boundary.sh` builds tiny noncompiled lexical
fixtures in a temporary directory, without copying the repository. Positive
controls cover line shifts/whitespace; negative controls cover membership,
multiplicity, source/function substitution, each legacy access category,
readiness and retained zero-pattern gates. Focused D1 controls also remove,
duplicate and replace its declaration/import records and add a different
unreviewed importer; the reviewed degree addition does not allow those changes.

## Русский

Boundary guard фиксирует существующий технический долг коммита `251b2a2`. Это не
заявление о завершении миграции NetCore или удалении raw socket dependencies.
Старый скрипт ожидал 12 деклараций и восемь importers, хотя этот коммит уже содержал
23 совпадения деклараций, 18 importers, четыре чтения registry, четыре metrics-чтения
`entry.core` и один helper с `uint64` identity. D1 отдельно добавляет одну
декларацию/import для degree observation и один
import исключительно для close sentinel readiness: итого 24 совпадения деклараций
и 20 проверяемых вхождений `"net"`. Прежние legacy числа 23/18 этих дополнений
не включают.

`make enforce-netcore-boundary` запускает
[`scripts/enforce-netcore-boundary.sh`](../../scripts/enforce-netcore-boundary.sh).
Статический контракт ревью находится в
[`scripts/netcore-boundary-baseline.tsv`](../../scripts/netcore-boundary-baseline.tsv).
Ожидания не вычисляются из HEAD во время запуска. Декларации сравниваются как
отсортированные **мультимножества** пути и нормализованной сигнатуры. Для legacy
доступов добавлены enclosing signature и полный нормализованный matching source.
Номера строк и пробелы игнорируются, кратность сохраняется. Добавление, удаление,
дубликат или замена при прежнем количестве требует явного ревью контракта.
Исключения относятся к отдельным местам, а не ко всему файлу.

### Зафиксированные декларации

Существующий parameter-pattern check содержит следующие 23 совпадения. Проверка
лексическая и не перечисляет все семантические способы использования `net.Conn`.

| Файл node | Декларации |
|---|---|
| `accept_inbound.go` | `acceptInbound` |
| `conn_registry.go` | `connIDForLocked`, `connIDFor`, `registerInboundConnLocked`, `attachOutboundCoreLocked`, `unregisterConnLocked` |
| `listener_refusal.go` | `refuse`, `closeRefusedSecure`, `sendRefusalAlert`, `closeUnregisteredInbound` |
| `peer_management.go` | `enableTCPKeepAlive` |
| `secure_session_store.go` | `outboundDialOf` |
| `service.go` | `handleConn`, `ConnAuthState`, `SetConnAuthState`, `isBlacklistedConn`, `registerInboundConn`, `unregisterInboundConn`, `closeRegisteredInboundCore`, `closeRegistryCore` |
| `session_liveness.go` | `newInboundFrameDeadline` |
| `session_probe.go` | `probeLegacy`, `armLegacyProbeDeadline` |

### Дополнение D1 для наблюдения degree

Новая точная декларация —
`internal/core/node/find_node_degree.go | func (s *Service) publishDegreeInbound(conn net.Conn)`.
Точная запись import — `internal/core/node/find_node_degree.go | "net"`.
Без degree observer функция ничего не делает; иначе проверяет существующий тип
`*peekedConn`, читает `wire.common.Load()` и передаёт результат прежним проверкам
публикации transport proof. В этой функции нет нового socket read/write/close
или registry. Записи фиксируют конкретную лексическую зависимость; они не
завершают миграцию NetCore и не разрешают произвольный network I/O в этом файле.
Compilation, behavior и SOURCE guards остаются отдельными проверками.

### Зафиксированный долг доступов

| Место | Сохранённые совпадения |
|---|---|
| `resource_breakdown.go`, `sessionsUsage` | `len(s.conns)`, цикл по `s.conns` и `entry.core.WriterQueue()` |
| `rollout_metrics.go`, `collectNeighbourComposition` | цикл по `s.conns`, `entry.core.Auth().ProvenIdentity()` и `entry.core.Declarations()` |
| `session_kind_metrics.go`, `refreshLiveSessionKinds` | цикл по `s.conns` и `entry.core.Auth()` |
| `peer_management.go`, `isActiveConnectionState` | существующий параметр `connID uint64` |

TSV фиксирует полные source expressions, включая присваивания и окружающие вызовы.
Эти metrics/helper sites остаются долгом; repair guard не меняет production Go
и не выполняет архитектурную очистку.

### Область ревью import

18 legacy importers остаются явным долгом с прежним назначением:

| Файл node | Существующее назначение |
|---|---|
| `accept_inbound.go` | admission принятого socket и source address |
| `conn_budget_dial.go` | reservation wrapper вспомогательного socket и результат dial |
| `conn_registry.go` | lifecycle registration и связь socket/ConnID |
| `connect_only.go` | host/port target и обработка IP |
| `contact_verify_budget.go` | canonical IP и учёт IPv6 prefix |
| `inbound_handshakes.go` | address/IP source groups и handshake budgets |
| `listener_refusal.go` | refusal до регистрации и закрытие socket |
| `loopback_inbound.go` | классификация loopback address |
| `netgroup.go` | IP grouping и reachability |
| `peer_management.go` | dial/socket helpers и TCP keepalive |
| `peer_provider.go` | peer address и CIDR policy |
| `routing_relay.go` | bootstrap edges дозвона notice |
| `secure_session_store.go` | атрибуция фактического outbound socket endpoint |
| `service.go` | прежний socket entry/lifecycle surface |
| `session_liveness.go` | прежние inbound frame deadlines |
| `session_probe.go` | socket и deadline legacy probe |
| `session_secure.go` | выбор session kind и TLS до регистрации |
| `socks5.go` | SOCKS5 handshake до NetCore |

Новый D1 import из `find_node_degree.go` — девятнадцатая запись существующей
категории сопоставления `legacy_import`. Имя категории сохранено для строгого
matcher и не означает, что дополнение существовало в `251b2a2`. Readiness import
остаётся отдельной двадцатой записью; ни одно дополнение не заменяет и не удаляет
18 исторических записей import.

`peer_session_readiness.go` рассматривается отдельно: единственный разрешённый
selector — `net.ErrClosed` в `(*peerSessionReadiness).stop`. Новый selector, перенос,
alias/dot import или дополнительный importer вызывает отказ. Import check
консервативно проверяет все non-comment source lines с точным quoted literal
`"net"`; это не import AST parser.

Остальные прежние zero-forbidden-pattern gates сохранены: параллельные socket
registries, raw session writes, неподтверждённые write wrappers, удалённые
lookup/mutation helpers и старые walker signatures. Прежние direct-write checks
сохраняют три notice-dial и два SOCKS5 совпадения. Compilation, tests и lint остаются
независимыми требованиями. Поиск enclosing function предполагает формат gofmt:
объявления функций и закрывающие их скобки начинаются с нулевой колонки.

`bash scripts/test-enforce-netcore-boundary.sh` создаёт маленькие некомпилируемые
лексические fixtures во временном каталоге без копии репозитория. Positive controls
проверяют сдвиг строк и пробелы; negative controls — membership, кратность,
замену source/function, каждую категорию legacy accesses, readiness и сохранённые
zero-pattern gates. Отдельные D1 controls удаляют, дублируют и заменяют
его declaration/import записи и добавляют другой непроверенный importer;
проверенное дополнение degree эти изменения не разрешает.
