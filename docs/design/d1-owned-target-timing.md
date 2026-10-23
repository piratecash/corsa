# Explicit inactive owned-target service timing / Явное неактивное время T

This private candidate separates signature-service admission from the existing
six-second answer deadline. It does not select a deployment profile, activate
Ready, establish G2, change R, or expose a configuration/capability.

For an original authenticated receive at `r`, an explicitly supplied owned-only
profile contains service bound `D_T` and runtime reserve `E_T`:

- `0 < E_T < D_T < responderTimeout`;
- `signatureReadyBy = r + D_T` (checked monotonic addition);
- `latestSignStart = signatureReadyBy - E_T`;
- `answerDeadline = r + responderTimeout`, retaining the existing six seconds.

The service interval is half-open. Admission requires authenticated final
`f < latestSignStart` and the original projected calendar slot
`s < latestSignStart`. `WaitDelay` and actual `Take` use the same start boundary.
Original `Complete(signature)` success defines signature-ready and requires
`f < signatureReadyBy`. Equality refuses. `E_T` covers Take/start through that
Complete success; it is an explicit profile requirement, not a measured CPU or
network guarantee. Successful answers and later cache recipients retain their
original answer deadlines. A protected duplicate retains the first admitted
job and its cutoffs. Cancellation, stale/clock/capacity/missing-proof refusal,
actual source/node debits, mandatory `Complete(nil)` and no-refund rules remain.

`NewLocalTarget` and standalone calendar/coordinator APIs keep their old path.
Only `NewLocalTargetWithTiming` and a separate private Node construction supply
an immutable timing value before escape/Seal. There are no numeric defaults,
late setters, background-per-request workers or changes to R spacing/limits.

The private test-only gate is installed solely by its dedicated construction.
It carries close signals, never authority/results/callbacks. Both channels are
unbuffered and their identities are bound before callbacks and Seal. The
existing single worker has already completed actual Take and installed its
mandatory nil-completion defer before waiting. It holds no Core/shared/Node
owner lock. Release or the original Done ends the wait; existing shutdown joins
that worker. Ordinary and normal timed constructors keep a nil gate.

The scalar `OwnedTargetTimingRefusal` is a sanitized ErrBudget diagnostic. Its
six int64 nanosecond values come from the SAME original admission trial. It is
allocated before owner locks/commit lease, retains no queue/history, exports no
Instant/domain/receipt/job/state pointer, preserves `errors.Is(ErrBudget)` and
uses the unchanged ErrBudget error text. The existing Node dispatch returns it
unchanged; the existing test reader wraps it with `%w` into its bounded error
channel. No production observer or state-return hook is introduced.

The Node test uses four genuine TLS/CM source paths, original raw reads and
retained challenges, original runtime-owned T, real signing and original answer
writers. Only the existing N-in maturity seam and observation-only initiator
clock are fixture clocks. The service profile `h=250ms`, `D_T=1.5h`, `E_T=h/4`
is test-only. It requires an actual refused trial witness
`r + D_T < s < answerDeadline`; it does not infer the slot from request count,
assert an arbitrary ingress SLA, retry, enlarge h, or simulate the Runtime
clock. If that actual case is not reached, it fails and reports observed
scalars. Resource snapshots and actual signatures/writers are checked before,
at the gated actual debit, and after release; a local refusal does not refund R.

Core boundary tests explicitly use FixtureDomain and are not evidence that the
Node fixture controls Runtime. SOURCE controls are finite raw-byte/callsite
closures, not typed/native/hermetic or allocation/performance proofs. No code
in this author packet has been formatted, compiled or executed. FORMAT, full
independent CODE review and fresh actual QA are separate future work.

## Русский контракт

Кандидат разделяет срок допуска к обслуживанию подписи и прежний срок ответа
6 секунд. Владелец профиля ещё не выбрал числовые параметры G2; активации нет.
Для исходного authenticated receive `r` задаются явные `D_T` и `E_T`, где
`0 < E_T < D_T < 6s`. Готовность подписи ограничена `r+D_T`, последний старт —
`r+D_T-E_T`; прежний срок ответа остаётся `r+6s`. Интервалы полуоткрытые:
равенство отказывает при projected admission, Take и Complete. Готовность —
успешный исходный `Complete(signature)`, а не начало криптографии или запись
ответа. Старые конструкторы и standalone API сохраняют семантику.

Оригинальные владельцы/receipt/challenge, последнее authenticated Sample,
порядок блокировок и CAS, отсутствие refund, protected duplicate, capacity,
обязательный `Complete(nil)` и очистка не заменяются фикстурой. Тестовая
close-only синхронизация разрешена только до escape/Seal, после настоящего Take
и установки defer; ожидание не удерживает owner locks и завершается release
или original Done. Обычные конструкторы не получают этот gate.

Настоящий Node тест проверяет четыре исходных TLS/CM пути и реальные подписи и
writer. Нужное неравенство проверяется непосредственно по числовому witness
того же trial, а не по порядку stdout/stderr или числу запросов. Малый ingress
span не объявляется SLA; при недостижении причинного случая тест отказывает,
не повышая таймеры. Числа фикстуры не являются параметрами deployment/G2.

Пакет — только виртуальный SOURCE. CODE/FORMAT/TYPE/QA/benchmark/G2/активация
ещё не выполнены и не утверждаются.
