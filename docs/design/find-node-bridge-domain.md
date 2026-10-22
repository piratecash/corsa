# Dormant FindNode bridge domain / Неактивный домен моста FindNode

## English

> Current narrow extensions admit exactly the two dormant dependencies in
> `secure_session_store.go` and `trust.go`; ordinary constructors pass zero.
> The original standalone scope below is superseded only by the bounded
> [pin](find-node-pin-publication.md) and [trust](find-node-trust-publication.md) memory leaves.
> Текущие узкие расширения допускают ровно две неактивные зависимости в
> `secure_session_store.go` и `trust.go`; обычные конструкторы передают ноль.
> Исходная standalone-область ниже изменена только для конечных публикаций
> [pin](find-node-pin-publication.md) и [trust](find-node-trust-publication.md).

The unused `internal/core/findnodebridge` package supplies only domain identity,
clock serialization and one-use native lock ownership. It imports the standard
library, has no production importer and changes no existing constructor. It has
no transport, Node, trust, degree, intake, handler, configuration, writer, timer,
worker, profile default, activation method or statistical hint policy. It does
not make C5 observations into admission permits. Runtime and resource acceptance
remain separate from this source implementation.

The base API follows the independently reviewed revised v2 surface; this private
candidate adds only the runtime pair method described below. Fresh CODE,
formatting and runtime qualification are still required for this derivative. `NewRuntimeDomain`
requires an immutable lifecycle channel and mints its own original `time.Now()`
monotonic origin. `NewFixtureDomain` requires a separate atomic clock and yields
a different immutable kind. Zero consumers and observers refuse. Wrapper field
names differ across roles, kinds and point/boundary types; callers cannot safely
convert one role into another or supply an arbitrary observation timestamp.

Every exported lease wrapper contains a pointer to one shared private cell.
Copying a wrapper copies that pointer, not an atomic or a mutex. Cell transitions
are LIVE → BUSY → LIVE for primitive Sample, and LIVE → CLOSING → CLOSED for one
terminal operation. Sample while BUSY refuses; terminal from BUSY panics before
unlocking. Repeated/zero/wrong-role terminal operations panic before domain or
native lock effects. Ordinary errors retain resources for the owner to release.
These checks do not observe external caller memory operations: future callers
must separately prove no lease escape and no concurrent early Complete/Release
between Sample and their own mutation. Acquiring another domain guard while a
lease is active, nested publication and taking clock after W are forbidden
caller sequences, not dynamically detected goroutine ownership rules.

`BeginCapture` briefly tries R and returns generation/domain without time. A
consumer's `TryReadClock` tries R, checks Ready/original generation/poison and
tries clock; failure releases every acquired resource. It never parks on a
native owner lock, which does not imply a scheduler, CPU or latency guarantee.
CommitReservation owns R+clock together and releases clock before R.

Mandatory nonclock publication owns W. Mandatory clocked publication acquires
clock then W and releases W then clock. Their acquisitions have no availability
error branch: constructing, closed Done, poison and generation exhaustion do not
veto cleanup. A zero Publisher returns a real one-use noop guard. A noop clocked
Sample refuses with no point and no clock read; Complete remains mandatory.
Callers still implement their own invalid-time cleanup and complete the guard.

Under an already-owned clock and shared BUSY claim, all real Samples follow:
raw q → nonnegative/non-MAX/nonregression validation → atomic poison postcheck →
last=q and tagged point. The optional early poison check is insufficient by
itself. A nonclock W completion can poison concurrently with clock-only
observation; the postcheck certifies the sticky latch was false at q. A later
poison store may coexist with a returned historical point. No returned point is
promised live at return. Commit Sample additionally checks the captured Done
channel AFTER raw/HWM/mint. Cancellation returns no point without poisoning;
last may already advance. Observation/cleanup clock points do not assert Ready,
Run readiness, complete membership or lifecycle-open.

Generation starts at1 and every real cleanup completion advances once under W,
saturating and poisoning at MAX rather than wrapping. SealConstruction is a
separate W-only decision. Ready repeat returns already-sealed without any writes,
even when poisoned. A first seal at generation>=MAX−1 poisons/refuses without
advancing or Ready; earlier poisoned construction also refuses without advance.
A successful first seal increments once and marks Ready. Its LP is the false
poison preflight; later clock-only poison may follow before physical return.

Valid observed q is in [0, MaxInt64). A pure exclusive boundary may equal
MaxInt64; raw observation at MaxInt64 poisons/refuses. Delta0 is allowed and its
boundary does not contain the same q. Negative/overflow arithmetic refuses
without domain mutation. Pure Matches/NotBefore/Contains retain immutable domain
and kind checks after release/poison and confer no present authority.

The source tests cover ownership copies, wrong views, BUSY/terminal arbitration,
try-lock cleanup, stale generations, construction/exhaustion, cancellation,
clock regression, mandatory cleanup and exact boundary arithmetic. Test-only
whitebox claims and atomic fixtures add no production callbacks or blocking
seams. Causal recipes label deterministic behavioral cases FUNCTIONAL and
uncontrolled adjacent-instruction/order/native-double-unlock cases SOURCE.
No raw/postcheck interleaving experiment is claimed. SOURCE readers are finite
standard-library parser/formatter checks and production import scanning, not a
compiler-read hermeticity proof.

The base intentionally retains the old F1 fixture. This leaf's source
ownership does not carry any R2/C5/degree/phase2A/B runtime qualification to the
new whole tree. Full Node writer/clock/temporal/construction closure, genuine
owned cuts and intake final commit, exclusive caller scope, producer integration, numeric/resource policy, G2, activation and release remain separate HOLDs.

### Runtime point and wall metadata from one reading

`ClockPublication.SampleRuntimePair() (OwnedInstant, time.Time, error)` is the
only additional exported method. Under the already-held clock/W and shared BUSY
claim, the uniform raw core takes one original `sample := time.Now()`, derives
elapsed with `sample.Sub(domain.origin)` and wall metadata with `sample.UTC()`.
The old three Sample signatures discard that metadata and use this same HWM
path. The observation LP is the actual raw reading, while the successful
LIVE→BUSY CAS is the reservation LP. The later false poison postcheck certifies
that the sticky latch was false at the raw LP; a racing poison may conservatively
refuse, and later poison does not turn the point into live authority.

The pair method rejects Fixture before its raw read/HWM, and a zero Publisher's
noop pair returns no point or wall value. Every error returns both values zero,
restores the claim to LIVE and retains the caller's mandatory Complete. The pair
method does not add Done/Ready checks: only CommitReservation.Sample retains its
post-sample Done check. Wall time is metadata and supplies no admission authority.
No Node caller, producer, constructor attachment or degree field is added here.

Real Publication and ClockPublication lease cells/wrappers are allocated before
their application-lock waits and remain local until acquisition succeeds. Native
order stays W for publication and clock→W for clocked publication, releasing W
before clock. This is source allocation placement, not a measured allocation,
GC, latency or caller no-escape guarantee. TryReadClock still allocates after
R+clock; a future no-allocation-under-intake contract must resolve that separately.
The owner must still exclude early/concurrent Complete during its own mutation.

New tests and virtual causal recipes are SOURCE-only. Old primitive25 or
standalone55 results do not qualify this method or the changed raw core/layout.
The existing one-writer d_win policy and its observed600/old timestamp/24h rules
are not changed or integrated by this leaf candidate.

## Русский

Пакет `internal/core/findnodebridge` не используется production-кодом. Он
предоставляет только идентичность домена, сериализацию часов и одноразовое
владение блокировками. Обычные конструкторы Node не изменены.
Нет интеграции с Node, trust, degree, intake, сетью, настройками, обработчиками,
писателями, таймерами или фоновыми задачами. Значения профилей и политика
стартовой статистики не выбираются; активация отсутствует.

Конструктор runtime сам получает исходное `time.Now()` с монотонной частью.
Fixture использует отдельный обязательный atomic clock и иной неизменный kind.
Копии lease разделяют одну ячейку LIVE/BUSY/CLOSING/CLOSED: конкурентный Sample
не читает часы, пока другой Sample BUSY; раннее завершение BUSY вызывает panic
до освобождения блокировки. Нулевой Publisher выдаёт одноразовый noop для
обязательной очистки, но не создаёт наблюдение времени.

Consumer одновременно владеет R и clock; неудачная попытка clock освобождает R.
Clocked publication получает clock перед W и освобождает W перед clock. Poison
запрещает новые наблюдения времени, а исчерпание generation — новый допуск consumer;
закрытый Done отклоняет только commit-наблюдение. Эти отказы не отменяют
обязательную очистку. Все реальные Sample выполняют raw → validation → повторную
атомарную проверку poison → HWM/point. Только commit проверяет Done после raw.
Отдельного API для преждевременной проверки lifecycle нет.

SealConstruction не является cleanup-публикацией: повтор Ready ничего не меняет;
при generation>=MAX−1 первый seal отказывает и не переводит домен в Ready.
Монотонное наблюдение MAX недопустимо, но вычисленная исключающая граница MAX
представима. Граница с delta0 не содержит исходную точку. Чистое сравнение
идентичности остаётся возможным после release/poison и не даёт права допуска.

Примитив не видит внешние записи вызывающего кода. Будущая интеграция должна
отдельно доказать исключительное владение scope, отсутствие раннего Complete и
утечки lease, полноту writer-family и корректность финальной временной проверки.
Полный мост, интеграция статистического producer, G2, ресурсная приёмка и выпуск остаются HOLD.
Тесты и причинные контроли этого среза пока являются только исходниками; их
результаты, компиляция и форматирование не заявляются.

### Runtime-точка и wall-метаданные одного чтения

Единственное дополнение API —
`ClockPublication.SampleRuntimePair() (OwnedInstant, time.Time, error)`.
При уже удерживаемых clock/W и BUSY общий raw core получает один исходный
`sample := time.Now()`: elapsed вычисляется через `sample.Sub(domain.origin)`,
а UTC-метаданные — через `sample.UTC()`. Прежние три Sample сохраняют сигнатуры
и используют тот же HWM, отбрасывая wall. Observation LP — само raw-чтение;
reservation LP — успешный LIVE→BUSY CAS. Последующий false postcheck подтверждает,
что sticky poison был false в raw LP; конкурентный poison может консервативно
отклонить результат. Возвращённая историческая точка не обещает текущий допуск.

Fixture отклоняется до raw/HWM; noop не создаёт пару. Любая ошибка возвращает
нулевые point/wall, восстанавливает LIVE и сохраняет обязательный Complete.
Пара не добавляет проверки Done/Ready: Done после sample проверяет только commit.
Wall остаётся метаданными. Caller Node, producer, новые поля degree и wiring
конструктора не добавлены; правила d_win600/старой даты/24h не меняются.

Lease/wrapper Publication и ClockPublication выделяются до ожидания application
locks и не передаются наружу до их получения. Порядок clock→W и освобождение
W→clock сохранены. Это SOURCE-размещение, а не измерение allocation/GC/latency.
TryReadClock по-прежнему выделяет guard после R+clock; будущая гарантия отсутствия
allocation под intake требует отдельного решения. Прежние25/55 QA не являются
приёмкой расширенного API. Новые тесты/recipes пока NOTRUN; полный мост остаётся HOLD.
