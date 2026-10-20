# Inactive Relation record admission core

## English

This candidate implements the already accepted
[record-admission decision](session-v2-relation-record-admission.md) in a private
candidate composition. The ordinary constructor keeps `recordAdmission=nil`.
No live ingress, public setting, flag, handler, capability, overlay priority,
K admission consumer, R2 producer, G2 or release is enabled.

`identityResolver.mu` owns one fixed 256-slot array, the 30-event elapsed ring,
qualification sequence and lifecycle. A prospective slot is reserved before
capturing the original pin owner, while the removal gate remains held through
actual attempt registration. Contention uses TryRLock/TryLock and opts out of R
for that attempt without blocking ordinary SendLocal. Generic frames, Recovery,
Presence, late UI joins and soft R0 diagnostics cannot supply missing provenance.
Only the original opaque Window action and registered question/label/generation
can transfer. Both verified answer and SendQueued must be observed in the
original inclusive 60-second attempt window. Qualification order determines R
FIFO; prospectives do not become unverified FIFO heads. Slot generations cannot
wrap. Processing pins the buffer until actual owner return, including cancel.

The normal key-resolution Success remains immediate and may precede SendQueued.
The physical slot acquires its record-protection ref before Success releases the
resolution's ref. Failed finish clears that ref and authority; reconcile counts
all slot-held refs under the resolver owner. Explicit cancellation scans at most
256 exact original action/resolution holders, including after ordinary Success;
it never looks up a replacement action by target. Runtime authority is revoked
even if later intent cleanup fails. No completed action/digest history is kept.

A shared sampling boundary strips the Go monotonic reading from the wall value
with Round(0), for both default and injected clocks. Retained startup/current/
original wall samples and opt-in createdAt use that same normalized paired
sample. Wall-deadline comparison also normalizes both operands defensively;
elapsed startup/rate and the original elapsed upper bound remain unchanged.
A separate observed clock supplies elapsed startup, rate and original lifetime.
Each actual Run waits 60 elapsed seconds; a new sequential owner gets a fresh
wait and empty ring. Rate uses a half-open trailing 60-second window: the exact
oldest+60 event expires; a stage is the only debit. The original resolution's
paired wall and elapsed seven-day upper bounds are immutable, inclusive at the
endpoint. Wall rollback cannot extend elapsed life, nor create rate allowance.
Reseeded intents have no observed original elapsed creation and remain ordinary
lookup-only in this candidate. Qualified queue residence may exceed attempt60.

Final stage locks are `pinGate.R -> trust.pinEvidenceMu.R -> resolver.mu ->
trust.mu.R -> store.mu`; cancellation/lifetime is rechecked after all waits.
Current signed record/sequence/min2 and original store incarnation or absent
removal epoch must still match. Retained trust R excludes a durable-contact
confirmation gap. Installed K, or a current confirmed explicit local contact
plus this min2 record, returns ContactPathRequired: no R debit/stage, no synthetic
K readiness. Otherwise all capacity/revision/rate trials precede Relation stage;
the prepared rate debit and slot-owned earned result are infallible afterwards.
Raw R accounting remains 8192 including R+K, unconfirmed and C3 reservation; the
20k unique bound is unchanged. No new K/R/N priority policy is inferred.

The single worker opens its immutable factory-bound backend outside owner locks.
The private dependency permits generated-backend failure controls; it is not a
configurable live port. Only one qualified head performs synchronous I/O. The
existing two-second whole-pass cadence prevents wake/retry busy loops; the
existing two-second attempt context is cooperative. Slow filesystem/kernel
calls may delay shutdown beyond it. No detached I/O goroutine fabricates a hard
bound. Unsupported preflight refuses before stage; transient errors retain the
same head. A staged head blocks later R work until completion or stale owner.

Earned completion binds original incarnation and the earned Relation allocation,
not a transient exact proof/protection revision. Reproof or unrelated metadata
cannot erase close/drain/purge already owed. Each retry writes a fresh strict
full snapshot even if the ordinary writer is current, ACKs only captured
metadata revisions, and retains a historical covering receipt. It preserves
format3 N-in rows/domain/highwater, capturing the fixed quota stamp before I/O
and recording it only on error-free DurableCommitted. A later quota suffix or
metadata change remains unACKed. Ordinary writes never grant this ACK. Unknown
has no promise that old disk bytes survived, no refund and no automatic C3
fence/network stop.

After covering commit, even cancellation leaves the real shared legacy
close/drain/purge owed. The barrier holds no pin gate/trust/store/resolver lock.
Continuity is checked before and after it; a concurrent C3 close cannot be
undone. C3 removal/recreation, including absent historical rollback restoration,
creates a new incarnation: the old ticket becomes stale and never repins.
Shutdown clears all replayable prestage authority, joins the actual worker,
then retains at most one StoppedOwed head in the same closed slot. Raw evidence,
action/resolution references and seq-floor ref are removed only after actual
return. This does not promise earned recovery after object destruction/restart.

The raw arrays have a structural upper bound of
`256 * (2048 + 64 + 64) = 557056` bytes. Headers, fixed clock/rate/receipt state,
256 bounded refs and underlying already-existing resolution/attempt companions
cost more; structural tests report actual Go `Sizeof`. Baseline parser/import
allocations and caller-held values are separate. Existing resolution/attempt
cardinality is not limited by these 256 slots; stale optional refs in those
baseline rows cannot address a recycled generation, and their header costs are
outside the fixed raw-owner bound. There is no second live map,
queue, per-candidate goroutine or resource/RSS acceptance claim. A strict write
still builds/encodes the existing full pin/endpoint snapshot and, for format3,
the journal; this is neither O(1) nor I/O under the store mutex. Existing snapshot
construction/base64 work under its owner is preserved, encoding/file I/O occurs
after releasing it. Full build/lint/native and combined acceptance remain
independent gates after this scoped candidate.

## Русский

Кандидат реализует уже принятое
[решение record admission](session-v2-relation-record-admission.md) только в
закрытой fixture-композиции. У обычного конструктора `recordAdmission=nil`.
Live ingress, публичные настройки/флаги/handlers, capability, overlay priority,
K consumer, R2 producer, G2 и выпуск не включаются.

`identityResolver.mu` владеет одним массивом 256 slots, elapsed ring30, FIFO
sequence и lifecycle. Prospective резервируется до capture исходного pin owner;
removal gate остаётся взят до фактической регистрации attempt. TryRLock/TryLock
при contention снимают только R authority этого attempt, обычный SendLocal
продолжается. Generic Frame, Recovery, Presence, поздний UI join и soft R0 не
заменяют исходный opaque Window action/label/generation/question. И verified
answer, и SendQueued должны наблюдаться в исходном inclusive60s окне. Порядок
qualification задаёт R FIFO, непроверенные prospectives головой не становятся.
Generation не переполняется. Processing удерживает buffer до actual return.

Обычный key-resolution Success немедленен и может опередить SendQueued. Slot
берёт record-protection ref до освобождения resolution ref; failed finish
снимает его и authority. Reconcile учитывает slots под resolver.mu. Explicit
cancel сканирует максимум256 точных исходных action/resolution holders и после
Success, не берёт action нового target resolution. Runtime authority отзывается
и при ошибке последующей intent cleanup. Истории completed action/digest нет.

Общая sampling boundary снимает Go monotonic reading с wall через Round(0)
для default и injected clocks. Retained startup/current/original wall и opt-in
createdAt получают ту же нормализованную пару. Wall comparison защитно
нормализует оба операнда; elapsed startup/rate и исходная elapsed граница не
меняются. Отдельные observed elapsed часы задают startup/rate/исходный lifetime. Каждый
настоящий Run ждёт60elapsed seconds; новый последовательный owner ждёт заново.
Rate — half-open rolling60: oldest+60 истекает, списание только при stage.
Исходные paired wall/elapsed границы7d неизменны, endpoint inclusive. Wall rollback
не продляет elapsed life и не даёт allowance. Resumed intents без доказанного
исходного elapsed anchor сохраняют ordinary lookup, но не R authority. После
qualification очередь может жить дольше attempt60.

Порядок final stage: `pinGate.R -> trust.pinEvidenceMu.R -> resolver.mu ->
trust.mu.R -> store.mu`; cancel/lifetime проверяются после всех ожиданий.
Нужны текущие signed record/seq/min2 и исходный owner incarnation либо absent
removal epoch. Удержанный trust R исключает разрыв с durable contact confirmation.
Installed K либо текущий confirmed explicit local contact с этим min2 record
даёт ContactPathRequired без R stage/debit и без выдуманной K readiness. Иначе
все capacity/revision/rate trials идут до Relation stage; подготовленные debit
и slot-owned earned result после него infallible. Raw R8192 считает R+K,
unconfirmed и C3 reservation; unique20k неизменён. Новая priority policy не вводится.

Один worker открывает immutable factory-bound backend вне locks. Эта private
зависимость нужна и для generated-backend failure controls, не является live
config port. Только одна head синхронно владеет I/O. Существующий whole-pass
cadence2s не даёт wake/retry busy loop; attempt context2s cooperative. Зависший
filesystem/syscall может задержать shutdown дольше. Detached I/O нет. Unsupported
preflight отказывает до stage, transient сохраняет ту же head. Earned head не
пропускает новые R до completion либо stale owner.

Earned completion связан с исходной incarnation и заработанным Relation
allocation, не с exact transient proof/protection revision. Reproof/COW не
снимает close/drain/purge. Retry пишет свежий strict full snapshot даже после
обычного save, ACK только captured metadata revisions; receipt исторический.
Format3 N-in rows/domain/highwater сохраняются: quota stamp фиксируется до I/O и
учитывается только при nil-error DurableCommitted. Более поздние suffix/metadata
не ACK. Ordinary write этого ACK не даёт. Unknown не обещает старые disk bytes,
не возвращает debit и не включает C3 fence/network stop.

После commit отмена не снимает реальный общий legacy close/drain/purge. Barrier
не держит gate/trust/store/resolver locks; continuity проверяется до и после.
Уже начатый close при concurrent C3 нельзя отменить. C3 remove/recreate и absent
rollback restore дают новую incarnation: старый ticket stale, без repin.
Shutdown очищает prestage authority, реально joins worker, затем максимум одна
StoppedOwed остаётся в том же закрытом slot. Raw/action/resolution/seq-floor refs
снимаются только после actual return. Across-restart recovery owed не обещается.

Структурный raw bound: `256*(2048+64+64)=557056` bytes. Headers, fixed state,
bounded refs и существующие resolution/attempt companions стоят дополнительно;
тест выводит Go Sizeof. Парсер/import allocations и внешние handles считаются
отдельно. Число baseline resolutions/attempts этим256 не ограничивается; старые
optional refs не адресуют recycled generation, а их headers не входят в raw bound. Второго map/queue/per-candidate goroutine и RSS acceptance нет. Strict
write по-прежнему строит/кодирует полный pin/endpoint snapshot и format3 journal:
это не O(1). Существующая snapshot/base64 работа под owner сохраняется; внешнее
кодирование/дисковый I/O идут после освобождения store.mu. Full build/lint/native
и combined acceptance остаются отдельными gates.


## Private R-only Service composition / Закрытая R-композиция Service

The unused `newRelationRecordCandidateService` factory selects only ChargedR.
The final resolver and immutable real-store backend are constructed after
secureSessions and the self-record. The registered post_identity handler reads
that final resolver through its original Service; R-only construction captures
no K push port or K transport capability. None and K construction retain their
previous branches; R|K and unknown masks refuse. No public factory/config/RPC
selects this candidate.

The new finite fixtures use actual Service.Run for startup60 and its one joined
R worker, typed user lookup, actual SendLocal class-queue acceptance, the actual
registered answer handler and a real strict backend. They never manually begin
the epoch, replace the resolver, or call processRelationSlot. A separate fixture
wraps the original real WriteMain: it delegates the real write and then holds
callback return to test Run join. This is callback ownership evidence, not a
blocked kernel syscall, TLS path or full Window-to-network integration. The
construction and positive fixtures keep the original backend method value.
Injected paired time demonstrates policy boundaries without claiming 60 real
elapsed seconds. Fresh CODE/TYPE/runtime acceptance is required for these
fixtures; their presence alone is not a test result or activation.

Неиспользуемая закрытая factory `newRelationRecordCandidateService` выбирает
только ChargedR. Финальный resolver и backend реального store создаются после
secureSessions и self-record. Зарегистрированный post_identity обращается к
финальному resolver через исходный Service; K port и K transport capability у
R-композиции отсутствуют. Ветки None/K сохранены, R|K и неизвестные mask
отклоняются; публичные factory/config/RPC кандидата не включают.

Новые конечные fixtures используют настоящий Service.Run: startup60, один
присоединённый R worker, typed user lookup, реальный SendLocal с приёмом в class
queue, зарегистрированный answer handler и strict backend. Ручного begin,
замены resolver и processRelationSlot в них нет. Отдельный wrapper исходного
реального WriteMain сначала делегирует запись, затем удерживает возврат callback
для проверки join. Это граница владения callback, не зависший syscall, TLS или
полный путь Window-to-network. Constructor и positive fixture сохраняют исходный
method value backend. Подставленное парное время не является 60 секундами реального
ожидания. Для новых fixtures ещё нужны CODE/TYPE/runtime проверки; наличие кода
не считается результатом теста или разрешением активации.
