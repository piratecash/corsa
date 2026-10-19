# C5/K2A: inactive signed-record contact protection

## English

This implements private candidate/verifier/stage and bounded Service-owned completion foundations.
No record ingress calls the candidate/verifier/stage roots. The existing joined contact worker can
complete genuinely registered owed stages; no record admission queue/rate/warmup, RPC/operator K
addition, named dial, priority or activation is installed. K1's existing fresh-session producer and R1a's TargetProof verifier are unchanged. This is
not completed K2, resource acceptance, S6, G2 or D1 release admission.

A candidate captures the **original durable explicit local contact fact** and pin-store owner before
asynchronous verification/import. An actual exact-identity/configured-network signed IdentityRecord
must declare min_session>=2. Received raw body, contact Source, old format, TLS, TargetProof, issued_at
or a newly inferred record TTL cannot substitute for that conjunction. Signature verification runs
outside trust/pin mutexes. The record bytes are frozen verbatim, including optional/unknown signed
fields; raw JSON is never re-marshalled to construct authority.

Stage preflights the strict backend outside memory locks. Under the actual evidence read gate it
rechecks the original confirmed fact, exact pin incarnation or original absence/removal epoch, and
current persistent record version/body/sequence. A newer min1/no-min, contact delete/readd, destructive
absence epoch, owner replacement or cancellation before effects refuses. The read gate spans the
current-record check through pin mutation. A new identity uses the existing hard20,000 limit including
C3 restoration reservation, without eviction or a second K quota. Complete same-identity backup metadata
and pinnedAt are preserved. Existing Contact allocation is never replaced; other classes and raw R8192
accounting including K remain intact. Evidence/protection overflow checks precede every increment.
Record evidence creates protection ordering/incarnation, never fabricated v2-session proof.

The pin entry atomically retains one shared owed state before stage returns. Copies of candidate/
stage handles share it. Fresh candidates join a pending owner without new revisions or allocation;
a completed owner yields historical completion only. An already Eligible present K1 or loaded K
without a record owner yields AlreadyEligibleNoRecordObligation: no synthetic evidence/barrier/job.
Original candidate eligibility checks still precede fresh joins; exact candidate replay returns its
original disposition, including a no-job outcome. There is no service pending map or second inventory.
A future live ingress still needs its reviewed original-event capture and startup reconciliation.
This foundation does not bound arbitrary external caller handles or make K2 a live producer.

Once legitimately staged, **historical K and owed close/drain/purge survive** contact deletion, a newer
min1/no-min, ordinary reproof, unrelated metadata, cancellation and strict Unknown. Original contact/
record stamps then describe history, not fresh eligibility. Loss of earned K in the same incarnation
is DeferredInvariant with the obligation retained; completion never silently re-stages it. C3 clean
remove/recreate, absent recovery or store restart makes the old incarnation-bound ticket stale forever.
A protective rollback preserving the same entry does not itself revoke the ticket, subject to existing
store safety/refusal checks. A completed result remains historical after later removal; it does not
certify current trust or recreate protection.

Every incomplete completion attempt forces a **fresh strict full covering snapshot** for the
same incarnation and earned Contact allocation, even if an ordinary save or earlier ACK already wrote
it. The joined worker can retry this same original ticket; it never re-stages it.
Only DurableCommitted without an error yields a receipt containing
the actual snapshot digest and captured protection revision. Current metadata is ACKed only when its
captured revision still matches; a concurrent newer revision remains dirty/unACKed. Receipt coverage is
historical, never a claim that every current metadata change is durable. NotCommitted/Indeterminate/
error keeps the ticket and creates no new receipt. A previous historical receipt may remain diagnostic;
attemptCovered explicitly says whether **this** invocation obtained a confirmed covering snapshot.
Commit outranks late cancellation, retaining CoveredOwed and the receipt before the barrier.

With all trust/store/evidence/removal locks released, completion invokes the actual common
publishIdentityProtection: close legacy claims, forget claimed addresses, drain pre-pin admitted
writers, purge their residue. It checks exact incarnation/earned K and store safety before and after.
Current contact and old evidence/protection revisions do not suppress cleanup. A cancelled drain keeps
the existing owePurge handoff to the last writer and leaves the original ticket retryable. Completion
is marked HistoricalProtectionCompleted only after the real barrier and final owner check. If C3 wins
during the unlocked barrier, stale output marks that the barrier was attempted and effects may be
partial; no replacement is re-pinned or rebound. Repeated successful completion is a historical no-op.

Lock order: candidate.mu → trustMutationMu → pinMutationGate.R → trust.pinEvidenceMu.R → brief trust.mu,
released before brief pin-store.mu. The public pin RLock facade is never called recursively while
already holding the read gate: a queued C3 writer would deadlock it. Generic merge/contact/fact changes
take pinEvidence WRITE before trust.mu and release both before persistence. Bound fact staging keeps
saveMu before the new evidence gate. Fact ACK/proof-pointer/last-online refresh preserves original
fact identity/revision and does not need this gate. Owed mutex serializes one shared ticket;
strict writer order remains removalGate.R → writeMu → brief store.mu, with encoding/I/O outside store.mu.
No trust/store/evidence/removal/write lock spans crypto or the legacy barrier. The ticket mutex may
span the barrier; owed.mu -> unprovenRouting.mu is permitted, with no reverse edge.

Common merge clones input Body/Sig and parsed DTypes; recordFor clones both raw and parsed output;
recordBodies clones DTypes. Existing Types/WireField copies preserve absent versus explicit-empty.
Private recordLocked/getLocked values remain borrowed under trust.mu and must be cloned before escape.
The existing trust snapshot still copies the contact/conflict maps and base64-encodes every persistent
record under trust.mu/new evidence sections. That proportional cost is preserved, not called O(1),
encoding-free or covered by a per-candidate bound. One candidate's raw payload is at most2048+64 bytes;
object overhead, full trust/pin snapshots and external handle counts are additional. Classified-resource
acceptance remains open.

### Owned completion, pacing and truthful limits

Optional classified metadata holds one ticket plus a store-owned progress mirror: Pending,
CoveredOwed, DeferredInvariant or Completed. Store readers never take the ticket mutex or read its
mutable completion. Generic strict metadata ACK does not set Completed. Only the actual barrier plus
an exact current owner/incarnation, same-ticket and earned-Contact check publishes that mirror.
Completed history does not ACK a later metadata revision. Canonical format2/backup never persist
runtime tickets, mirrors or receipts; loaded K remains historical protection, not a fresh K2 event.

Ordinary COW metadata changes preserve the ticket/mirror. Changing/removing its earned Contact,
including an all-zero metadata update, refuses before counters/effects. ACK and unrelated classes
preserve ownership; protective rollback of the same entry does too. Clean C3 deletion/recreation or
absent recovery never rebind an old ticket. Unexpected out-of-contract loss remains DeferredInvariant.

The existing contact worker offers K1 first and then one RR-selected K2 ticket, even when K1 returns
an error. Selection scans at most20,000 current pin entries under store.mu, without retaining a batch
or scanning the uncapped contact book. Cursor advances on selection, including Busy/error/deferred.
TryLock returns Busy if an explicit caller holds the ticket. Minimum whole-pass start spacing and
K2 cooperative attempt ceiling are both 2 seconds. Capacity-one wakes coalesce while the pacing timer
waits; they cannot trigger immediate retries. One active attempt timer is cancelled on return; no
per-identity timers/goroutines/history queue are retained.

These are cooperative bounds. A mutex holder or executing filesystem syscall can outlive context
cancellation. The same worker/reference remains owned until it really returns; shutdown join remains
pending, with no detached I/O or replacement worker pretending completion. No unconditional latency
or resource acceptance is claimed. Service-owned retention is at most20,000 resident tickets plus
one selected reference which may retire during C3; external handles/old stores/C3 undo copies and
COW/full-snapshot allocations are additional. Normally unclassified entries gain no mandatory field.

Cancelled waitDrained removes only its own channel under the routing-writer leaf mutex, nils the
removed slice tail and never closes it. Other waiters and the last writer's owed purge survive;
leave alone closes surviving waiters after the actual purge. Repeated cancellations therefore do
not retain one extra channel per retry. No aggregate cap on other callers' active waiters is claimed.

## Русский

Реализованы private candidate/verifier/stage и bounded Service-owned completion foundations.
Record ingress не вызывает candidate/verifier/stage. Прежний joined contact worker доводит реально
зарегистрированные owed stage; queue/rate/warmup, RPC/operator K addition, named dial и priority
не добавлены. Активации нет; существующие K1 fresh-session producer и R1a TargetProof verifier не меняются.
Это не полный K2, resource acceptance, S6, G2 или допуск выпуска D1.

Candidate до асинхронных verification/import захватывает **исходный durable explicit local contact
fact** и pin owner. Настоящий signed IdentityRecord должен доказать точные identity/настроенную сеть
и объявить min_session>=2. Raw body, contact Source, старый формат, TLS, TargetProof, issued_at или
выдуманный record TTL это сочетание не заменяют. Подпись проверяется вне trust/pin mutex; signed bytes,
включая optional/unknown поля, сохраняются verbatim без повторного marshal.

Stage делает strict preflight вне memory locks. Под actual evidence read gate перепроверяются исходный
confirmed fact, pin incarnation либо исходные absence/removal epoch и текущие persistent version/body/seq.
Новый min1/no-min, delete/readd контакта, смена owner/absence epoch или отмена до effects отказывают.
Read gate сохраняется от проверки record до pin mutation. Новая identity занимает место прежнего hard20,000
с C3 restoration reservation; нет eviction или отдельной K quota. Сохраняются полные same-identity backup
metadata/pinnedAt, уже заработанный Contact, остальные классы и raw R8192 включая K. Overflow проверяется
до инкрементов. Record evidence создаёт protection ordering/incarnation, не поддельный v2-session proof.

Pin entry атомарно удерживает общий owed state до возврата stage. Копии handles делят его; fresh
candidate присоединяется к pending owner без новых revisions/allocation. Completed owner возвращает
только историю. Present Eligible K1/loaded K без record owner получает AlreadyEligibleNoRecordObligation:
нет synthetic evidence/barrier/job. Fresh join сохраняет original eligibility checks; exact replay
возвращает прежний disposition, включая no-job. Service pending map/второго inventory нет. Будущему
live ingress ещё нужны проверенные original-event capture/startup reconciliation; произвольные
внешние handles не ограничены, K2 live producer этим не подключён.

После законного stage **historical K и долг close/drain/purge сохраняются** при удалении контакта,
новом min1/no-min, ordinary reproof, unrelated metadata, отмене и strict Unknown. Старые fact/record
stamps — история, не новая eligibility. Потеря earned K в той же incarnation даёт DeferredInvariant
с сохранением долга; completion его молча не создаёт заново. C3 clean remove/recreate, absent recovery
или restart store навсегда делают старый ticket stale. Protective rollback с той же entry сам его
не отзывает при соблюдении текущих store safety checks. Завершённый результат остаётся историческим
после удаления; он не подтверждает нынешнее доверие и не восстанавливает pin.

Каждая незавершённая явная попытка completion требует **новый strict full covering snapshot** той же
incarnation/earned Contact, даже после ordinary save/старого ACK. Joined worker повторяет тот же original ticket без нового stage.
Только DurableCommitted без error создаёт receipt с actual snapshot digest/captured protection revision.
ACK получают только всё ещё совпадающие captured revisions; новые metadata остаются dirty/unACKed.
Receipt покрывает исторический snapshot, не все нынешние изменения. NotCommitted/Indeterminate/error
сохраняют ticket и не создают нового receipt. Старый receipt может остаться диагностикой;
attemptCovered явно отмечает, получила ли **эта** попытка подтверждённый covering snapshot. Commit
важнее поздней отмены: CoveredOwed и receipt сохраняются до barrier.

После освобождения всех trust/store/evidence/removal locks вызывается настоящий общий
publishIdentityProtection: close legacy claims, forget claimed addresses, drain уже допущенных до pin
writers и purge residue. До/после проверяются exact incarnation/earned K/store safety. Нынешний контакт
и старые evidence/protection revisions cleanup не подавляют. Отменённый drain сохраняет owePurge последнему
writer и retry исходного ticket. HistoricalProtectionCompleted выставляется только после actual barrier
и финальной проверки. Если C3 выигрывает во время unlocked barrier, stale outcome отмечает попытку barrier
и возможность partial effects, без repin/rebind replacement. Повтор успешного completion — historical no-op.

Порядок locks: candidate.mu → trustMutationMu → pinMutationGate.R → trust.pinEvidenceMu.R → короткий
trust.mu, освобождённый до короткого pin-store.mu. Public RLock facade рекурсивно не вызывается под
read gate: ожидающий C3 writer вызвал бы deadlock. Generic merge/contact/fact mutations берут evidence
WRITE до trust.mu и снимают оба до persistence. Bound fact staging сохраняет saveMu перед новым gate.
Fact ACK/proof-pointer/last-online refresh сохраняют fact ID/revision и gate не требуют. Owed mutex
сериализует только внешний ticket. Strict writer сохраняет removalGate.R → writeMu → короткий store.mu;
encoding/I/O вне store.mu. Trust/store/evidence/removal/write locks не охватывают crypto/legacy barrier. Ticket mutex может
охватывать barrier; разрешено owed.mu -> unprovenRouting.mu, обратного ребра нет.

Common merge копирует входные Body/Sig и parsed DTypes; recordFor возвращает копии raw/parsed, recordBodies
копирует DTypes. Types/WireField сохраняют отсутствие и explicit-empty. Private recordLocked/getLocked
возвращают borrowed values под trust.mu, которые копируют до escape. Прежний trust snapshot всё ещё
копирует contact/conflict maps и base64-encode все persistent records под trust.mu/new evidence sections.
Эта пропорциональная цена не объявляется O(1), encoding-free или покрытой размером candidate. Его raw
payload не больше2048+64 байт; object overhead, полные trust/pin snapshots и внешние handles добавочны.
Classified-resource acceptance остаётся открытым.

### Владение completion, pacing и честные границы

Optional classified metadata хранит один ticket и store-owned mirror: Pending, CoveredOwed,
DeferredInvariant или Completed. Store readers не берут ticket mutex и не читают mutable completion.
Generic strict ACK не выставляет Completed: только actual barrier с final exact owner/incarnation,
same-ticket/earned-Contact check. История completion не ACKed позднейший metadata revision.
Format2/backup не сериализуют runtime tickets/mirrors/receipts; loaded K — историческая защита,
а не fresh record event.

Ordinary COW сохраняет ticket/mirror. Изменение/удаление earned Contact, включая all-zero metadata,
отклоняется до counters/effects. ACK, unrelated classes и protective rollback той же entry сохраняют
owner. Clean C3 remove/recreate/absent recovery не привязывают старый ticket к новому владельцу.
Unexpected вне-контрактная потеря остаётся DeferredInvariant.

Прежний contact worker сначала даёт возможность K1, затем одному RR-selected K2 ticket, даже после
ошибки K1. Выбор — O(<=20,000 pins) scan под store.mu без batch через I/O и без uncapped contact-book
scan. Cursor продвигается при выборе, включая Busy/error/deferred. TryLock возвращает Busy при
занятом явным caller ticket. Минимум между началами passes и cooperative ceiling K2 attempt — 2 секунды.
Capacity-one wake coalesces во время pacing timer, immediate retry loop нет. Один active attempt timer
отменяется по return; per-identity timers/goroutines/history queue нет.

Это cooperative границы: mutex holder или выполняющийся filesystem syscall могут пережить отмену.
Тот же worker/reference остаётся владельцем до actual return; shutdown join ждёт, detached I/O и
replacement worker не изображают завершение. Нет unconditional latency/resource acceptance.
Service удерживает <=20,000 resident tickets плюс одну selected reference, которая может устареть
при C3. Внешние handles/old stores/C3 undo copies и COW/full snapshots добавочны; normally unclassified
entry не получает нового обязательного поля.

Cancelled waitDrained удаляет только свой channel под routing-writer leaf mutex, зануляет удалённый
slice tail и не закрывает channel. Чужие waiters и owed purge последнего writer сохраняются; только
leave закрывает surviving waiters после настоящей purge. Последовательные отмены не удерживают новый
channel на каждый retry. Общий cap чужих одновременно active waiters этим не утверждён.
