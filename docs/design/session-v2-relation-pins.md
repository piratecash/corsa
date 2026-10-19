# Record-only R protection: inactive foundation

R1a provides private store primitives; no production caller creates an R pin from
a received record. The genuine Window lookup still resolves keys without adding
R protection to a target with no direct proven session. R1b integration remains
unfinished. There is no new queue, worker, limiter, startup warmup, RPC, capability
or network activation in this increment.

The private record verifier checks the real signed identity record and the target
proof for the exact network, target, label, question hash and minimum sequence.
The record must declare `min_session >= 2`. Its informational `issued_at` creates
no expiry policy. A private verified capsule is separate from both TLS proof and
trusted Window action authority; future production admission still needs the
reviewed pre-attempt action, queue, cancellation and rate owners. It cannot use
a generic import, claimed sender, boolean or fabricated `ProvenIdentity`.

An owner observation binds the running store and either an existing entry
incarnation or explicit absence plus a destructive-removal epoch. Existing
observations survive normal reproof and metadata changes; deletion/recreation
and restart invalidate them. Absence never rebinds to a later present entry.
The runtime epoch advances immediately before external `BeginRemoval`, after
the validated removal preflight/token/backup/guard/cancellation checks. Every
later error, Unknown outcome and protective restore retains that increment.
Overflow refuses before external destructive effects. Unrelated removals
conservatively invalidate absent observations, without a tombstone/history map.

The legacy wire field/type `ProofRevision` remains compatible. Its private name
is now `evidenceRevision`: historical loading/restoration already advanced this
ordering without a live session. Genuine TLS construction remains distinct.
Record-only construction invents neither a session nor endpoint, source group,
nearest hop or current contact trust. A new target uses trusted local time;
an existing target preserves its original time, incarnation and other metadata.
A same-identity absent C3 backup restores its complete old facts/time before
adding R, consuming only its own reserved places. Existing recovery/fatal-store
refusals remain in force.

R occupancy counts **all non-absent Relation allocations**, including K+R,
unconfirmed state and an absent C3 restoration reservation. The bounds remain
8192 R allocations and 20000 unique protected identities. Contact has no own
class quota; it does not subtract an R allocation or change Selected accounting.
Admission checks and revision allocation precede atomic publication. Unavailable
accounting refuses; earlier overcapacity is preserved and reported. There is no
eviction. Metadata promotion of an existing identity uses no new global place.

Strict confirmation uses the existing forced full-snapshot writer and captured
protection revisions. Ordinary persistence does not acknowledge metadata. An
unknown strict outcome leaves counted, unconfirmed protection; a later strict
retry can acknowledge the current revision. Fresh accepted record evidence can
invalidate an older C3 preview while preserving an unchanged class ACK; that
does not claim a newly persisted signed-record proof.

The genuine TLS wrapper and the inactive record capability share legacy
close/forget/drain/purge work outside owner locks. Cancellation passes the owed
purge to the last admitted legacy writer. A record capability is checked against
the current store entry before and after the barrier; stale publication cannot
recreate a removed pin. Shared diagnostics say that the identity requires v2,
without claiming record-only protection came from a TLS session.

R1a adds only fixed runtime fields and owner-maintained counters to existing
entries/store, plus ephemeral private witnesses. No second identity map or
completed-record history exists. This is not a memory/resource acceptance result:
full snapshots, earlier overcapacity and uncapped pending lookup inventories
retain their existing costs. Functional/race/mutant controls do not establish
full R1, T5, G2 or D1 readiness.

The future aggregate30/min and queue256 policy still needs an explicit owner
choice of rolling/burst/restart semantics, service order, fresh existing-class
record cost and original-task deadline transfer. The separately reviewed
proposal is not implemented here. No K2/R2 or unsolicited-record producer is
connected by this foundation.

## Russian

# Record-only защита R: неактивная основа

R1a добавляет private store primitives; production-код не создаёт R pin из
полученного record. Настоящий Window lookup по-прежнему получает ключи без R
защиты target, с которым не было прямой доказанной сессии. Интеграция R1b не
завершена. В этом инкременте нет новой очереди, worker, limiter, startup warmup,
RPC, capability или сетевой активации.

Private verifier проверяет настоящую подпись identity record и target proof
для точных network, target, label, question hash и minimum sequence. Record
должен заявлять `min_session >= 2`. Информационный `issued_at` не задаёт срок
годности. Private verified capsule отделён от TLS proof и authority настоящего
Window action; будущему production admission нужны проверенные pre-attempt
action, queue, cancellation и rate owners. Generic import, claimed sender,
boolean или поддельный `ProvenIdentity` не заменяют эту границу.

Owner observation связывает текущий store и либо incarnation существующей
записи, либо явное отсутствие с destructive-removal epoch. Обычные reproof и
metadata update сохраняют existing observation; delete/recreate и restart её
инвалидируют. Absence не привязывается заново к появившейся записи. Runtime epoch
увеличивается непосредственно до внешнего `BeginRemoval`, после проверок
preflight/token/backup/guard/cancellation. Любые последующие error, Unknown и
protective restore сохраняют increment. Overflow отказывает до внешнего
destructive effect. Чужое снятие консервативно инвалидирует absent observations
без tombstone/history map.

Внешние wire field/type `ProofRevision` совместимы с прежними. Private имя
теперь `evidenceRevision`: исторические load/restore и раньше меняли этот порядок
без живой сессии. Настоящий TLS construction остаётся отдельным. Record-only
construction не создаёт session, endpoint, source group, nearest hop или текущий
contact trust. Новый target получает trusted local time; существующий сохраняет
исходное время, incarnation и остальные metadata. Для absent C3 backup той же
identity сначала восстанавливаются все старые факты/время и затем добавляется R,
с использованием только собственных reserved places. Действующие recovery и
fatal-store отказы сохраняются.

R occupancy считает **все non-absent Relation allocations**, включая K+R,
unconfirmed state и absent C3 restoration reservation. Пределы прежние:8192 R
allocations и20000 unique protected identities. У Contact нет собственной class
quota; он не вычитает R allocation и не меняет Selected accounting. Проверки
admission/revision предшествуют атомарной публикации. Недоступный accounting
отказывает; прежний overcapacity сохраняется и виден в диагностике. Eviction
нет. Повышение metadata существующей identity не занимает новое global place.

Strict confirmation использует существующий forced full-snapshot writer и
captured protection revisions. Обычная persistence не подтверждает metadata.
Unknown strict outcome оставляет учтённую неподтверждённую защиту; strict retry
может подтвердить текущую revision. Новое принятое record evidence инвалидирует
старый C3 preview, сохраняя ACK неизменившегося class fact; это не обещание новой
durable signed-record proof.

Настоящий TLS wrapper и неактивная record capability используют общие legacy
close/forget/drain/purge вне owner locks. При отмене owed purge переходит
последнему admitted legacy writer. Record capability проверяется по текущему
store entry до и после barrier; stale publication не восстанавливает снятый pin.
Общая диагностика говорит, что identity требует v2, не называя record-only защиту
результатом TLS-сессии.

R1a добавляет только fixed runtime fields и owner-maintained counters к прежним
entry/store и временные private witnesses. Второй identity map и completed-record
history нет. Это не результат memory/resource acceptance: full snapshots, прежний
overcapacity и pending lookup inventory без общего cap сохраняют свою стоимость.
Functional/race/mutant controls не означают готовность полного R1, T5, G2 или D1.

Будущая политика aggregate30/min и queue256 требует явного решения владельца о
rolling/burst/restart semantics, service order, стоимости свежего record для
существующего класса и переносе исходного task deadline. Отдельное рассмотренное
предложение здесь не реализовано. K2/R2 и unsolicited-record producers не
подключаются этой основой.
