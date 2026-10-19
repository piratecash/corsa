# Lookup provenance and exact completion ownership

## English

R0 records the runtime origin of one actual identity-lookup attempt and prevents
late work for resolution A from mutating a replacement B for the same identity.
It creates no Relation metadata or pin, and changes no class quota, C3 removal,
dial policy, priority, outgoing-message path or protocol capability.

The sole trusted producer in this slice is
`Window.maybeResolveIdentityKeys` → `DMRouter.ResolveUserIdentity(ctx, target)` →
typed `DesktopClient.StartUserIdentityLookup` → embedded node. The Window uses
its existing UI operation context and the router's operation lease; the node
admits an asynchronous intent, never adopts that request context as its lifetime.
Returning from the call, switching conversation or closing the Window does not
add a new cancellation rule for the existing seven-day ordinary intent.
Generic Frame/default `ui_chat`, LocalRPC/HTTP commands, recovery, Presence,
old format1 rows, restart and SDK sends carry no Window authority. Generic
lookup/send commands retain their compatibility behavior. No caller-supplied
origin flag is serialized or trusted.

A nonserialized stamp lives on the existing singleton UI reason row. Repeated
trusted starts preserve its generation. A first genuine Window action joining
generic work affects only future attempts. Explicit cancel/readd mints a new
generation; the opaque expected-generation handle cannot cancel replacement B.
A synchronous generic current-reason cancellation keeps its previous semantics;
delayed work never uses that target-only operation. Removing the Window reason
can revoke its provenance while recovery/Presence still keep normal lookup alive.

Every attempt retains its exact resolution owner, label, attempt generation,
question digest, sequence floor and immutable optional action stamp. Slot waits
are context-aware and precede registration. All rechecks after a wait, route gate,
class-queue return, record verification/import and expiry use the captured owner,
never the current target alone. The existing three send slots cover synchronous
enqueue only and are released before persistence. Attempt/action/snapshot
counters refuse exhaustion instead of wrapping. A fresh completion clock checks
the existing60-second window after expensive work; a backwards time observation
also cannot revalidate an attempt.

A verified answer may improve globally available keys even after A expired or
was removed. That normal contact-plane change can improve B's usable/provisional
view. It does not let A finish B, stamp B's answer generation, change B's schedule,
charge its budget, decrement its occupancy or remove its intents. A retained
terminal polling state (StateByID) is not overwritten by a delayed nonterminal
snapshot of the same ID. This does not serialize unlocked ebus delivery order.

Queue state is Pending / QueueAccepted / Refused. QueueAccepted means class-queue
admission: no socket emission, concrete accepting hop, delivery or durable ACK is
inferred. A fast verified answer completes normal key lookup immediately. Only
its already-claimed row can survive successful finish while its queue result is
pending; other attempts and ordinary reason rows are removed. The exact late
queue result settles that row once. Acceptance plus a live, nonrevoked Window
stamp and verified min_session>=2 returns one private soft completion value;
refusal, expiry, matching cancel or runtime shutdown discards it. A duplicate
cannot claim twice. No attempt is resurrected, and no replacement's reasons are
consulted to reconstruct old authority. Pending claims use the existing attempt
expiry schedule even after their resolution completed.

The production caller writes one debug diagnostic containing scalar origin,
resolution/attempt/action identifiers and QueueAccepted, then discards the value.
It logs no record payload or keys. There is no completed-history map, new queue,
worker, public completion protocol or pin consumer. R0 soft output cannot later
be replayed as R1 authority: R1 requires a fresh pin-owner observation before a
new attempt. The class queue has no downstream drop completion callback, so R0
makes no sent/hop assertion about accepted frames that subsequently expire/drop.

Reason membership, action generation, counters, finish and immutable durable
snapshot creation linearize under resolver.mu → intent.mu. Both are released
before disk, logging and event publication. This includes background attempt
accounting and terminal intent removal. The saved immutable snapshot, rather
than a delayed target mutation, moves to I/O. Existing saveMu/snapshotGen/savedGen
ordering rejects older saves after a newer generation reached disk. Runtime
stamps are absent from format1 snapshots/restarts. Persistence errors cannot
restore revoked runtime authority. Owning runtime cancellation/shutdown revokes
claims; durable ordinary intents may resume after restart without origin.

Fixed fields and at most one claim per existing attempt are O(existing lookup
work). The existing per-resolution5-open-attempt bound and60-second windows are
not a global lookup/intent count or memory cap. No new absolute cap is selected.
R1 record admission/persistence and R2 own-message recipient/actual-hop evidence
remain separate work; this document makes no D1 resource or activation claim.

## Русский

R0 фиксирует происхождение конкретной lookup-попытки и не позволяет запоздалой
работе A изменить новый resolution B той же identity. Relation metadata/pin не
создаются; квоты, C3, dial/priority, исходящие сообщения и capability не меняются.

Единственный trusted producer этого среза — настоящий Window → typed DMRouter →
DesktopClient → embedded node. Используются существующие UI context и router
operation lease. Context вызова разрешает admission, но не становится lifetime
асинхронного семидневного intent. Возврат вызова, переключение или закрытие окна
не вводят отмену этого intent. Generic Frame/default UIChat, LocalRPC/HTTP,
recovery, Presence, format1/restart и SDK send остаются non-R; compatibility
lookup/send работают как прежде. Строка/флаг caller не становятся authority.

Runtime stamp живёт только в существующей reason row и не сериализуется.
Повтор genuine start сохраняет generation; первый Window join generic-работы
влияет только на будущие попытки. Cancel/readd получает новую generation;
opaque expected-generation cancel A не отзывает B. Generic синхронный cancel
текущей reason сохраняет старую семантику, но delayed callbacks его не вызывают.
Отмена Window authority не мешает recovery/Presence продолжать обычный lookup.

Attempt хранит exact owner, label/gen, qHash, minSeq и immutable action stamp.
Ожидание send-slot прерывается context; после ожидания, route gate, enqueue,
verify/import и expiry проверяется тот же owner. Три slot освобождаются сразу
после синхронного enqueue, до диска. Generation не переполняется с повторным
использованием. После verify/import заново проверяется60-секундное окно;
обратный ход времени не оживляет старую попытку.

Проверенные ключи могут нормально импортироваться и после завершения A,
естественно улучшая usable/provisional B. При этом A не завершает B, не меняет
его answer generation, schedule/budget/openAttempts/intents. Поздний nonterminal
snapshot того же resolution ID не затирает сохранённый terminal в polling
StateByID. Порядок доставки ebus после unlock этим не сериализуется.

QueueAccepted означает только admission в class queue. Это не socket send,
подтверждённый hop, доставка или durable ACK. Быстрый ответ сразу завершает
обычный lookup. Только уже созданный claim в той же attempt row может ждать
поздний enqueue result после Success; остальные rows удаляются. Accepted и
живая неотозванная Window generation с проверенным min_session>=2 дают один
private soft value. Refusal, expiry, matching cancel и shutdown отбрасывают его;
duplicate не создаёт второй value. Новая reason B не используется для оживления
A. Expiry обслуживает claim и после удаления resolution.

Production observer пишет один debug scalar diagnostic и сразу выбрасывает
value, без payload/ключей. Нет growing history, новой очереди/worker, публичного
wire completion или pin consumer. R1 не может ретроспективно использовать R0:
нужен новый owner observation до новой попытки. Downstream-drop callback у
class queue нет; R0 не выдаёт QueueAccepted за доказательство реального send.

Все reason/action/counter/finish и копия snapshot изменяются под resolver.mu →
intent.mu; disk/log/ebus — после обоих locks. Это включает background attempt
и terminal removal. В I/O передаётся immutable snapshot, а не поздняя target
mutation. saveMu/snapshotGen/savedGen не дают старой записи затереть новую.
Runtime stamp отсутствует на диске/после restart; ошибка записи не возвращает
отозванную authority. Shutdown отзывает runtime claims, обычный durable intent
может продолжиться после restart без provenance.

Дополнительные fixed fields ограничены существующим lookup working set.
5 attempts на resolution и60 секунд не означают общий count/byte cap;
новый абсолютный предел не выбран. R1 admission/persistence, R2 own-message/hop,
ресурсная приёмка D1 и activation остаются отдельными незавершёнными этапами.
