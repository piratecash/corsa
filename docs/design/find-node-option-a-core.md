# Inactive FindNode option-A core

## English

The private A-core in `internal/core/findnode` implements the owner's accepted bounded-intake
option A as a local state machine. It has no production caller, worker, socket writer, signer,
RPC or capability advertisement. Its `candidateReserved` outcome is **not full AdmittedAtR or
AdmittedAtT**. Explicit timing inputs are proposals/test inputs, with no deployment defaults.

One PendingTable owns provisional, reserved, dispatching, awaiting-answer and terminal-awaiting-
dispatch rows. All phases share 16/source Session, 16/target identity across links and 512/node.
There is at most one retained provisional/source/node round. A fixed cut does not extend on new
input or flush early; only a worse provisional can be replaced. Already committed work is retained.
L1 uses a successful-commit cursor and membership watermark; L2 selects four oldest eligible live
sessions per target, including connections with no pending/cache row. Remaining sessions rotate
by accounting cell and Session. Priority belongs to the Session, never its group.

The group registry has 256 sticky named cells and one overflow cell. Physical overflow group names
are not retained. The rolling ledger charges `(cell,target)` only with a committed reservation;
refused trials allocate neither name, record, outgoing ID nor calendar debt. Counts use live ledger
cells, not all sticky names. Share is `max(16,floor(C_node/k))`; hard H is `C_node+16*257<=7952`.
Old records survive share decreases. The first 256 groups can retain their advantage indefinitely;
overflow groups share one floor, and hard H can refuse below-share work, including late L1/L2.
There is no unconditional discovery or protected physical-overflow minimum.

Each identity has one canonical calendar, at most 512 calendars. All links and priority levels
share its real 1/s burst-4 budget and old projected debt. A new reservation trial copies the actual
bucket, pessimistically replays every unconsumed alias at `max(now,all old latest)`, then computes
one candidate's actual floor. The projected bucket reserves at or after that floor. Virtual actual
charges are never published. This is conservative, not maximal admission. It can accept fewer
requests than the projected-only Python witness. A late actual-consumption trace with projected
next1.1s but actual next1.6s refuses a candidate whose latest is1.35s before any shared effects.

Commit locks are `coordinator.mu → pending.mu → one calendar.mu → target Session.state.mu`.
The final outgoing-ID peek and advance share one uninterrupted Session lock. All checks precede
infallible shared assignments. No callback, crypto or I/O executes under those locks. Classification
and full-membership enumeration occur before entry, retaining only at most512 source facts and
four witnesses per represented target. The trusted classifier's honesty and actual connection
binding are external requirements; a capture constructor does not prove them.

Take additionally debits actual-time tokens. Before the reserved slot it returns NotReady. Missing
actual capacity inside a promised window is an assumption/invariant violation, not a retry that
repairs an impossible admission. Out-of-window Take grants no send permit. Cancel, close, failed
write and expiry never refund projected or actual credits. A dispatching row remains physically
charged until its sole Finish, even after answer/cancel/timeout; Finish extends accounting through
completion+600s. Completion beyond latest is a conservative diagnostic overrun; completion
is an upper bound on writer start, not proof of its timestamp. A stuck external writer may therefore retain a bounded slot. No bounded-I/O or
network guarantee is inferred. Repeat Finish and stale/cross-owner handles refuse.

Calendar retirement requires zero row/alias/dispatch references and both buckets honestly full
without future debt; link close alone cannot reset it. Group reset needs a fully idle rolling
window, expired records and zero rows. This reset check is complete only in this disconnected core,
where no TargetCache can own a cell reference. Future shared cache integration must add actual
row-owned reference lifetimes before allowing reset. No second epoch/history or caller-supplied
`allRefsGone` assertion is used.

The pending entry gains one optional-companion pointer, including standalone entries; a composed
row additionally owns that companion. Counts are structural bounds, not measured bytes, CPU, RSS
or resource acceptance. Full connection
enumeration and caller-held inputs/handles are outside the retained-owner bound. Remaining work:
actual strict-named dispatch/roles/connection adapter; input limits; atomic pair3/30s and source128/
10min histories; actual degree/window producer; current durable C5 provider; complete T integration;
writer lifecycle, timing/network evidence, G2 and the separate release sequence. No pass boolean
stands in for these gates. Standalone public state APIs keep their old limited semantics and cannot
access this core's private subowners. Closed call/reference and state-field guards preserve that
boundary. See [accepted option A](d1-bounded-intake-proposal.md),
[conditional deadlines](d1-admission-budget.md), and [state blocks](inactive-find-node-state.md).

## Русский

Private A-core в `internal/core/findnode` реализует принятый вариант A как локальный автомат
состояния. Production caller, worker, socket writer, signer, RPC и объявления capability нет.
`candidateReserved` **не означает полный AdmittedAtR или AdmittedAtT**. Численные timing inputs —
проект/вход теста без deployment defaults.

Один PendingTable хранит provisional/reserved/dispatching/awaiting-answer/terminal-awaiting-dispatch.
Все фазы вместе занимают 16/source Session, 16/identity цели по всем её links и 512/node. В общем
раунде узла от source одновременно удерживается максимум одна provisional. Cut не продлевается
новыми offers и не закрывается раньше; заменяется только худшая provisional. Принятое не вытесняется.
L1 использует cursor успешных commits и membership watermark. Четыре L2 выбираются для каждой
цели из всех подходящих живых сессий, включая не имеющие pending/cache row. Остальные идут RR по
accounting cell и Session. Приоритет сессии не передаётся группе.

Реестр содержит 256 sticky named cells и одну overflow, без истории физических overflow-групп.
Ledger `(cell,target)` списывается только вместе с commit; отказ не выделяет name, record, outgoing
ID или calendar debt. k берётся из живых записей, не из всех sticky names. Share равна
`max(16,floor(C_node/k))`; hard H=`C_node+16*257<=7952`. Падение share не удаляет прежние записи.
Первые256 могут удерживать преимущество бесконечно; overflow делит один floor, H может отказать
ниже share, включая поздние L1/L2. Безусловного нахождения и независимого physical-group floor нет.

Для identity есть один канонический calendar; всего их не больше512. Все links/уровни делят
1/с burst4 и прежний projected debt. Trial копирует actual bucket, консервативно воспроизводит
все unconsumed aliases в `max(now,all old latest)` и получает actual floor нового запроса.
Projected bucket резервирует не раньше этой границы. Виртуальный actual расход не публикуется.
Алгоритм не максимальный и может принять меньше Python projected-only witness. Контрпример с
projected next1.1s и actual next1.6s отказывает candidate с latest1.35s до shared effects.

Commit держит `coordinator.mu → pending.mu → один calendar.mu → target Session.state.mu`.
ID peek/advance выполняются без разрыва Session lock; все проверки предшествуют безошибочным
shared assignments. Callback/crypto/I/O под locks нет. Classification/full-membership enumeration
идут заранее; сохраняются не больше512 source facts и четыре witness на представленную цель.
Честность trusted classifier и actual connection binding остаются внешними требованиями:
конструктор capture их не доказывает.

Take дополнительно списывает actual-time token. До reserved slot — NotReady; нехватка actual token
внутри обещанного окна — assumption/invariant violation, а не retry, исправляющий неверный admission.
Вне окна send permit не выдаётся. Cancel/close/failed write/expiry не возвращают credits. Пока sole
Finish не завершил dispatch, строка остаётся физически занята даже после answer/cancel/timeout;
Finish продлевает ledger до completion+600s. Completion позже latest даёт консервативную
диагностику overrun: это верхняя граница writer start, а не доказанный timestamp. Зависший внешний writer может удерживать bounded slot;
гарантия bounded I/O или сети не заявляется. Повторный Finish и stale/cross-owner handles отказывают.

Calendar удаляется только без row/alias/dispatch refs и при обоих честно полных buckets без future
debt. Close link не reset. Group reset требует idle rolling window, expired records и ноль rows.
Проверка полна только в этом отключённом core без TargetCache cell refs. Перед общей cache-
композицией нужны реальные row-owned lifetimes; caller bool `allRefsGone`, второй epoch/history
не заменяют их.

Pending entry получает один optional-companion pointer даже в standalone mode; composed row
дополнительно владеет companion. Это структурные пределы, не измерения bytes/CPU/RSS и не resource acceptance. Полный обход
connections и caller-held inputs/handles вне retained-owner bound. Ещё нужны actual strict-named
roles/connection adapter, input limits, атомарные pair3/30s/source128/10min histories, degree/window
producer, current durable C5 provider, T/writer integration, timing/network evidence, G2 и отдельный
выпуск. Pass boolean их не заменяет. Public standalone API сохраняет прежние ограниченные semantics
и не получает private subowners core. Closed-call/reference и field-inventory guards удерживают
границу. См. [принятый A](d1-bounded-intake-proposal.md),
[условные сроки](d1-admission-budget.md), [блоки состояния](inactive-find-node-state.md).
