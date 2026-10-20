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
and full-membership enumeration occur before entry, retaining only at most 512 source facts and
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
actual strict-named dispatch/roles/connection adapter; input limits; real-path composition of the
local source/pair histories; actual degree/window producer; current durable C5 provider; complete T integration;
writer lifecycle, timing/network evidence, G2 and the separate release sequence. No pass boolean
stands in for these gates. Standalone public state APIs keep their old limited semantics and cannot
access this core's private subowners. Closed call/reference and state-field guards preserve that
boundary. See [accepted option A](d1-bounded-intake-proposal.md),
[conditional deadlines](d1-admission-budget.md), and [state blocks](inactive-find-node-state.md).

### Atomic source-session / target-identity histories

The disconnected owner now composes source128 distinct targets/10min and pair3/30s in the same
commit as target calendar, group ledger and outgoing ID. Standalone `PairWindow` remains one
caller-owned gate; it is not mutated by this composition. One optional atomic companion in a
canonical Session contains 128 fixed identity rows, each with projected/actual three-event
histories, distinct expiry, checked generation and pending refs. All target links share the same
identity row; a genuinely new source Session has independent history. A permanent, explicitly
nonzero-sized owner token prevents independent coordinators spending the same Session history.
It has no coordinator backpointer. First companions are prepared off-lock and attached by CAS only
after every fallible joint trial. Offer/refused provisional work binds no owner and charges nothing.

The pair floor combines projected history with copied actual history pessimistically replaying
only existing unconsumed same-pair aliases at their maximum latest. It is an input to target slot
selection, never the debit timestamp. Projected pair charge is stamped at the FINAL joint `slot.at`;
source expiry is `max(old, slot.at+600s)`. Actual pair and target budgets are trialled together on Take
before either debit or alias consumption. At exactly30s an event leaves the sliding window. A
reachable synthetic trace at O=1s retains projected O+[0,.6,.7] and actual O+[.6,.7,.8]; its fourth
cut O+27.4 refuses original latest O+30.35 because actual earliest O+30.6 is too late. These explicit
fixture timings are not deployment parameters or network evidence.

One source-row ref follows each existing committed pending row. Normal Finish extends expiry by
`max(old, Finish+600s)` and keeps that ref while AwaitingAnswer. Answer/cancel/close/expiry during
Dispatching keeps TerminalDispatch/ref1 until sole Finish. Final release decrements once without
refunding histories; nominal request deadlines do not make an unresolved writer disappear. Row
reuse requires ref0, reached distinct expiry and BOTH actual/projected debt truly expired. Generation
never wraps, and exact Session/owner/identity/generation checks reject stale handles. Invalid time
cannot create a debit, renewal, reuse or ownership claim; existing closed-session reconciliation
remains terminal-only. No reset or rebind operation exists.

Retained overhead is one optional 128-row companion per participating canonical Session plus fixed
source refs in the same at-most 512 pending companions. No retained source/pair registry, completed
history map, worker or second queue is added. Cost is O(128×participating Sessions +512), not 7952:
7952 bounds only the group ledger. A seal may transiently prepare at most 512 first companions;
caller-held ended Sessions, snapshots and other transient allocations remain additional. Struct
sizes, heap/CPU/RSS, timing profiles and resource acceptance have not been measured by this slice.
The source-ID replay-lifetime gap and the real class/membership/degree/input/dispatch/T composition,
G2, capability activation and separate release gates all remain open.

### Inactive F1 proof-transport foundation

The private, unused FindNode factory composes a memory-only transport adapter. Ordinary constructors leave it nil and still advertise neither overlay capability. It installs no handler, admission, class provider, worker, signer or socket writer. Explicit-cap genuine TLS fixtures do not change production hello/welcome capabilities. A canonical `SessionSeed` captures actual local/remote negotiated facts immediately after Accept/Dial; copies share one consumption latch. Invalid Bind arguments do not consume it. The seed and Session retain identity proof values, not a full TLS ProvenSession or connection. End closes the one canonical Session without refund or I/O.

One `proofTransportLifetime.mu` orders original terminal, accepted/CM publication and the independent N-in companion. Raw R and decoded W, the registry and the outbound peer alias the SAME cell. An atomic provisional terminal latch plus bind-once CAS covers Read/Write/Close before the cell is attached; it cannot mint proof. End clears proof/socket/entry/peer/store chains before existing blocking diagnostics. Accepted and dialled datagrams capture an original terminal-only arrival capsule before the synchronous pipeline; outbound initial push captures its original owner before background scheduling. No current-ID/address lookup substitutes another transport.

Accepted publication follows actual auth. Outbound publication follows the CM generation-accepted callback, successful setup/registration and the legacy-pin recheck, immediately before SessionInitReady. It samples independent monotonic elapsed time AFTER owner-lock waits. The coverage is only **registered accepted and CM post-setup readers**; it excludes one-shot/unregistered readers and does not claim complete overlay admission or already-consumed CM Active. CM deactivation/stale-success refusal Ends the selected original session before later teardown diagnostics.

N-in remains listener-only, with the same original mandatory pin/incarnation, ten-minute continuous lifetime, source-group journal/debit, strict ACK and owed-work semantics. A dialler has no N-in companion. An N-in clock failure cannot mint its class or suppress a separately valid F1 publication. No limits, activation, resource admission or release gates change. One cell/optional companion per original transport and an on-demand O(current registered readers) snapshot are structural costs; TLS evidence before publication, snapshot allocations, external delayed handles and full process/heap costs remain additional.

F1/N-in transport fixtures set the existing synchronous-withdrawal test seam before starting their workers. These checks exclude production route-grace timing and its default resource cost; dedicated routing tests and production defaults remain unchanged.

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
roles/connection adapter, input limits, композиция локальных source/pair histories на реальных путях, degree/window
producer, current durable C5 provider, T/writer integration, timing/network evidence, G2 и отдельный
выпуск. Pass boolean их не заменяет. Public standalone API сохраняет прежние ограниченные semantics
и не получает private subowners core. Closed-call/reference и field-inventory guards удерживают
границу. См. [принятый A](d1-bounded-intake-proposal.md),
[условные сроки](d1-admission-budget.md), [блоки состояния](inactive-find-node-state.md).

### Атомарная история source Session / identity цели

Неактивный owner объединяет source128 различных целей/10min и pair3/30s с calendar, ledger и
outgoing ID в одном commit. Standalone `PairWindow` сохраняет прежнюю отдельную семантику. Один
optional atomic companion canonical Session хранит128 identity rows: по три projected/actual
события, distinct expiry, checked generation и pending refs. Links одной цели разделяют row;
новая настоящая source Session начинает независимо. Постоянный nonzero-sized owner token
запрещает двум coordinators независимо тратить её историю и не удерживает coordinator. Первый
companion готовится вне locks и присоединяется CAS после всех fallible trials. Offer и отказанная
provisional не закрепляют owner и не списывают бюджет.

Pair floor объединяет projected history с копией actual и консервативным replay только прежних
unconsumed aliases этой пары в максимальном latest. Это граница выбора, а не время списания.
Projected event получает FINAL joint `slot.at`, expiry — `max(old, slot.at+600s)`. Take сначала
совместно проверяет actual pair/target и лишь затем списывает оба и снимает alias. Ровно 30s выводит
событие из окна. Достижимая synthetic последовательность с O=1s даёт projected O+[0,.6,.7] и actual
O+[.6,.7,.8]; четвёртый cut O+27.4 отказывает latest O+30.35 из-за actual earliest O+30.6. Эти входы
фикстуры не являются deployment profile или сетевым доказательством.

Один source ref принадлежит существующей committed pending row. Обычный Finish продлевает expiry
до `max(old, Finish+600s)` и сохраняет ref в AwaitingAnswer. Answer/cancel/close/expiry при Dispatching
оставляют TerminalDispatch/ref1 до sole Finish. Final release снимает ref ровно один раз без
возврата истории; номинальный deadline не устраняет зависший writer. Reuse требует ref0, истечения
distinct и ОБЕИХ projected/actual histories. Generation не переполняется; точные Session/owner/
identity/generation защищают от stale handles. Неверное время не даёт debit/renewal/reuse/claim;
прежняя terminal reconciliation закрытых сессий сохраняется. Reset/rebind API нет.

Retained cost — optional 128-row companion участвующей canonical Session и fixed refs в прежних
≤512 pending companions. Дополнительных retained source registry, completed map, worker/queue нет.
Оценка O(128×участвующие Sessions +512), а7952 ограничивает только group ledger. На seal временно
готовится максимум 512 первых companions; caller-held ended Sessions, snapshots и transient
allocations учитываются дополнительно. Struct sizes, heap/CPU/RSS, timing/resource acceptance не
измерены. Срок source-ID replay и реальная class/membership/degree/input/dispatch/T композиция,
G2, activation и отдельные release gates остаются открытыми.

### Неактивная proof-transport основа F1

Private неиспользуемая FindNode factory собирает memory-only transport adapter. Обычные конструкторы оставляют его nil и по-прежнему не объявляют обе overlay capability. Handler, admission, class provider, worker, signer и socket writer не устанавливаются. Genuine TLS фикстуры с явно согласованными capability не меняют production hello/welcome. Canonical `SessionSeed` сразу после Accept/Dial захватывает настоящие согласованные факты; копии разделяют одноразовый latch. Неверные Bind arguments его не расходуют. Seed и Session удерживают identity proof values, а не полный TLS ProvenSession/connection. End закрывает единственный canonical Session без refund и I/O.

Один `proofTransportLifetime.mu` упорядочивает исходный terminal, accepted/CM publication и независимый N-in companion. Raw R, decoded W, registry и outbound peer ссылаются на ТОТ ЖЕ cell. Atomic provisional terminal latch и bind-once CAS покрывают Read/Write/Close до attach и не создают proof. End очищает proof/socket/entry/peer/store chains ДО прежних блокирующих diagnostics. Accepted и dialled datagram захватывают исходную terminal-only arrival capsule до синхронного pipeline; outbound initial push захватывает исходного owner до background scheduling. Поиск текущего ID/address не подставляет другой transport.

Accepted publication следует за настоящим auth. Outbound publication идёт после CM generation-accepted callback, успешных setup/registration и legacy-pin recheck, непосредственно перед SessionInitReady. Независимый monotonic elapsed читается ПОСЛЕ ожиданий owner locks. Coverage — только **registered accepted и CM post-setup readers**: one-shot/unregistered readers исключены; это не полная overlay admission и не утверждение уже обработанного CM Active. CM deactivation/stale-success refusal завершает выбранную исходную session до последующих teardown diagnostics.

N-in остаётся listener-only с прежними исходными mandatory pin/incarnation, десятью минутами непрерывного lifetime, source-group journal/debit, strict ACK и owed-work semantics. Dialler не получает N-in companion. Отказ N-in clock не даёт классификацию и не запрещает независимо допустимую F1 publication. Лимиты, activation, resource admission и release gates не меняются. Один cell/optional companion на исходный transport и on-demand O(current registered readers) snapshot — структурные издержки; допубликационные TLS evidence, snapshot allocations, внешние delayed handles и полная стоимость heap/process учитываются дополнительно.

Транспортные фикстуры F1/N-in включают существующий тестовый режим синхронного withdrawal до запуска работников. Эти проверки не покрывают production route-grace timing и его стоимость ресурсов по умолчанию; отдельные routing tests и production defaults остаются неизменными.


### Private bounded C5 observation / Private C5-наблюдение

The unused private observer uses explicit copied MaxMembers/MaxRegistryVisits inputs, complete-or-refused capture, genuine current F1 owner rechecks and current strict classes. Target links count toward M and are excluded from requester oldest-four selection. This historical observation is not AdmittedAtR/T, full online composition or a C3-immune permit. See [bounded class reader](find-node-priority-adapter.md). Deployment profiles, resources, G2 and activation remain separate.

Private observer читает настоящие F1 members и strict C5 через явные M/V; переполнение отказывает целиком. Это наблюдение, без допуска R/T, гарантии доступности или жёсткой задержки mutex/CPU.
