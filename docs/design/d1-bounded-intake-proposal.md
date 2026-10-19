# D1 bounded intake and group overflow — accepted option A

## English

### Status and corrected resource boundary

**Owner decision, 2026-10-19: option A approved for inactive implementation and light checks.**
The owner accepted 256 named groups plus one overflow cell, H≤7952, one provisional request per
session per node round, fair admission order and atomic shared debit only at admission. Refused
provisional requests do not spend shared quota; admitted obligations survive cancellation and
disconnect without refund. This changes no Go, runtime, wire contract, pins or capability by itself.
Numerical timing profiles, full real-path composition, resource acceptance, G2, activation, S6 and
D1 release remain separate. Read with
[aggregate admission](d1-admission-budget.md), [inactive blocks](inactive-find-node-state.md) and the
public FIND_NODE contract. The companion documents are in the main checkout.

R1 incorrectly treated 256 as the total including overflow. The actual draft 18a §8.6 and §8.1
explicitly allow **256 named groups plus ONE overflow cell**, 257 total, with
`H = C_node + 16 * 257 <= 7952`. R2 preserves this original bound; it requests **no resource increase**.
The real contradiction is that every group with a level-1/2 session should always be individually
included, although 257 aged sessions from distinct groups already exceed 256 named slots. Four
level-2 priority positions per target do not bound that population.

| Option | Registry and record bound | Disposition |
| --- | --- | --- |
| **A: preserve the original bound; detailed candidate below** | 256 named + one OVERFLOW; 257 total; H ≤ 7952 | Approved on 2026-10-19, including shared overflow, loss of an independent physical-group floor, sticky first-arrival bias and the priority/privacy limitations. |
| B: optional tightening, not required by the draft | 255 named + one OVERFLOW; 256 total; H ≤ 7936 | This deliberately reduces the existing bound and offers no cure for unlimited privileged groups. R2 does not recommend silently choosing it. |
| C: refuse unrepresented groups | No shared overflow admission | Changes the existing overflow treatment and can exclude a later legitimate level-1 session entirely. |

The owner selected A; B and C remain unselected alternatives. Initial named groups may retain their
advantage under continuous load; physical overflow groups have no independent guaranteed minimum.
Hard H may refuse a group below its current share. This is neither guaranteed L1/L2 availability
nor complete protection against budget monopolization. Acceptance must exercise sustained load from
the first 256 groups and late L1/L2 competition in overflow. Timing parameters and complete policy
composition still need evidence before G2; the synthetic six-second example below is not an
approved numerical timing profile.

### 1. Authoritative inputs and bounded ownership

A trusted future adapter supplies actual `ProvenSession`, nonreused monotonic local ConnID,
reached-address `SourceGroup`, authoritative K/N-sel/N-in classification and live-session age.
Address binding, a pin or a group claim does not establish intent, class or service priority.
Level 1 is K/N-sel. Four oldest eligible live sessions have level-2 priority **per target**; the
rest are new. Select those four from the connection owner, not only existing cache holders. No
unbounded priority map is proposed, and CPU cost of scanning unlimited connections is not proved.

There is one open node-wide R round. Provisional, reserved/queued and forwarded-awaiting-answer
requests occupy the **same** pending rows: **16 per target identity across links, 16 per source
session, 512 per node**. A row includes source Session, target identity, actual link when selected,
request/challenge, original receipt/deadline, round and generation reference. `AdmittedAtR` converts
that row in place. No second staging queue or attacker-keyed backlog map exists.

At most one provisional row is retained per source Session **per node round**, including offers to
different targets. Extra offers are immediately refused with no deferred copy. The owner approved
this stricter intake rule; it does not reduce the existing 16 admitted-pending allowance.
A replaced row was provisional and promised no service. Replacement cannot evict any admission.

Canonical target-identity calendar/cursor metadata is proposed to have at most 512 entries, without
an extra job queue. Retire an entry only with zero references and fully restored token state with
no projected debt. If none can retire, a new target refuses; TCP close cannot reset its budget.
The canonical registry, its exact bytes and CPU cost remain implementation/measurement work.
Candidate sorting uses at most 512 rows; round/cursor/watermark are scalars. Group metadata uses
only 257 cells and at most 7952 accounting records. Target dedup remains **256/node, eight references
per session**, with the existing three-link history and 60-second lifetime.

### 2. Fixed round, provisional replacement and fair order

`DeltaR > 0`, dispatch slack, `deltaRuntime`, `deltaTarget` and `deltaNetwork` are explicit proposed
parameters requiring evidence and owner approval before G2. The Python input DeltaR = 0.1s is a
synthetic witness value. First eligible input fixes `cut = first_received + DeltaR`. Later offers
do not extend it. Full intake does not flush early. Arrivals at/after cut belong to another round;
no unseen arrivals have a service promise.

Count `Offered`, `Provisional`, `ReplacedBeforeAdmission`, `RefusedBeforeAdmission`, `AdmittedAtR`,
`AdmittedAtT` and `Answered` separately. Provisional retention promises no forwarding, answer or
non-eviction. D1 has no admission acknowledgement on the wire; silence cannot imply admission.

Intake applies strict codec, proven role, input rate and per-source eligibility gates, then the
one-provisional rule and physical caps. **It does not debit the shared group-target ledger.**
It retains the best provisional rows by service level and cyclic ordering. Replace only a worse
provisional; at a full target the victim must be for that identity, at a full node any eligible
provisional can be a victim. Source space cannot be borrowed. Equal rank cannot churn incumbents.
Input/source-local gates may charge refused attempts to that source; they must not charge another
source's shared group budget. A source reaching its own 128-distinct-target cap is not excused.

L1 order uses actual stable ConnID after a persistent cursor, wrapping once within a frozen
membership watermark. IDs above that watermark are refused before admission until a later sweep.
Advance the watermark to the connection owner's current maximum only after a **successful admitted
wrap**; an empty sweep may renew it after its fixed collection interval. A stable cursor without
this barrier can starve old IDs if fresh higher IDs arrive endlessly. Reconnect gets a new actual
ID and joins as a newcomer; claimed identity cannot choose its ordering.

At cut, visit L1 rows in that order, then the four eligible L2 by age, then new sessions round-robin
by accounting cell and Session. OVERFLOW is one cell for this last step. Session priority is separate:
a new peer sharing a cell with K does not inherit K priority; a K Session in OVERFLOW remains L1.
Advance the cursor only on successful admission, never every inspected offer. With ten sources
and seven places, advancing past all ten repeatedly rejects the same last three.

For each retained row, under one state owner, perform this **trial → atomic commit**:

1. Revalidate source/link, cancellation, deadline, input/pair/source-local gates and physical row
   ownership. Previously passed mutable gates must not silently expire during collection.
2. Simulate the canonical target calendar, including actual tokens already spent and all old
   reservations, including lower levels. Compute a legal slot satisfying the original deadline.
3. Look up the prospective group cell without allocating a name/record. Trial its current rolling
   distinct-target share and hard H bound. A failure publishes neither a new record nor calendar debt.
4. Only if all trials succeed, atomically commit group debit/renewal, any source/pair reservation,
   the calendar reservation and conversion of the existing row to `AdmittedAtR`.

No crypto or socket write occurs under this owner. A rejected pre-admission trial creates no shared
charge; it is counted as refusal, not refunded work. Post-admission cancellation, failed write or
close **retains committed charges and projected tokens**. A later send overrun is observable and
extends relevant accounting to cover actual consumption; it cannot create an uncharged retry.
There is no refund-on-cancel mechanism. All lower-level commitments present before this algorithm
remain charged; fair new intake cannot retroactively recover someone else's spent budget.

For original receipt `a`, retain:

```text
deadline = a + 6s
latest_forward = deadline - deltaTarget - deltaNetwork - deltaRuntime
DeltaR + dispatch_slack <= deltaRuntime
```

Reserve only `slot <= latest_forward`. At actual dispatch enforce the same 1/s, burst-4 bucket;
coincident wakeups cannot bunch projected tokens. A timer beyond the approved slack refuses the
unadmitted round and records an assumption violation, without extending deadlines or revoking L1
promises. The witness uses zero dispatch slack and conservatively appends after prior reservations.

### 3. What is charged, and what fairness is claimed

Draft 18a §8.1 expressly calls the pair limit **forwardings** (three/30s), and lists distinct targets
per source (128/10min) and per node/group. It does **not explicitly define** whether the group ledger
is charged on every syntactically valid offer, provisional retention, admission or socket dispatch.
R1's claim that conservative all-attempt charging was necessary therefore lacked a normative basis.
The approved option charges **committed forwarding reservations**. This is conservative relative to actual
successful sends because an admitted-but-unsent failure still consumes the reservation. The owner
explicitly approved this charging point; it does not claim to implement every source/pair gate.

Do not precharge group attempts before one-provisional or fair ordering. Counterexample: after named
slots fill, one L1 source offers 16 distinct targets; it retains one row, yet precharging all offers
exhausts the common 16-target floor and excludes competing L1s before RR sees them. Running RR only
on survivors of that gate is unfair admission. R2 allows that source at most **one fresh shared debit
in that round**, in the same fair order as concurrent L1s. Refused extra offers create no persistent
group-target records. Existing per-session input rates and distinct-target restrictions remain.

The current owner override at the head of draft 18a withdraws the old unconditional deadline.
Historical timing arithmetic and the forwarding bottleneck were **per (R,T)**; the owner's public decision
requires aggregate admission fair between L1 Sessions. First-turn-before-second for a finite
equal-demand cohort is the discipline proposed here, not a separate owner-approved algorithm. The
owner does not explicitly prescribe node-global max-min latency across heterogeneous targets or an independently
protected quota for every physical source group. R2's single node-wide one-candidate round is an
explicit conservative proposal to prevent a source capturing shared cross-target group quota.
It was explicitly approved with option A, rather than inferred from per-target RR.

For a continuously submitting finite L1 cohort contending for the same compatible scarce slots,
rotation gives one turn before another. For homogeneous requests with K successful admissions per
round, counts differ by at most one. The **same order applies to new shared group debits**, instead
of filtering by first-arrival group charges first. Thus where several L1s share a common overflow
floor and have equal marginal gate/resource conditions, a fast producer cannot buy extra turns
by oversubmission. Tests exercise both a shared target calendar and scarce cross-target overflow
space. Their conditions include existing committed occupancy, not an earlier unfair charging gate.

This is not a complete max-min proof for arbitrary overlapping calendars/groups, new-versus-already-
accounted targets, changing cohorts or full previously admitted physical storage. A Session with an
old already-accounted target may need no new distinct debit while another needs one: that is unequal
resource demand, not evidence of equal new-target allocation. Counterexamples and refusals must be
reported per gate; service order alone proves neither discoverability nor an unconditional deadline.
No independent floor for an unseen physical group inside OVERFLOW is promised. Historical lower-
level/shared charges can leave no room until expiry. The design cannot turn them into fresh credit.

### 4. One bounded sticky registry and rolling distinct ledger

Option A has **256 named slots plus one OVERFLOW, 257 total**. No map/set/list remembers physical
overflow group names. On a successful group-accounted admission the first 256 names acquire named
slots; later names use OVERFLOW. Trial/refused offers allocate no slot. No LRU, privileged displacement,
promotion from overflow to a newly freed name, or simultaneously allocated second epoch is allowed.
Under continuous load initial named groups may keep an advantage forever: explicit first-arrival
bias, not a claimed fair registry rotation. The owner explicitly accepted this limitation.

Window W = 600s is rolling, not a fixed-epoch reset. Each `(cell,target)` committed record retains
expiry at least `reserved_forward_time + W`; repetitions take the maximum expiry. If dispatch is
late, account through `actual_forward_time + W` too. Failed committed work remains charged. Closing
TLS never clears a record. Reset the **same** registry only after W of no committed group use, all
rolling records expired and **all** old provisional/pending/cache references gone. Increment a
nonwrapping generation. Old CellRef is not authority: rederive the actual group under current mapping
and charge/reuse the live record; stale tags cannot request fresh credit. No two epochs or retained
unbounded overflow-name history may appear during reset.

The concrete candidate uses `share = max(16, floor(C_node / k))`, where k is the number of active
accounting cells including a newly charged candidate cell, **at most 257 including overflow**.
Counting overflow in k and conservative integer rounding are explicit proposed completions of the
draft, not hidden interpretations. `C_node` stays within the existing 512..3840 range and degree/
window rules. The reference receives that value, rather than implementing degree history.

A new distinct record must pass both cell share and **hard H = C_node + 16*257 ≤ 7952**. A repeat
renews an existing record without increasing distinct count. A share decrease never evicts old
records or refunds credit. The hard bound wins even if a below-current-share request would otherwise
fit: changing k and overlapping rolling history must not violate memory limits. That explicitly
qualifies the draft's unconditional “below share always admitted” statement. Source-specific
128/10min accounting is separate and remains required. Accounting counts aggregate `(cell,target)`
commitments conservatively; record payloads alone do not establish total process memory bytes.

257 aged groups cannot all be named: the 257th enters OVERFLOW; the 258th shares it. Session priority
survives, but each physical overflow group loses a protected independent floor. An earlier producer
or lower level may already have spent the common budget, and a sufficiently large concurrent
population still shares only one floor. The owner accepted this narrowing of the old absolute
“L1 is unaffected” claim. Hard finite registry, an independent floor for every physical
group and arbitrarily many admitted groups cannot all hold together. No extra L1 reserve is hidden
inside 7952. Priority never inherits by group; the separate privacy/enumeration tradeoff is that
admitted-but-failed work reduces capacity, while rejected extra offers reveal no additional targets
through forwarding. Cross-R enumeration still needs the original rate/target limits.

### 5. T boundary and executable evidence

T collection would use the same explicit outcomes in existing 256 Reserved/Ready rows, eight refs
per Session, three-link/60s dedup history, and no second queue. A Ready copy requires no fresh sign.
A new sign needs the single node signer calendar and its incoming Session's 1/s burst-4 bucket.
`DeltaT + target_service_slack <= deltaTarget` and proposed node spacing at least 250ms require
approval/evidence. Other R and direct callers compete. R admission cannot reserve T: a T refusal
after R admission falsifies the target-admission assumption. No new wire acknowledgement is proposed.

[d1_bounded_intake_reference.py](d1_bounded_intake_reference.py) runs 17 exact-rational synthetic tests:
physical 16/16/512; fixed cut and exact deadline; no preemption of admitted rows; lower-level spent
and reserved tokens; provisional priority replacement and full-L1 intake; cursor/watermark churn; 257/258 aged groups;
sticky mapping/reset/stale generation; 16 distinct oversubmissions producing only one shared debit;
an executable counterexample to old precharging; scarce overflow turns across different targets; refusal publishing neither calendar nor record;
reserved-time accounting and no cancellation refund; hard-bound guards. One control constructs an
internal snapshot; another reaches 7,952 records through ordinary model commits at 32/s, with 3,840
distinct targets shared across four cells. This establishes reachability in the partial ledger model,
not compliance of that traffic with the missing full runtime gates.

This is an executable **partial witness**, not runtime admission, a full online-policy proof or G2.
Missing: lower-level RR and authoritative live membership/C5, full source/pair/degree gate composition,
canonical bounded calendar registry, T integration, concurrency transactions, actual dispatch, wire
handling, clock/network/Tor measurements and complete memory/CPU evidence. No test value approves a
Delta/profile. Before G2, validate selected A with its accepted limitations, finish composition, and test maximum legal
bursts, full admitted/provisional occupancy, 257+ groups, hostile overflow, changing membership,
close/cancel, aliases, stale generations and scheduling overrun on the actual implementation.

## Русский

### Статус и исправленная граница

**Решение владельца от 2026-10-19: вариант A утверждён для неактивной реализации и лёгких проверок.**
Приняты 256 именованных групп и одна overflow-ячейка, H≤7952, один provisional от сессии на общий
раунд узла, справедливый порядок допуска и атомарное списание общей квоты только при admission.
Отказанные provisional общую квоту не расходуют; после допуска отмена и разрыв не возвращают расход
и не снимают прежние обязательства. Сам документ не меняет Go/runtime/wire/pin/capability.
Численные временные профили, полная композиция на реальных путях, ресурсная приёмка, G2, активация
и выпуск S6/D1 остаются отдельными.

В R1 была ошибка: реальный 18a §8.6/§8.1 уже разрешает **256 именованных групп + одно переполнение**,
всего257, `H=C_node+16*257<=7952`. R2 сохраняет эту границу и **не просит расширения ресурсов**.
Противоречие другое: одновременно нельзя иметь максимум256 именованных и всегда отдельно учитывать
все группы L1/L2. 257 возрастных сессий из разных групп уже это нарушают; четыре приоритетных L2
для одной цели число таких групп не ограничивают.

| Вариант | Граница | Решение |
| --- | --- | --- |
| **A: сохранить исходную; подробно описана здесь** | 256 named + OVERFLOW, всего257; H≤7952 | Утверждён 2026-10-19 с потерей независимого минимума физических overflow-групп, first-arrival bias и оговорками приоритета/privacy. |
| B: необязательное ужесточение | 255 named + OVERFLOW, всего256; H≤7936 | Это сокращение прежнего лимита, не требование исходного документа. Молча его не выбираем. |
| C: отказывать неучтённым группам | Без общего overflow-допуска | Меняет прежнее правило и может полностью отрезать позднюю честную L1. |

Выбран A; B/C остаются непринятыми альтернативами. Первые именованные группы при непрерывной
нагрузке могут сохранять преимущество; физическим overflow-группам отдельный минимум не обещан.
Hard H может отказать даже ниже текущей доли. Это не гарантированная доступность L1/L2 и не полная
защита от монополизации бюджета. В приёмку входят длительная нагрузка первых 256 групп и
конкуренция поздних L1/L2 в overflow. Численные профили и полная композиция ещё требуют evidence;
пример с шестью секундами ниже — синтетический, не утверждённый временной профиль.

### Объекты, вместимость и раунд

Будущий доверенный adapter предоставляет настоящую ProvenSession, непереиспользуемый монотонный
ConnID, SourceGroup достигнутого адреса, авторитетные K/N-sel/N-in и возраст живой сессии. Address
binding/pin/группа сами по себе не доказывают intent, класс или приоритет. L1 — K/N-sel; четыре
старейшие подходящие L2 выбираются **на цель** владельцем всех живых соединений, не по cache holders.
Неограниченной priority map нет, но CPU обхода неограниченных соединений пока не доказан.

У R один node-wide раунд. Provisional + reserved/queued + forwarded-awaiting-answer используют
**те же строки**: 16 на identity цели суммарно по link, 16 на source Session, 512 на узел. Admission
меняет строку на месте. Дополнительной staging queue/скрытого backlog нет. От одной Session остаётся
не больше одного provisional **на весь node-round**, даже для разных целей. Остальные получают
отказ; admitted остаются в прежнем лимите16. Этот более строгий intake утверждён владельцем.
Provisional ничего не обещает и может быть вытеснен; admitted никогда не вытесняется.

Предлагается максимум512 метаданных канонических календарей целей. Удалять только без refs, после
полного восстановления токенов и исчезновения projected debt; нет места — новая цель получает
отказ. Закрытие TCP не сбрасывает бюджет. Реестр, байты и CPU ещё нужно реализовать/измерить.
Group metadata —257 cells и≤7952 records; target dedup остаётся256/node,8 refs/session,3 link/60с.

Первый допустимый input фиксирует `cut=first_received+DeltaR`; новые offers его не продлевают,
full queue не закрывает раньше срока. На/после cut запрос относится к другому раунду.
Offered/Provisional/ReplacedBeforeAdmission/RefusedBeforeAdmission/AdmittedAtR/AdmittedAtT/Answered
считаются отдельно. Wire-ack admission отсутствует; тишина не доказывает допуск.

До удержания provisional действуют codec/role/input/source-local gates, one-provisional и физические
caps. **Общий group-target ledger на ingress не списывается.** Лучший по уровню/циклическому порядку
provisional вытесняет только худший provisional: при полной цели — этой identity, при полном узле —
узла. Чужой source cap заимствовать нельзя; равный rank не churn-ит incumbent. Source-local gates
могут учитывать отказанные попытки своему источнику, но не расходовать общую долю других сессий.
Per-session128 distinct targets остаётся обязательным ограничением, а не обходится новой схемой.

L1 идут по настоящему ConnID после persistent cursor с одним wrap внутри membership watermark.
Новые high IDs получают отказ до следующего sweep. Watermark растёт только после **успешного
admitted wrap**; пустой sweep может обновить его после фиксированного интервала. Иначе бесконечные
новые IDs не дадут старым дождаться wrap. Reconnect получает новый настоящий ID. Cursor двигается
только по successful admissions: движение по всем десяти просмотренным при семи местах постоянно
отвергает тех же троих. Затем идут четыре L2 по возрасту, новые RR по cells/Session. OVERFLOW одна
cell, но L1 в ней остаётся L1; сосед по группе не наследует приоритет K.

При закрытии один owner выполняет trial→commit в этом справедливом порядке: перепроверяет сессию,
cancel/deadline/изменяемые source/pair gates/владение строкой; моделирует настоящий календарь вместе
с уже потраченными токенами и прежними резервами нижних уровней; без allocation проверяет будущую
cell, rolling share и H; только при успехе **всех** проверок атомарно публикует общий debit/renewal,
source/pair reservation, календарь и AdmittedAtR. Crypto/write вне owner-lock. Отказ до admission не
создаёт shared record или calendar debt. Cancel/failed write/close **после** admission сохраняет
committed charges и projected tokens. Поздняя фактическая отправка продлевает учёт actual consumption;
ошибка не создаёт бесплатный retry. Старые lower-level commitments нельзя вернуть ради fair intake.

Сохраняется исходное `a`: `deadline=a+6s`,
`latest_forward=deadline-deltaTarget-deltaNetwork-deltaRuntime`,
`DeltaR+dispatch_slack<=deltaRuntime`. Slot должен попасть до latest_forward. Actual dispatch сохраняет
1/с burst4 даже при одновременном пробуждении. Поздний таймер сверх slack отвергает unadmitted round
и учитывает нарушение предпосылки, а не продлевает deadline. Все Delta/δ — предложенные параметры
для утверждения/измерения до G2; 0,1с в Python — только синтетический input, slack там нулевой.

### Что списываем и какая справедливость доказана

18a §8.1 прямо называет pair3/30с **пересылками**, отдельно перечисляет128 различных целей от сессии
и node/group distinct budget. Момент списания group record — valid offer, provisional, admission
или dispatch — явно не определён. Поэтому обязательность R1 all-attempt charging из нормы не
следовала. Владелец утвердил учёт **committed forwarding reservations**. Это консервативнее успешных
отправок: admitted, но не отправленная работа всё равно расходует квоту. Это принятое уточнение,
а не утверждение, что все source/pair gates уже реализованы.

Контрпример precharge: L1 предлагает16 разных целей после заполнения named slots, удерживает один
provisional, но до RR тратит весь общий floor16 и исключает других L1. Справедливость среди оставшихся
после такого gate этого не исправляет. Принятый A допускает от неё **один новый shared debit за раунд**, в том
же справедливом порядке, что у конкурентных L1. Отклонённые offers persistent group records не создают.

Текущий owner override в начале 18a отзывает прежний безусловный deadline. Историческая арифметика
и forwarding bottleneck относились к паре(R,T). Публичное решение владельца требует fair admission
между L1. Первый ход до второго для конечной равнопотребляющей когорты — предлагаемая здесь
дисциплина, а не отдельный утверждённый владельцем алгоритм. Решение явно не задаёт node-global
max-min latency по разным целям или независимую защищённую долю каждой физической группе. Node-wide one-candidate round здесь —
отдельно утверждённое с вариантом A ограничение против захвата cross-target shared budget,
а не выведенное из per-target RR следствие.

Для конечной continuously submitting L1-когорты с совместимыми слотами ротация даёт первый ход до
следующего. При однородных запросах и K допусках за раунд счётчики различаются максимум на1.
**Новые shared group debits проверяются в этом же порядке**, а не как несправедливый предварительный
фильтр. При одинаковой маржинальной потребности конкурентов в общем overflow oversubmission не
покупает лишних ходов. Старые committed occupancy учитываются как реальные ограничения; новая схема
не отмывает прошлую трату в свежую квоту.

Это не полное max-min доказательство для произвольных пересекающихся calendars/groups, changing
cohorts, полной admitted storage и сочетания новых/уже учтённых целей. Старой цели может не
требоваться новый distinct debit — это другая потребность в ресурсе. Отказы видны по gates; один
порядок обслуживания не доказывает discoverability/безусловный deadline. Невидимой физической
overflow-группе отдельный floor не обещается; прежняя общая или lower-level трата может не оставить
места до expiry.

### Один sticky registry и rolling ledger

В A **256 named +1 OVERFLOW=257**. Физические overflow-имена не запоминаются в map/set/list.
Первые256 имён получают slots при **успешном admission**, отказанные trial ничего не выделяют.
Нет LRU, privileged displacement, promotion или второго epoch. При постоянном трафике первые имена
могут сохранять преимущество навсегда: владелец явно принял этот first-arrival bias.

W=600с скользящее. `(cell,target)` хранится минимум до `reserved_forward_time+W`, повтор берёт max
expiry, поздняя actual отправка учитывается до `actual_forward_time+W`. Failed committed остаётся.
TLS-close не сбрасывает запись. Reset тех же257 cells возможен только после W без committed group
use, expiry всех records и исчезновения **всех** старых provisional/pending/cache refs. Generation
возрастает без wrap; stale CellRef требует авторитетного lookup, не открывает новый бюджет.
Нет одновременно двух epochs или памяти всех overflow-имён.

Предложение: `share=max(16,floor(C_node/k))`; k — активные accounting cells вместе с впервые
списываемой candidate cell, **≤257 с overflow**. Считать overflow в k и округлять вниз — явные
уточнения для ревью. C_node сохраняет прежние512..3840 и degree/window rule; Python получает его
аргументом, не реализуя degree history. Новый record проходит share и **H=C_node+16*257≤7952**.
Повтор только продлевает запись. Уменьшение share не вытесняет прошлые записи и не возвращает кредит.
Hard H важнее ниже-долевого допуска, что явно ограничивает старое «ниже доли принимается всегда» при
изменении k/rolling history. Отдельный source128/10мин обязателен. `(cell,target)` commitments
учитываются консервативно; число payload не доказывает полный process memory budget.

257-я возрастная группа уже OVERFLOW,258-я делит его. Session priority сохраняется, независимый
floor каждой физической overflow-группы теряется. Большая конкурентная когорта и прежняя трата
делят один floor. Владелец принял это сужение старого «L1 не затрагивается». Нельзя одновременно
обеспечить конечный registry, независимый floor всем физическим группам и произвольное их число.
Дополнительного L1 reserve внутри7952 нет. Уровень по группе не наследуется; privacy/enumeration
ограничения не снимаются: неудавшийся committed расходует квоту, отказанные лишние offers не
пересылаются, cross-R enumeration по-прежнему ограничивается исходными rate/target gates.

### Цель T, эталон и оставшаяся работа

T должна использовать те же256 Reserved/Ready,8 refs/session,историю3 link/60с без новой очереди.
Ready-copy не требует подписи. Новая подпись резервируется в одном node signer и входящем1/с burst4
bucket. `DeltaT+target_service_slack<=deltaTarget` и предлагаемый spacing≥250мс требуют evidence/
утверждения. Другие R/direct callers конкурируют; AdmittedAtR не резервирует T. Отказ T после R
нарушает условие target-admission, нового wire-ack здесь нет.

Python содержит17 точных синтетических тестов: caps16/16/512, fixed cut/deadline, сохранение admitted
и lower tokens/reservations, provisional replacement/full-L1 intake, cursor/watermark churn,257/258 aged groups,
sticky reset/stale generation,16 distinct offers только с одним shared debit, executable контрпример
старого precharge, ротацию дефицитного
cross-target overflow, отказ без calendar/record allocation, отсутствие cancel-refund и reserved-time
expiry, hard-bound guards. Один контроль использует искусственный внутренний snapshot; другой
достигает7952 записей обычными model commits при32/с и3840 разных целях, разделяемых четырьмя
cells. Это достижимость в частичной ledger-модели, а не доказательство соблюдения всех ещё
не реализованных runtime gates этим потоком.

Это **частичный witness**, не runtime/полное online-policy proof/G2. Остаются lower-level RR,
авторитетные C5/live membership, полная source/pair/degree композиция, канонический bounded target
registry, T, concurrency transactions, actual dispatch/wire, clock/network/Tor и byte/CPU evidence.
До G2 нужны проверка выбранного A с принятыми ограничениями, профили и реальные тесты max legal bursts, full occupancy,
257+groups, hostile overflow, changing membership, close/cancel, aliases, stale generations и overrun.
Сам проект и зелёные тесты не закрывают ни один пункт D1.
