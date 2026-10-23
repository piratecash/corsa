# D1: joint 18a / 21a-min review record / Совместная сверка 18a / 21a-min

## English

### Status and authority

**Review draft, 2026-10-23. G2: OPEN. Deployment profiles: UNSELECTED. Activation: NOT AUTHORISED.**

This record consolidates accepted owner decisions and identifies the remaining joint review. Publishing it does not fix the wire contract, select a timing/resource profile, approve G2, enable a capability or release S6/D1. Earlier documents describe different implementation stages; their historical status paragraphs are not current readiness evidence.

The owner approved [decision D](overlay-design-decision.md) on 2026-10-18: `initiated-limit`, d16/B32/q1, up to10,000 nodes, including the recorded limitations and obligations. **O5 is closed within that scope.** G2 and proof on actual anonymous transport paths remain separate. The owner also approved [aggregate admission](d1-admission-budget.md), [bounded option A](d1-bounded-intake-proposal.md), and the [degree-hint persistence contract](find-node-peer-state-degree-hint.md). These policies are settled; this draft does not ask for them again.

G2 is the **joint design fixation of18a and minimal21a**, including return-routability and the fields that anonymity will require changing. It is not proof of the future complete overlay delivery path. Later owner decisions additionally require the aggregate-admission formulas, parameters and relevant policy checks before G2. S6 admission/release/observation, final resource measurements, activation, D1 release and21c have their own evidence and decisions.

### Joint wire and privacy boundary

Current inactive D1 codecs in `internal/core/protocol/find_node.go` have closed version1 field sets, a1,024-byte frame cap and duplicate-key rejection:

| Frame | Fields |
|---|---|
| `find_node` | `type`, `v`, `id`, `target`, `challenge` |
| `find_node_answer` | `type`, `v`, `id`, `target`, `pubkey`, `sig` |

There is no endpoint address or `issued_at` in these frames. The target key must fingerprint to `target`. The answer signature transcript contains its domain/network, target identity and the retained request challenge. Envelope ID/target and the original receiving session are checked separately; the ID is not an extra signed transcript field. The caller must supply the proven v2 session, negotiated rights and admission. A parser or valid signature alone grants none of them.

D1 uses the exact known live neighbour and original session path; an unknown target receives silence. It introduces no recursive D2 routing, general public initiator or address-based dial fallback. A target signature proves authorship, not ownership of an IP address. Return traffic on a one-shot reverse slot does not create a durable relay chain; end-to-end chain readiness and continuation remain a separate contract.

The minimal21a compatibility review retains these requirements:

| Future anonymous routed plane | Required treatment |
|---|---|
| `src` and reverse-answer source | Replace with hop-local labels on the protected segment; `src=request.dst` must not expose the target there. Rewriting only `dst` is insufficient. |
| Transit `auth.pubkey` / `auth.sig` | Remove from transit author parsing; the end authenticates authorship, with abuse constrained by budgets. |
| Request `dst` / payload | Hide the target before the entry point. Structural search participants may see the target after that point, without a binding to the initiator under the stated threat model. |
| Response body / continuation binding | Protect inside the end exchange; the ephemeral reply key travels inside the sealed request, not as a new clear routing field. |
| `dtype`, `class`, `ttl`, sizes and timing | Remain observable; this record claims no traffic-analysis protection. |
| Mesh-DM identity-bearing fields | Do not use that revealing plane as a hidden fallback for anonymous mode. |

These are design requirements, not descriptions of current D1 neighbour frames or implemented21b. The accepted design does not promise that only the entry point ever sees the target. Format compatibility without a wire change is **not established**: the design must retain an explicit future version/capability and migration boundary. No speculative field or future handler is added by this record. No current path is labelled anonymous; O5 approval does not prove the21c actual-path property.

### Admission and timing contract under review

Separate `AdmittedAtR`, `AdmittedAtT` and a returned answer. An R reservation cannot reserve T's contended global signer; four responders at1/s can already consume its4/s budget. Without a reservation acknowledgement on the wire, admission at R does not imply admission at T. A refusal above the aggregate budget is a valid outcome. The old promise that every per-session-legal request is served within6s is withdrawn.

Let `a` be the original authenticated receive at R. The existing answer expiry remains `a+6s`. The candidate profile has strictly positive target service, R↔T network and R-side runtime allowances `D_T`, `D_N`, `D_R`, with sum below6s:

```text
latestForward = a + 6s - D_T - D_N - D_R
```

`D_T` covers target receive through successful signature readiness **conditional on target admission**. `D_N` covers R→T and T→R. `D_R` must cover the declared R-side collection, scheduling, application-lock waits, processing and original-writer handoff, without spending one allowance twice. R→initiator delivery belongs to the separate conditional12s initiator deadline. Unknown bounds do not become zero.

The profile proof must name the original receive, projected slot, actual forward Take, T receive/admission, successful signature Complete, original answer read at R and answer-writer Take. Queue insertion, raw Write start and full-length nil-error Write completion are different events. The existing strict expiry check occurs at answer Take; it does not alone bound byte completion or delivery. No future proof may substitute an earlier queue event for its claimed terminal event.

The actual calendar accounts for all existing projected reservations and spent actual tokens, including lower levels and every connection to the same target identity. Projected admission permits a slot at `s<=latestForward`; actual send also charges the real bucket. Cancellation/close do not refund debt. For an empty calendar only, `floor(beta+W/1s)`, `0<=beta<=4`, `W=latestForward-now>=0`, is an upper bound on admissions, also limited by free physical/source places. If `W<0`, no forwarding slot fits and admission is refused; this is not a negative capacity. The formula is not a sufficient test with existing reservations and does not promise maximal admission.

**Conditional strict composition; endpoint coverage still needs a profile proof.** Let `f` be the actual R forward-Take sample, `r` the original T receive sample, `c` the successful timed-T Complete sample, `h` the original answer-receive sample at R, and `u` R's answer-Take sample. A successful actual forward Take checks `s<=f<=latestForward`; projected `s<=latestForward` alone is insufficient. If the applicable T service bound is no larger than R's `D_T`, `c-r<D_T`, and the fully covered remaining durations satisfy `(r-f)+(h-c)+(u-h)<=D_N+D_R`, then:

```text
u = f + (r-f) + (c-r) + (h-c) + (u-h)
  < f + D_T + D_N + D_R
  <= a + 6s
```

Thus the existing strict timed-T component can supply the strict inequality even when every remaining bound is closed; no extra positive margin is mathematically necessary under those premises. These symbols are event times on one proof scale, not raw Instants from different owner clock domains; a profile must justify the duration/clock interpretation. The statement is conditional on the exact live original sessions, successful target admission/signing and the declared bounds. It neither proves service for every admitted R request nor promises byte completion or initiator delivery. Untimed/cached/coalesced paths require their own applicable-bound/recipient proof and are not silently retimed by this theorem.

`c` is the genuine sample inside Complete after signing and lock waits, **before** cache completion, answer-cell minting, publication/unlock and return. It is not the function-return time or T answer-writer start. The interval `h-c` includes those T tails, Node registry/common waits, answer marshal/queue, the shared writer's backlog/deadline/Take/Write, the return transport and R's original read/capture. A wire-only `D_N` together with an R-only `D_R` leaves that T-local tail uncharged. The selected profile must explicitly cover every such segment exactly once, for example within a declared return-leg bound or separately justified target headroom; this draft selects neither allocation. An `E_T` proven to bound actual T Take-sample→Complete return would also imply a strict return bound from actual Take<latestSignStart, but the constructor inequality and the Complete-sample check alone do not prove that physical duration. Reusing an already spent runtime allowance is insufficient. G2/profiles remain open; no numerical reserve, timer/code change or unconditional guarantee is introduced.

For explicitly timed owned-only T, the current candidate separates signature service from answer expiry:

```text
0 < E_T < D_T < 6s
signatureReadyBy = r + D_T
latestSignStart = signatureReadyBy - E_T
admission final f < latestSignStart
projected signing slot s < latestSignStart
successful original Complete(signature) final f < signatureReadyBy
answerDeadline = r + 6s
```

All equalities refuse. `E_T` is an explicit allowance for actual Take→successful Complete; it is not measured CPU/network evidence. Cached/duplicate recipients retain their original deadlines; an already protected job is not retimed. Old untimed constructors keep their existing semantics and do not silently acquire a smaller service bound.

| Explicit profile input | Status / obligation |
|---|---|
| `D_T`, `D_N`, `D_R`, `E_T` | **UNSELECTED**. Define events, strict boundaries, evidence, supported participants and behaviour when an assumption fails. Join R's target allowance to the service profile applicable at each eligible T; the APIs do not prove remote agreement. |
| Round, Slack | **UNSELECTED**. Round>0, Slack≥0, Round+Slack≤`D_R`; demonstrate actual scheduler/lock/writer tails rather than treating this constructor inequality as a measurement. |
| Signer spacing, MaxReservations | **UNSELECTED**. Current API accepts spacing≥250ms and≤6s, reservations1…256; no extra node-wide burst. Existing source-session1/s burst4 remains independent. API ranges are not deployment choices. |
| Initial R tokens / membership-resource profile | **UNSELECTED**. Initial tokens0…4 do not permit resetting old debt. `d_win<=64` is not a bound on all open sessions or snapshot/transient memory. |

Fixture values, including100ms collection/slack and the timed test's250ms scale, are test inputs. They are not substitutes for these selections or Tor/first-contact/resource measurements.

### Settled bounded policy

All provisional/reserved/dispatching/awaiting/terminal rows together remain bounded by16/source session,16/target identity and512/node. One provisional per source session participates in a fixed node round. L1 applies to the original session's authenticated K/N-sel identity class and admission rotates between proven source sessions; fair service of an already occupied queue is insufficient. L2 is eligible through live N-in or that session's age>10min, and chooses only the four oldest eligible sessions per(R,T). A stored N-in pin without a live session retains downgrade protection but supplies no priority. Priority is a session property, not inherited by its physical group. Accounting groups remain IPv4/16, IPv6/32 and one loopback group; this gives no independent per-onion-peer group floor.

There are256 sticky named accounting cells and one shared overflow cell. A successful atomic commit alone allocates a name/record and shared debit. Refused provisional work spends no shared quota. With `C_node=max(512,60*min(d_win,64))<=3840`, the hard group-record bound is `H=C_node+16*257<=7952`; share is `max(16,floor(C_node/k))`. H can refuse even below share. Existing debt/accepted obligations survive a smaller current share and cancellation. The first256 groups can retain an advantage indefinitely under load; overflow preserves session priority but grants no independent physical-group minimum. Neither L1/L2 availability nor full resistance to budget monopolisation is promised.

The accepted hint law keeps the old value **and original date** until a complete600s observed window after observer installation. Age≥24h makes it ineligible; no eligible hint means initial512. After a full window, a genuine zero replaces an old nonzero. The hint applies only to the first capacity epoch. H bounds a group ledger, not whole RSS: source companions, pending/cache state, snapshots, retained handles and transient allocations remain separate costs.

### Exact historical statements superseded for the current review

The following paths/section anchors identify historical plan passages; they are not files to add to Git. Preserve the historical text and use the accepted replacement in the current contract.

| Historical anchor / phrase | Current disposition |
|---|---|
| `docs/refactoring/dht/18a-wire-contract-draft.md`, opening note: contradiction about always naming priority groups “remains open” | Policy choice is settled by option A; its limitations and evidence remain. |
| Same, §8.6: all groups with L1/L2 sessions “are always included” among256 names | Replaced by256 named+one overflow; session priority survives, individual overflow group minimum does not. |
| Same, §8.6: overflow has “no priority” | Accounting cell does not transfer/remove the proven session's priority. |
| Same, §8.6: a group below its share “is always admitted” | False under the accepted hard H and other admission gates. |
| Same, §11 TM-18: L1 “is not affected” | Too strong as an availability/budget claim; retain only accepted session-priority/protected-obligation semantics and named limitations. |
| Same, historical introduction/§8.6 arithmetic: all per-session-legal calls fit6s | Withdrawn by the2026-10-18 aggregate-admission decision. |
| `18-session-establishment.md` §5.8; `21-anonymity-transport.md` §10.3/§11.2: O5 open / design not yet accepted | Superseded by the signed decision D **in its accepted scope**; G2 and actual-path/21c obligations stay open. O6/O7 do not become new questions. |
| Earlier public slice documents saying Node composition is future or no code has run | Historical author-stage limitations. Current private Ready/chain/timing QA exists; public activation, complete online/profile evidence and release are still absent. |

### Evidence ledger and remaining review

The counts below are attributed to the accepted project QA record; this document performs no new audit or run. Carry evidence by exact source/body identity, not by matching a test name. Different source inventories and composite runs must not be counted as one fresh whole run.

This matrix includes both pre-G2 contract/profile evidence and downstream final-candidate evidence. Listing final Linux builds/resources/T5–T7 here does not create a new design gate or require them all to close G2; their release obligations remain separate.

| Area | Accepted scoped evidence | Still required |
|---|---|---|
| A intake / debt / priority | QA355 on4c9/1991:1671 ordinary+1671 race PASS; SOURCE110 and124 functional modes accepted by tester/reviewer. Calendar14 is a subset, not extra runs. Current tests include sustained first256/late overflow competition. | Map maximum legal burst, full lower-level occupancy, reconnect/churn and rotating L1 admission to the exact selected Runtime profile and genuine read→round→writer paths. Reuse unchanged accepted QA; do not repeat QA355 without changed inputs. |
| Capacity / hint | Approved d_win contract and later degree/Ready checks are in committed `ecaebdc`; constructor/epoch/observer origins remain distinct. | Final-candidate Linux source/build/runtime binding and resource evidence; structural bounds alone are not RSS/CPU acceptance. |
| N-in→Ready and complete original A→R→T→R→A | exact16 N-in/Ready:55 parents/142 PASS ordinary and race (TESTER68d54424/REVIEWER81e2dab8). Chain:1 parent/4 children/5 PASS in each mode (0a580eb9/7b78af2f). | These use real TLS/F1/writers/signatures with fixture N-in/initiator clocks. They do not prove whole Service.Run/public lookup or every concurrent target/relay policy scenario. |
| Explicit T timing | timing25 accepted (c9ebf811/c7700734): composite Core70/156 and Node68/224 in each mode, with changed trials/TYPE/lint/causal checks qualified separately. | Not a new single138-parent run. Select/evidence the deployment profile, strict deadline composition, competing R/direct clients and target saturation. |
| Current final Linux candidate — downstream gates | Committed source capture1794/73dd is distinct from historical canonical1790/fee57 and diagnostic1791. A current app attempt completed only lane0's three artifacts, then failed before lane1 Go. | Both independent lanes/equality, fresh test artifacts, actual runtime/ABI/selector bindings and required T5/T6/T7 evidence. A diagnostic branch or one completed lane gives no final candidate acceptance. |
| Joint anonymity design | Decision D closes scoped O5; O6/O7 decisions and required field/privacy changes are recorded. | One jointly reviewed18a+21a-min normative revision, version/migration boundary and explicit G2 decision. Actual anonymity transport/21c remains separate. |

The concrete pre-G2 checklist is: reconcile the superseded promises; supply one profile with every item above explicitly selected or marked unavailable; join each formula to its actual event/strict boundary; publish a scenario-to-existing-QA/missing-evidence matrix; and submit the exact joint revision for the owner's G2 decision. No broad “approve all future gates” request follows from this draft. [S6 external review](../operations/s6-external-review-package.md), [build provenance](loadstand-build-provenance.md) and the [release sequence](../release.md) retain their own scopes.

## Русский

### Статус и основание

**Проект на ревью от2026-10-23. G2: ОТКРЫТ. Профили эксплуатации: UNSELECTED. Активация: НЕ РАЗРЕШЕНА.**

Документ сводит решения владельца и остаток совместной проверки. Публикация не фиксирует провод, не выбирает временной/ресурсный профиль, не закрывает G2, не включает capability и не разрешает выпуск S6/D1. Исторические заголовки документов относятся к разным этапам реализации.

[Акт D](overlay-design-decision.md) утверждён2026-10-18: `initiated-limit`, d16/B32/q1, до10000 узлов, с оговорками и обязательствами. **O5 закрыт в этой области.** G2 и доказательство на фактических анонимных путях отдельны. [Совокупный допуск](d1-admission-budget.md), [вариант A](d1-bounded-intake-proposal.md) и [сохранение degree hint](find-node-peer-state-degree-hint.md) уже согласованы; повторного вопроса об этих политиках нет.

G2 — **совместная фиксация дизайна18a и минимального21a**, включая return-routability и поля, которые потребуется менять для анонимности. Это не доказательство всей будущей доставки оверлея. Более поздние решения владельца требуют до G2 формулы, параметры и соответствующие проверки совокупного допуска. Допуск/выпуск/наблюдение S6, итоговые ресурсы, активация, выпуск D1 и21c имеют собственные доказательства и решения.

### Граница провода и приватности

Текущие неактивные codecs `internal/core/protocol/find_node.go` имеют закрытые наборы полей версии1, предел1024байта и запрет повторяющихся JSON-ключей:

| Кадр | Поля |
|---|---|
| `find_node` | `type`, `v`, `id`, `target`, `challenge` |
| `find_node_answer` | `type`, `v`, `id`, `target`, `pubkey`, `sig` |

Адреса endpoint и `issued_at` нет. Отпечаток ключа цели обязан совпадать с `target`. Подписывается домен/network, identity цели и **сохранённый challenge запроса**. ID/target конверта и исходная принимающая сессия проверяются отдельно; ID не является дополнительным полем подписанного transcript. Proven v2, согласованные права и admission обеспечивает вызывающий путь; parser/подпись их не выдают.

D1 идёт через точного известного живого соседа и исходные сессии; неизвестная цель получает молчание. Рекурсивный D2, общий публичный инициатор и дозвон по адресу из ответа не добавляются. Подпись доказывает автора, а не владение IP. Однократный reverse slot не создаёт долговременную цепь; сквозная готовность/продолжение цепи — отдельный контракт.

Минимальный21a сохраняет следующие требования к **будущей анонимной routed-плоскости**:

| Поля | Требование |
|---|---|
| `src`, включая обратный ответ | Локальная метка на защищённом участке; `src=request.dst` там не раскрывает цель. Одного изменения `dst` недостаточно. |
| Transit `auth.pubkey` / `auth.sig` | Убрать из транзитного разбора авторства; авторство проверяет конец, злоупотребление ограничивается бюджетами. |
| Request `dst` / payload | Скрыть цель до точки входа. После неё структурный поиск видит цель, но не привязку к инициатору в названной модели угроз. |
| Response body / продолжение | Внутри защищённого обмена; эфемерный ключ ответа находится внутри печати запроса, а не в новом открытом routing-поле. |
| `dtype`, `class`, `ttl`, размеры, тайминги | Наблюдаемы; защиты от анализа трафика документ не обещает. |
| Раскрывающие identity поля mesh-DM | Не использовать эту плоскость как скрытый fallback анонимного режима. |

Это требования дизайна, не описание нынешних соседских D1-кадров и не готовая21b. Модель не обещает, что цель знает только точка входа. Совместимость без смены провода **не установлена**: сохраняется явная граница будущей версии/capability и миграции. Новые поля/handlers «на будущее» не добавляются. Нынешний путь не называется анонимным; принятие O5 не доказывает21c.

### Допуск и время — предмет проверки

`AdmittedAtR`, `AdmittedAtT` и ответ — разные события. R не резервирует глобальный signer T: четыре R по1/с уже расходуют4/с цели. Без wire acknowledgement принятие у R не означает принятие у T. Отказ выше совокупного бюджета допустим. Прежняя гарантия всех разрешённых по сессии запросов за6с отозвана.

Для исходного authenticated receive `a` у R прежний expiry ответа — `a+6s`. Явные `D_T,D_N,D_R>0`, сумма<6с:

```text
latestForward = a + 6s - D_T - D_N - D_R
```

`D_T` — receive цели→готовая подпись **при допуске у T**. `D_N` — R→T и T→R. `D_R` покрывает объявленные collection/scheduling/ожидания application-lock/обработку/передачу исходному writer, без двойного расходования одного allowance. R→инициатор относится к отдельному условному12с deadline. Неизвестная граница не равна0.

Доказательство профиля обязано назвать исходный receive, projected slot, actual forward Take, receive/admission T, успешный Complete подписи, исходное чтение ответа у R и answer-writer Take. Queue insertion, начало raw Write и полный Write с nil error — разные события. Строгая expiry-проверка стоит на answer Take и сама не ограничивает завершение байтов/доставку. Нельзя подменять заявленное конечное событие более ранней постановкой в очередь.

Календарь учитывает все прежние projected reservations и потраченные actual tokens, включая нижние уровни и все links к той же identity цели. Projected slot допускается при `s<=latestForward`; actual send отдельно списывает реальный bucket. Cancel/close долг не возвращают. Только для пустого календаря `floor(beta+W/1s)`, `0<=beta<=4`, `W=latestForward-now>=0` — верхняя оценка с дополнительными физическими/source-пределами. При `W<0` подходящего forwarding slot нет и допуск отказывает; отрицательная ёмкость из формулы не выводится. Формула не является достаточным критерием при прежних резервах и не обещает максимального допуска.

**Условная строгая композиция; покрытие конечных событий ещё требует доказательства профиля.** Пусть `f` — sample фактического forward Take у R, `r` — исходный receive sample у T, `c` — sample успешного timed-T Complete, `h` — исходный answer-receive sample у R, `u` — sample answer Take у R. Успешный actual forward Take проверяет `s<=f<=latestForward`; одного projected `s<=latestForward` недостаточно. Если применимый service bound T не больше `D_T` у R, `c-r<D_T`, а полностью покрытые остальные длительности удовлетворяют `(r-f)+(h-c)+(u-h)<=D_N+D_R`, то:

```text
u = f + (r-f) + (c-r) + (h-c) + (u-h)
  < f + D_T + D_N + D_R
  <= a + 6s
```

Значит, имеющийся strict timed-T component может дать строгое неравенство и при замкнутых остальных bounds; дополнительный положительный запас математически не нужен при этих предпосылках. Символы — времена событий на одной шкале доказательства, не raw Instant разных owner clock domains; профиль обязан обосновать интерпретацию длительностей/часов. Утверждение условно: нужны точные живые исходные сессии, успешный допуск/подпись у T и объявленные границы. Это не доказательство обслуживания каждого принятого R запроса и не гарантия завершения байтов/доставки инициатору. Untimed/cached/coalesced paths требуют своего applicable-bound/recipient proof и этой теоремой молча не передвигаются.

`c` — genuine sample внутри Complete после подписи и lock waits, **до** cache completion, создания answer cells, publication/unlock и возврата. Это не время возврата функции и не старт answer writer у T. Интервал `h-c` включает эти хвосты T, ожидания Node registry/common, marshal/queue ответа, backlog/deadline/Take/Write общего writer, обратный транспорт и исходные read/capture у R. Чисто wire-only `D_N` вместе с R-only `D_R` оставляют такой локальный хвост T без бюджета. Выбранный профиль обязан явно покрыть каждый участок ровно один раз, например объявленной границей обратного leg либо отдельно обоснованным target headroom; проект не выбирает ни одно распределение. Если доказано, что `E_T` ограничивает фактические Take-sample→возврат Complete у T, actual Take<latestSignStart также даёт строгую границу возврата; но inequality конструктора и проверка Complete sample сами эту физическую длительность не доказывают. Повторно использовать уже потраченный runtime allowance нельзя. G2/профили остаются открытыми; численный запас, новый таймер/код или безусловная гарантия не вводятся.

Явный owned-only timed T отделяет сервис подписи от expiry ответа:

```text
0 < E_T < D_T < 6s
signatureReadyBy = r + D_T
latestSignStart = signatureReadyBy - E_T
admission final f < latestSignStart
projected signing slot s < latestSignStart
успешный исходный Complete(signature) final f < signatureReadyBy
answerDeadline = r + 6s
```

Равенства отказывают. `E_T` — явный allowance actual Take→успешный Complete, не замер CPU/сети. Cached/duplicate recipients сохраняют исходные сроки; protected job не передвигается. Старые untimed-конструкторы не получают меньший service bound автоматически.

| Вход профиля | Статус / обязательство |
|---|---|
| `D_T`, `D_N`, `D_R`, `E_T` | **UNSELECTED**. Определить события, строгие границы, доказательства, область участников и отказ при нарушении предпосылки. Связать allowance R с действующим профилем каждой допустимой T; API не доказывают согласование удалённых узлов. |
| Round, Slack | **UNSELECTED**. Round>0, Slack≥0, Round+Slack≤`D_R`; нужны фактические scheduling/lock/writer tails, не только inequality конструктора. |
| Signer spacing, MaxReservations | **UNSELECTED**. API принимает spacing≥250мс и≤6с, reservations1…256; без дополнительного burst узла. Отдельный source-session1/с burst4 сохраняется. Диапазон API — не выбор эксплуатации. |
| Initial R tokens / membership-resource profile | **UNSELECTED**. Initial tokens0…4 не разрешают сброс старого долга; d_win<=64 не ограничивает все открытые сессии, snapshots и transient memory. |

Фикстурные100мс collection/slack и250мс масштаба timed-теста — входы тестов, не параметры эксплуатации и не замена замерам Tor/первого контакта/ресурсов.

### Согласованная ограниченная политика

Все provisional/reserved/dispatching/awaiting/terminal вместе занимают максимум16/source session,16/identity цели и512/node. В фиксированном раунде узла — одна provisional от сессии. L1 — authenticated класс K/N-sel identity исходной сессии; admission вращается между proven сессиями, а не только внутри уже занятой очереди. L2 даёт живая N-in либо возраст самой сессии>10мин; выбираются только четыре старейшие на(R,T). Pin N-in без живой сессии защищает downgrade, но приоритета не даёт. Уровень не наследуется физической группой. Accounting группы — IPv4/16, IPv6/32 и одна loopback; отдельного group floor каждому onion-пиру нет.

256 sticky named cells и одна overflow; только успешный атомарный commit выделяет name/record и общий дебет. Отказ provisional общую квоту не расходует. `C_node=max(512,60*min(d_win,64))<=3840`; hard `H=C_node+16*257<=7952`; share=`max(16,floor(C_node/k))`. H может отказать ниже share. Снижение share и cancel не снимают прежний долг/обязательства. Первые256 могут удерживать преимущество бесконечно под нагрузкой; overflow сохраняет приоритет сессии, но не отдельный минимум каждой физической группе. Доступность L1/L2 и полная защита от монополизации не гарантированы.

Прежний hint и **исходная дата** сохраняются до полных600с после установки наблюдателя. Возраст>=24ч делает hint непригодным; без него старт512. Полное новое окно сохраняет настоящий0 вместо старого ненулевого. Hint действует только первое capacity epoch. H ограничивает group ledger, а не весь RSS: source companions, pending/cache, snapshots, retained handles и временные allocation учитываются отдельно.

### Какие исторические обещания заменены

Пути/разделы ниже — точные указатели исторического плана, а не файлы для Git. История сохраняется; текущая редакция использует принятые замены.

| Исторический указатель / формулировка | Текущее положение |
|---|---|
| `docs/refactoring/dht/18a-wire-contract-draft.md`, вводное примечание: противоречие «все приоритетные группы именованные» остаётся открытым | Выбор A уже сделан; остаются принятые ограничения и доказательства. |
| Там же §8.6: группы L1/L2 «учитываются всегда» среди256 | 256 named+одна overflow; приоритет сессии сохраняется, независимого physical-group floor в overflow нет. |
| Там же §8.6: overflow «без приоритета» | Accounting cell не выдаёт и не снимает proven session priority. |
| Там же §8.6: группа ниже доли «принимается всегда» | Неверно при hard H и остальных gates. |
| Там же §11 ТМ-18: «Уровень1 … не затрагивается» | Слишком сильная гарантия доступности/бюджета; остаются только session priority/protected obligations и названные ограничения. |
| Историческая арифметика §8.6/введения: весь разрешённый сессиями поток укладывается в6с | Отозвана решением2026-10-18 о совокупном допуске. |
| `18-session-establishment.md` §5.8; `21-anonymity-transport.md` §10.3/§11.2: O5 открыт/дизайн не принят | Акт D закрывает O5 **в принятой области**. G2 и actual-path/21c остаются; O6/O7 не становятся новыми вопросами. |
| Ранние public slice docs: Node композиция впереди/код не исполнялся | Исторический author-stage. Ныне есть private Ready/chain/timing QA; public activation, полная online/profile evidence и выпуск не подтверждены. |

### Свидетельства и остаток проверки

Числа ниже атрибутированы принятому проектному QA; документ не выполняет новый аудит/прогон. Carry требует точного source/body, а не совпадения имени теста. Разные inventories и составные результаты не превращаются в один свежий whole-run.

Матрица включает как pre-G2 contract/profile evidence, так и последующие final-candidate свидетельства. Перечисление Linux builds/ресурсов/T5–T7 не создаёт новый design gate и не требует закрыть их все для G2; обязательства выпуска отдельны.

| Область | Принятое ограниченное свидетельство | Что остаётся |
|---|---|---|
| A intake/debt/priority | QA3554c9/1991:1671 ordinary+1671 race; SOURCE110 и124 functional modes приняты TESTER/REVIEWER. Calendar14 — поднабор. Есть sustained first256/late overflow. | Максимальный legal burst, полная lower-level occupancy, reconnect/churn и L1 rotation на точном выбранном Runtime profile и реальных read→round→writer. Не повторять неизменённый QA355 без новых входов. |
| Capacity/hint | Контракт d_win и degree/Ready проверки в `ecaebdc`; constructor/epoch/observer origins раздельны. | Final Linux source/build/runtime binding и ресурсы; структурные пределы не RSS/CPU acceptance. |
| N-in→Ready и A→R→T→R→A | exact16:55parents/142PASS ordinary/race (68d54424/81e2dab8). Chain:1parent/4children/5PASS в каждом режиме (0a580eb9/7b78af2f). | Настоящие TLS/F1/writers/signatures при fixture N-in/initiator clocks; не весь Service.Run/public lookup и не все конкурентные сценарии T/R. |
| Explicit T timing | timing25 принят (c9ebf811/c7700734): составной Core70/156 иNode68/224 в каждом режиме; изменённые trials/TYPE/lint/causality квалифицированы отдельно. | Это не новый единый138-parent run. Нужны deployment profile, strict composition, competing R/direct clients и saturation цели. |
| Текущий final Linux — следующие gates | Source1794/73dd отличается от canonical1790/fee57 и diagnostic1791. Текущая app-попытка завершила только три артефакта lane0 и отказала до Go lane1. | Две независимые lanes/equality, свежие test-артефакты, runtime/ABI/selector bindings и требуемые T5/T6/T7. Диагностика/одна lane final acceptance не дают. |
| Joint anonymity design | D закрывает scoped O5; O6/O7 и требуемые field/privacy изменения записаны. | Совместная normative18a+21a-min, version/migration boundary и отдельный G2. Actual anonymity/21c отдельно. |

Конкретный pre-G2 пакет: устранить противоречащие текущие обещания; предъявить один профиль с выбранными либо явно недоступными пунктами; привязать формулы к фактическим событиям/strict boundaries; составить сценарии→принятое QA/недостающие доказательства; передать точную совместную редакцию на решение G2. Общего разрешения будущих gates из проекта нет. [S6 review](../operations/s6-external-review-package.md), [build provenance](loadstand-build-provenance.md) и [порядок выпуска](../release.md) сохраняют свои области.

## P0 numerical proposal: reconciled ledger / Предложение P0: единый бюджет

### English — UNSELECTED, no activation

This appendix is a proposal for a later joint revision, not a selected profile, measurement, G2 decision or permission to enable D1. It replaces P0's earlier allocation of the entire R-runtime allowance to post-answer work. The preceding general conditional theorem remains; the following one ledger gives P0's proposed meaning of each allowance.

| Proposed P0 input | Value | Status |
|---|---:|---|
| R `TimingProfile.Target`, `Network`, `Runtime` |2s /2s /1s| Budget targets requiring evidence, not observed latency bounds |
| Timed T `ServiceBound`, `RuntimeReserve` |2s /500ms| Applicable T profile and physical Take→Complete evidence required |
| Collection Round / Slack |100ms /100ms| Configuration choices, charged inside pre-forward R work below |
| Signer `NodeSpacing` / `MaxReservations` |250ms /8| Existing minimum spacing; no extra node-wide burst or promise to finish8 jobs |
| R initial tokens |4| Existing maximum; reconnect/cancel never renew spent debt |
| Degree `MaxActive` / Priority `MaxMembers` / `MaxRegistryVisits` |512 /512 /1024| Proposed coverage envelope, subject to actual connection limits, registry multiplicity and resource evidence; no new public default |

**One nonoverlapping segment ledger.** Let `P_pre` be R's collection, scheduling, processing, application-lock and forward-writer-handoff time before the original forward Take `f`, excluding intentional waiting solely for the assigned future token/calendar slot. Propose `P_pre≤400ms`; Round100ms+Slack100ms are part of those400ms, leaving at most200ms for the other pre-forward work. Let `P_post=u−h` be original R answer-read/capture→processing/locks→answer-writer Take, with proposed `P_post≤600ms`. Thus R's aggregate runtime allowance is `D_R=400ms+600ms=1s`; it is not1s available independently on each side. These are unproved physical budget premises, not new enforced400/600ms subdeadlines.

Let `Q` be the remaining pre-forward elapsed time spent solely waiting for the assigned calendar/token slot. In the proposed nonoverlapping trace, `f−a=P_pre+Q`. All actual queue delay is therefore already inside `f−a`; scheduler lateness and lock waits belong to `P_pre`, not an uncharged part of `Q`. `D_N=2s` bounds `(r−f)+(h−c)`, including T's entire post-Complete publication/unlock/return, marshal/queue, common-writer backlog/Take/Write and return transport/original R read. `D_T=2s` bounds `c−r` strictly, conditional on successful timed T admission/signing. No T-local tail is left outside the ledger.

The unchanged R constructor gives `latestForward=a+1s`; every actual forward Take still checks `f≤a+1s` and real tokens. With the stated physical premises:

```text
u−a = (f−a) + (r−f) + (c−r) + (h−c) + (u−h)
    < 1s + 2s + 2s + 600ms = 5.6s.
```

`P_pre` is not added again after using the complete `f−a` interval. The400ms difference to6s is nominal algebraic headroom only. It is not an accepted clock-error budget or a measured safety margin. If estimates on different clocks are used, an explicit physical-duration/clock-rate/offset-error model must cover every term, including the forward deadline interpretation. A proposed aggregate positive error bound would have to fit strictly inside that nominal400ms; no such bound is supplied or selected here. Raw Instants from different owners cannot be subtracted as evidence. Without those premises the arithmetic proves no physical latency guarantee. Answer Take remains distinct from Write completion or initiator delivery. Untimed/cached/coalesced paths need their own applicable-bound proof.

Aggregate admission remains bounded option A. Empty-calendar beta4/W1 gives an upper bound5; collection at a+100ms gives upper bound4, not a guaranteed count. Existing lower-level debt/reservations, occupied places, other R/direct clients and actual elapsed time can reduce admission to zero. Preserve atomic debit only on acceptance, one provisional/session/round, rotating L1 admission, cancellation debt and first256/overflow limitations. Four idle target projections at0/.25/.5/.75s fit strictly before latestSignStart=r+1.5s, but calendar fit proves neither physical500ms completion nor T admission for each R admission.

Only these evidence gaps remain for this proposal; no new framework or run is authorised:

| Existing suitable scoped evidence | Necessary missing evidence |
|---|---|
| QA355/A/calendar/overflow, and original membership/raw-capture coverage controls in `find_node_connection.go` | Match maximum burst, occupied lower-level debt and genuine session rotation to this profile. For512/1024, bound the full visited registry population, including nonmembers/registered/accepted/CM-post-setup entries;512 eligible sessions alone does not prove1024 visits sufficient. Preserve complete-or-refuse, not first-M acceptance |
| N-in→Ready142-event composition; original A→R→T→R→A chain5 events/mode (0a580eb9/7b78af2f) | Reuse original TLS/F1/read/writer/target-signature custody. These fixture-clock receipts do not supply one physical six-event duration record or this profile's clock/error bound |
| Explicit target timing25 (c9ebf811/c7700734) | Qualify actual Take→Complete500ms, Complete→original target writer→R read tail and R pre/post work under the declared R/direct contention and supported environment. Inventory existing observations first; add only an indispensable missing observation after separate review |
| Existing bounded constructors and degree/capacity/hint checks | Map512/512/1024 to actual session/connection limits and maximum registry multiplicity; inventory retained/transient allocations and obtain separately agreed CPU/RSS/churn evidence. Structural integer checks and d_win≤64 are not whole-node resource acceptance |

No existing receipt is relabelled as a fresh P0 measurement. S6 Windows/crypto/resource admission, release/observation, final13b-D1, PR6 activation and D1 release remain separate. No typedlookup/Ready/N-in permutation is added as a new G2 gate.

### Русский — профиль НЕ ВЫБРАН, активации нет

Это предложение для последующей совместной редакции, а не замер, решение G2 или разрешение включить D1. Общая таблица выше задаёт кандидата2/2/1с, timed T2с/500мс, Round/Slack100/100мс, signer250мс/8, начальные4 токена и предлагаемый coverage512/512/1024. Все значения требуют отдельного выбора и необходимых свидетельств; defaults не меняются. Прежнее предложение отдать весь1с R-runtime только работе после ответа заменено единым бюджетом.

`P_pre≤400мс` включает сбор раунда, планирование, обработку, ожидание application-lock и передачу исходному forward-writer до Take;100мс Round и100мс Slack входят в эти400мс, остальные pre-операции получают не более200мс. Исключено только намеренное ожидание будущего назначенного token/calendar slot (`Q`). `P_post=u−h≤600мс` включает исходное чтение/capture ответа R, обработку, locks и answer-writer Take. Итого `D_R=400+600=1000мс`, а не по секунде каждой стороне. Это пока недоказанные физические предпосылки, не новые enforced subdeadlines.

Разбиение `f−a=P_pre+Q` учитывает всю очередь до forward Take. Scheduler lateness и locks относятся к `P_pre`; повторно прибавлять `P_pre` после полного `f−a` нельзя. `D_N=2с` покрывает оба остаточных интервала `(r−f)+(h−c)`, включая весь хвост T после sample Complete: publication/unlock/return, marshal/queue, общий writer с backlog/Take/Write и обратную сеть/исходное чтение R. `c−r<2с` требует успешного допуска и подписи применимым timed T. При фактическом `f≤a+1с` формула выше даёт `u−a<5,6с`.400мс до6с — только номинальный арифметический остаток, не принятый error budget и не измеренный запас.

Для разных owner-clock нужен явный переход к физическим длительностям: rate/offset/error для каждого слагаемого и forward deadline. Положительная совокупная ошибка должна была бы строго поместиться в номинальные400мс; такого доказанного/выбранного предела здесь нет. Raw Instants разных owners не вычитаются как свидетельство. Без предпосылок физическая гарантия не доказана. Answer Take не равен завершению Write/доставке инициатору; untimed/cached/coalesced случаи требуют собственного applicable-bound доказательства.

Вариант A сохраняется: предел5 при пустом календаре beta4/W1 и предел4 после100мс сбора — верхние оценки, не обещание допуска. Уже занятые lower-level токены/места, другие R/direct клиенты и время могут дать ноль. Списание атомарно при принятии; одна provisional/сессия/раунд, L1 rotation, долг после отмены и ограничения первых256/overflow прежние. Четыре idle signing slots0/.25/.5/.75с укладываются до r+1.5с, но это не замер500мс и не обещание T admission каждому принятому R запросу.

Остаток свидетельств ровно из таблицы: привязать существующие QA355/coverage к профилю; не считать512 подходящих сессий доказательством достаточности1024 visits с nonmember/registered/accepted/CM-post-setup; сохранить complete-or-refuse. Принятые N-in/Ready142 и chain5 подтверждают исходные TLS/F1/read/writer/signature пути, но при fixture clocks не дают единого физического журнала шести событий. Target timing25 не заменяет физическое доказательство500мс, хвоста T, pre/post R при объявленной конкуренции и модели часов. Сначала инвентаризуются готовые observations; только необходимый отсутствующий observation обсуждается отдельно. Для coverage нужны реальные session/connection bounds, максимальная кратность registry, retained/transient allocation и отдельно согласованные CPU/RSS/churn данные. Новый framework, прогоны или permutation-тесты этим приложением не назначаются. Старые receipts не становятся свежими P0-замерами; S6/Windows/экспертиза/наблюдение/13b/PR6/выпуск D1 остаются отдельными.
