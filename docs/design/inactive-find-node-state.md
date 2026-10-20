# Inactive FIND_NODE state building blocks

## English

This is a partial implementation of PR-4/5 in `internal/core/findnode`, after the separately
reviewed codec. Only the private inactive F1 adapter imports this package; no live intake caller is installed. It installs no dispatcher, advertises
no capability, writes no frame, signs no payload and changes no pin. It is not completed PR-4/5,
G2 approval, a timing-profile approval or D1 release admission.

### Implemented boundaries

| Block | Enforced locally | Required enclosing contract |
| --- | --- | --- |
| `Session` | Identity and remote capability come from a real `sessionv2.ProvenSession`; zero proof and missing local/remote `overlay_find_node_v1` refuse; outbound IDs never wrap or reuse | Bind one retained Session to its actual reader/ConnID, opening time and reached-address `node.sourceGroup`; perform role/responder authorization |
| `PrioritySnapshot` | Explicit authoritative K/N-sel/N-in class function; priority belongs to a session, never its group; four oldest eligible level-2 sessions; no zero class default | Supply **all live candidates for this target**, not just cache holders; install a real C5 class provider; do not derive protected classes from unpinned metadata |
| `PendingTable` | Exact direct target, one forwarded request per active source ID, source/target session correlation, original challenge and ID retained; pending 16/source session, 16/target identity across all its TLS links, 512/node; six-second expiry | Role/capability, command and input-rate gates; pair/distinct-target admission; fair deadline reservations; actual send and reader-close lifecycle |
| `TargetCache` | At most 256 Reserved/Ready rows, eight active row references/session, three historical session recipients/challenge; one work token per cached challenge; one response/copy per challenge/session; 60-second Ready lifetime; stale completion refuses | Actual node/per-session signing admission and crypto; finite signing deadline; complete current priority snapshot |
| `PairWindow` | Required typed `PairKey` = proven source Session + target identity across its links; three attempts per 30-second sliding window, three timestamps, no map | Real-path admission integration; private A-owner now has separate fixed source128/pair3 histories |
| `ForwardCalendar` | Required target-identity binding; one shared R→target 1/s, burst-4 bucket across its TLS links, at most 16 reservations; append-only slot reservation through an explicit latest time; actual-time charge prevents delayed jobs from bunching beyond burst 4 | All priority levels share the calendar; reserve before admitting; bounded fair intake rounds, timing evidence, cancellation/expiry and writer integration |
| `SigningCalendar` | All R and direct sources share one target calendar; independent 1/s, burst-4 session buckets; required spacing profile cannot exceed the established 4/s node budget; at most 256 budget aliases | Every alias belongs to an existing Reserved cache row; no separate job queue; actual signing occurs only after Consume, outside locks; apply the approved fair intake policy |
| `TargetCoordinator` | Private cache/calendar composition, consumed-only completion, exact retained-row generation, fixed public transcript inputs, paired alias cleanup and one composed local snapshot | Actual reader/session/role binding, approved intake/timing policy, external signing, pending/writer integration; this is not network admission |
| State snapshots | Immutable value snapshots published with `atomic.Pointer`; bounded ownership counters and pending/cache gauges | Service snapshot composition, RPC/SDK DTOs and `ResourceBreakdown` integration |

The priority source is a required typed function receiving a proven identity, ConnID, group and
session opening time. It runs outside state-owner mutexes. The current class-provider gap is
tracked in [session-v2-pin-classes.md](session-v2-pin-classes.md); these blocks do not implement that
store policy or change its limits. Group values reuse `domain.SourceGroup`; no competing /24 or
/48 grouping function is introduced. The future adapter must call the existing reached-address
/16 IPv4, /32 IPv6, loopback grouping, not trust an advertised endpoint.

The private F1 adapter binds captured genuine proof to the original transport gate and retains one canonical Session after actual accepted auth or CM post-setup registration. It is described below. Claimed identity, cached peer metadata and a Peer literal cannot create a Session. Ordinary nodes do not negotiate the overlay capability.

### Cache, copies and lifecycle

The signed body consists of `target`, checked `pubkey` and `sig`. Its transcript binds the retained
challenge. Across up to three source sessions the **signed body is identical**. The envelope ID is
local to each session/direction: requests with IDs 11, 22 and 33 receive those IDs respectively.
R retains the exact actual target Session/ID for reply correlation even though its capacity count is
shared by target identity. It also restores its source's original ID and copies the signed body without signature verification.
The initiator must verify the target signature against its retained target/challenge and session.
The state package accepts a completed body from its future local signer; it does not claim to have
performed that cryptographic operation.

Reserved jobs consume cache rows. Each gets an explicit deadline no later than the existing
six-second responder timeout; an expiry/cancel/eviction invalidates its opaque work token, even if
the same challenge is subsequently admitted again. Ready rows expire 60 seconds after completion. An ordinary close removes only active recipients
and their per-session quota charges. A fixed history of at most three ConnIDs remains with the row;
it neither holds closed TLS objects nor forgets a previously admitted copy. Closing the last
recipient preserves a Ready signed body and retains the Reserved generation. In the composed
coordinator, closing the original source before Consume makes that retained row Unsignable; it
cannot transfer its allowance to another recipient. After Consume, close preserves timely completion,
even with no recipients. A later permitted recipient waits for retained work or receives the cached
body without a new signature. A fourth historical session still refuses. Close is not a dedup reset.

Explicit cancellation of Reserved work invalidates its generation and releases active recipients,
but retains a Cancelled tombstone/history until the original signing deadline or legitimate cache
pressure eviction. While that row is retained, another work generation cannot be silently created. A Ready
row cannot be cancelled. Late completion of a cancelled/expired generation cannot modify a later
row with the same challenge. Cancelled rows, active references and history references are separately
visible in the bounded snapshot.
Duplicates of Reserved rows wait for that work and do not request another signature. A ready copy
is charged to its recipient's eight-row cap and does not request a signature reservation.

Eviction considers only bounded cache rows: oldest entry of the heaviest new group first, then
the youngest eligible level-2 session. Protected level-1 recipients are not evicted; a lower-priority
arrival cannot evict a higher-priority row. If the source's own eight-row cap is full, only a row
holding that source may free its place. If no eligible victim exists, admission refuses. This is
conservative bounded cache admission, not a completed fairness guarantee. No global group registry
or distinct-target memory claim is implemented here.

Pending cancellation, close and expiry release each entry once. Target close releases active session
references without removing challenge history or a completed answer. After close, only live recipients
contribute service priority; historical ConnIDs do not confer priority. Original source-group
attribution remains available for legitimate pressure eviction. The future adapter must fan out
a connection-close event to every owning block. Cache/pending snapshots are independent samples,
not a globally atomic Service snapshot.

### Inactive local target composition

`NewTargetCoordinator` requires a checked self public key, network, explicit signing profile and
monotonic start. It privately constructs one cache and one calendar; neither mutable subowner is
exposed. New work returns `LocalReservation`; duplicates return only WaitingExisting or ReadyCopy.
`TakeSignWork` consumes both actual budgets and returns a distinct `ConsumedSigningPermit` plus
immutable public network/target/challenge values. Every `Transcript()` builds a fresh byte slice.
The external caller signs after all locks are released. `identity.SignPayload` returns RawURL Base64;
the fixture decodes exactly 64 bytes and verifies the eventual answer with the normal protocol verifier.
The coordinator accepts signature bytes from its future local signer; it does not itself verify
arbitrary signature bytes. Fixed self/key/transcript inputs do not establish runtime caller admission.

Completion requires the exact current Reserved row in Consumed phase and a time **strictly before
the original signBy**, checked before replacing expiry with completion+60s. The standalone calendar's
existing equality-at-latest behavior is unchanged. Repeated Take/completion refuses. Cancellation
revokes pending or consumed authority, keeps spent credits and leaves the existing bounded tombstone.
Legitimate expiry or pressure eviction may admit a **fresh independently charged generation** of the
same challenge; every old reservation/permit remains stale. No retired-generation history is added.

An optional signing record lives on the existing cache row: AliasPending owns one calendar alias;
Consumed and Unsignable own none. Expiry, cancellation and eviction clear authority and recipients
before a detached row can be retained by caller-held handles. A fixed temporary batch collects at
most 256 retired aliases, drains with cache.mu released, and resets between cleanup phases. Evicted
aliases are cancelled **before** a replacement calendar Reserve. There is no second jobs map, queue,
worker, signer callback or new global registry. Existing cache recipients/history have at most 256×3
references; the session map is not claimed to have only 256 entries. Caller-held tokens and external
crypto scheduling are outside this owner-retained bound and have no resource-acceptance claim.

Reconciliation scans existing recipients/session references for already-closed Sessions, including
closure observed through another state owner. It uses the same idempotent close transition: retire
the original pending alias, drop closed TLS references, preserve Consumed completion, Ready body and
ConnID history. It introduces no new expiry or authority. Future production adapters must still fan
actual connection close to every owner; no such integration is installed here.

Lock order is coordinator.mu → cache.mu **or** calendar.mu → sessionState.mu (only via calendar).
Cache.mu is never held across a calendar call. No crypto, callback, I/O or worker executes under these
locks; authoritative priority callbacks run before entry. A composed immutable atomic snapshot is
published after cleanup, with actual calendar aliases equal to AliasPending rows. Its diagnostic
sequence saturates with an explicit flag and never gates authority or cleanup. Snapshot is the last
sample, not automatic freshness or AdmittedAtT. Bad-time Reserve/Take/Complete grant no authority;
Close/Cancel still do their exact normal cleanup and return ErrClock at the last valid sample time.
Bad-time Expire reconciles observed closures only, without time-based expiry. Close preserves Consumed;
explicit Cancel revokes it. A valid-time refusal may already have expired old rows.

Pair-window registries must use `PairKey`, so choosing another TLS link to the same target cannot
reset the source-session/target-identity gate. The future calendar registry must likewise have one
canonical owner per target identity; constructor target binding does not itself implement that
registry. Ordinary target-link close must not reset the identity's budget. Registry lifetime and
bounded global accounting still belong to the enclosing admission.

Calendars never refund projected tokens on cancellation; actual tokens spent on a failed write or
signature stay spent. This conservative append-only policy can refuse a request that another policy
could fit. It preserves admitted slots under later arrivals but does not establish maximal admission
or fair online intake. Exceeding a latest time refuses consumption; crypto and writes are external.

### Explicit proposals and remaining gates

`TimingProfile` requires positive target, network and runtime bounds whose sum is less than six
seconds. Unknown values refuse instead of becoming zero. `SigningProfile` requires explicit
node spacing of at least 250 ms, no extra node-wide burst, and an explicit reservation limit up to
256. These are **inputs to a proposed conservative algorithm**, not approved deployment defaults.
See [d1-admission-budget.md](d1-admission-budget.md). In particular, admission at R does not reserve
T's global budget and does not imply admission or an answer at T.

Before full PR-4/5: complete actual-path composition of accepted option A and approve timing profiles; implement degree/window and real-path composition of local
source/distinct-target accounting; specify duplicate source-ID lifetime beyond active pending entries;
wire the named-classifier/strict-codec gate in **both** dispatchers and `find_node` into the outbound
command limiter; bind actual proven connections; compose the local target owner with pending reservations and
rollback/close/late-answer paths; implement snapshot DTO/resource accounting; run the required
end-to-end scenarios and regression suite. Capability advertisement and socket writes remain
forbidden before the separate PR-6/S6/G2 release gates.

Tests use real v2 TLS handshakes and synthetic monotonic instants. They cover zero-proof refusal,
session/target/ID binding, copy IDs with an identical signed body, stale completion, bounded cache
and pending state across duplicate target links, closed-recipient history and in-flight cancellation, protected/heaviest-group eviction, four-oldest priority, exact release,
three-per-30-second boundaries, cancellation without token refunds, delayed actual bursts, and
contention among several R at one target signer. These are functional state tests, not Tor/network
measurements or a full 13b-D1 run.

### Inactive option-A core

A private candidate-reservation owner now composes the same512 pending rows, the accepted256+one
group ledger and at most 512 canonical target calendars. Combined actual/projected/remaining-alias
trials precede atomic commit; continuous locks serialize outgoing IDs. Fixed rounds, true per-target
four-oldest L2 capture, L1 cursor/watermark and lower cell/session RR are state-level mechanisms.
The private owner also composes atomic source128/10min and pair3/30s histories with final-slot
projected charges and jointly trialled actual budgets. This does not install real-path
source/pair/degree/role gates, a C5 production adapter, T admission or any network caller. Public standalone semantics remain limited. See the precise
[option-A core contract](find-node-option-a-core.md) for no-refund/dispatch retention, reset and
structural-bound limitations. Its outcomes are not full AdmittedAtR/T.


The added source history is one optional 128-row companion per participating canonical Session,
with two fixed 3-event histories/row and a nonzero-sized permanent owner token. One fixed ref in
an existing pending companion keeps a row through AwaitingAnswer and unresolved TerminalDispatch.
Only final row release drops the ref; Finish first retains max(old, completion+600s). Reuse needs
ref0, distinct expiry and both pair debts expired, with checked nonwrapping generation. No global
source map, extra queue or worker is added. Retained bound is O(128×participating Sessions+512),
not the group ledger's7952. Up to512 first companions can be prepared transiently at one seal;
caller-held ended Sessions, snapshots, transient allocations and full heap/CPU/RSS remain additional.
These are source-functional guarantees of an inactive core, without timing/resource admission.

### Inactive F1 proof-transport foundation

The private, unused FindNode factory composes a memory-only transport adapter. Ordinary constructors leave it nil and still advertise neither overlay capability. It installs no handler, admission, class provider, worker, signer or socket writer. Explicit-cap genuine TLS fixtures do not change production hello/welcome capabilities. A canonical `SessionSeed` captures actual local/remote negotiated facts immediately after Accept/Dial; copies share one consumption latch. Invalid Bind arguments do not consume it. The seed and Session retain identity proof values, not a full TLS ProvenSession or connection. End closes the one canonical Session without refund or I/O.

One `proofTransportLifetime.mu` orders original terminal, accepted/CM publication and the independent N-in companion. Raw R and decoded W, the registry and the outbound peer alias the SAME cell. An atomic provisional terminal latch plus bind-once CAS covers Read/Write/Close before the cell is attached; it cannot mint proof. End clears proof/socket/entry/peer/store chains before existing blocking diagnostics. Accepted and dialled datagrams capture an original terminal-only arrival capsule before the synchronous pipeline; outbound initial push captures its original owner before background scheduling. No current-ID/address lookup substitutes another transport.

Accepted publication follows actual auth. Outbound publication follows the CM generation-accepted callback, successful setup/registration and the legacy-pin recheck, immediately before SessionInitReady. It samples independent monotonic elapsed time AFTER owner-lock waits. The coverage is only **registered accepted and CM post-setup readers**; it excludes one-shot/unregistered readers and does not claim complete overlay admission or already-consumed CM Active. CM deactivation/stale-success refusal Ends the selected original session before later teardown diagnostics.

N-in remains listener-only, with the same original mandatory pin/incarnation, ten-minute continuous lifetime, source-group journal/debit, strict ACK and owed-work semantics. A dialler has no N-in companion. An N-in clock failure cannot mint its class or suppress a separately valid F1 publication. No limits, activation, resource admission or release gates change. One cell/optional companion per original transport and an on-demand O(current registered readers) snapshot are structural costs; TLS evidence before publication, snapshot allocations, external delayed handles and full process/heap costs remain additional.

## Русский

Это частичная реализация PR-4/5 в `internal/core/findnode` после отдельно проверенного кодека.
Production-код пакет не подключает. Он не регистрирует dispatcher, не объявляет capability,
не пишет кадры, не подписывает данные и не меняет pin. Это не завершённые PR-4/5, утверждение G2,
профиля времени или допуска выпуска D1.

Реализованы следующие ограниченные блоки:

- `Session` принимает настоящий `sessionv2.ProvenSession`, проверяет identity и согласованную
  find-node capability. Нулевой proof и отсутствие capability отказывают. Будущий adapter связывает
  единственный Session с фактическим reader/ConnID, временем открытия и существующим `node.sourceGroup`
  достигнутого адреса; объявленный endpoint и заявленная identity доказательством не являются.
- `PrioritySnapshot` требует authoritative-функцию классов K/N-sel/N-in, получает тип доказанной
  identity, ConnID, группу и возраст. Приоритет относится к сессии; четыре старейшие подходящие
  сессии получают уровень 2. Функция вызывается вне mutex владельца. Вызывающий обязан передать
  **все живые кандидаты данной цели**, а не только держателей кэша. Production C5 provider ещё
  отсутствует; классы из непрошитых metadata не выводятся, pin-политика и лимиты не меняются.
- `PendingTable` сохраняет точные source/target session, target, challenge и исходный ID; назначает
  отдельный исходящий ID без повторов/переполнения; ограничивает pending 16 на source session, 16 на identity цели суммарно по всем её TLS-сессиям,
  512 на узел и срок шестью
  секундами. Это только хранение/корреляция: role, command/input-rate, pair/distinct-target gates,
  справедливый допуск и запись в writer должны быть выполнены снаружи.
- `TargetCache` ограничен 256 Reserved/Ready-записями, восемью активными ссылками на сессию и историей не больше трёх ConnID на
  challenge. Дубликат той же сессии ответа не получает. Reserved-работа имеет явный срок не позже
  шести секунд и занимает прежний cache slot; Ready живёт 60 секунд после завершения. Поздний work
  token не восстанавливает отменённую, истёкшую или вытесненную запись, даже с тем же challenge.
- `PairWindow` требует `PairKey` из доказанной source Session и identity цели, общей всем её TLS-
  соединениям; хранит три timestamps одной такой пары, проверяет 3 за 30 секунд без
  attacker-keyed map. Private A-owner отдельно объединяет fixed source128/pair3 histories;
  реальная admission integration остаётся внешней.
- `ForwardCalendar` требует identity цели и моделирует общий для всех её TLS-соединений и уровней R→T бюджет 1/с, burst 4 и не больше 16 резервов.
  Явный latest time ограничивает допуск; отдельное списание по фактическому времени не позволяет
  опоздавшим работам собраться во всплеск больше четырёх.
- `SigningCalendar` делит бюджет T между всеми R и прямыми сессиями, сохраняя отдельные 1/с, burst 4
  на сессию. Требуются явный spacing-профиль не быстрее 4/с и предел не больше 256 ссылок на бюджет.
  Каждая ссылка должна соответствовать существующей Reserved-записи, дополнительная очередь работ
  не разрешается. Подпись выполняется снаружи лишь после Consume и без удержания mutex.
- `TargetCoordinator` неактивно связывает собственные cache/calendar: завершение только после
  фактического Consume, точная generation retained-записи, неизменяемые публичные данные транскрипта,
  совместная очистка alias и локальный atomic snapshot. Это не сетевой допуск; reader/role/session,
  политика приёма, pending/writer и внешняя подпись ещё не подключены.
- Счётчики и bounded gauges публикуются неизменяемыми value snapshots через `atomic.Pointer`.
  Service/RPC/SDK DTO и `ResourceBreakdown` пока не подключены.

Группы используют `domain.SourceGroup`: новой /24 или /48 функции нет. Будущий adapter применяет
существующие /16 IPv4, /32 IPv6 и loopback. Private F1 adapter связывает genuine proof с исходным transport и одним canonical Session после actual auth/CM post-setup registration. Обычные узлы overlay capability не согласуют. Claimed identity/Peer literal не создают proof, а сам конструктор не доказывает корректность ConnID, адреса и local node context.
См. [разрыв C5](session-v2-pin-classes.md).

Подписанное тело `target/pubkey/sig` у копий одного challenge **одинаково**. Envelope ID относится
к сессии/направлению: запросы 11, 22 и 33 получают свои ID. R сохраняет точные фактические target Session/ID для ответа, хотя квота относится к identity.
Он также возвращает исходный ID и копирует
подписанное тело без проверки подписи. Инициатор проверяет подпись по сохранённым target/challenge
и сессии. Пакет состояния получает тело от будущего локального signer, но не выдаёт это за
выполненную им криптографическую операцию.

Вытесняется старейшая запись самой тяжёлой новой группы, затем младшая подходящая сессия уровня 2.
Запись с получателем уровня 1 защищена; нижний уровень не вытесняет верхний. При заполнении восьми
мест источника освобождать можно только запись с его ссылкой. Нет подходящей жертвы — отказ.
Это консервативный cache admission, не доказательство полной справедливости. Глобальный реестр
групп и лимиты различных целей здесь не реализованы.

Закрытие снимает только активные ссылки и их per-session quota; отдельная история максимум трёх
ConnID остаётся в записи, не удерживая закрытые TLS-объекты. Даже закрытие последнего получателя
сохраняет Ready body и исходную Reserved generation. В coordinator закрытие исходной сессии до
Consume оставляет retained-запись Unsignable без передачи её бюджета другому получателю. После
Consume своевременное завершение сохраняется даже без получателей. Новый допустимый получатель
ждёт прежнюю работу либо получает кэш; четвёртая историческая сессия отказывает. Исторические ConnID не дают
приоритета; исходная группа сохраняется для допустимого вытеснения под давлением.

Явная отмена Reserved generation освобождает активных получателей, но оставляет Cancelled tombstone
и историю до исходного signing deadline либо допустимого pressure eviction. Пока запись сохранена, новую работу
с тем же challenge создать нельзя; Ready не отменяется. Позднее завершение старой generation не
меняет новую запись. Snapshot отдельно считает active/history references и Cancelled rows.

Реестр пар обязан использовать `PairKey`, а будущий реестр календарей — единственного владельца
на identity цели. Смена/закрытие её TLS-link бюджет не сбрасывает. Привязка конструктора к identity
сама такой реестр не реализует; его ограничение и lifetime остаются внешним admission.

Отмена, закрытие и expiry освобождают активные места однократно. Adapter обязан передать close во все
владельцы состояния. Снапшоты подсистем независимы и не являются общим атомарным snapshot Service.
Календари не возвращают projected-токены при отмене; фактически потраченные при ошибке записи или
подписи токены остаются потраченными. Append-only политика может отказать там, где другая вместила
бы запрос. Онлайн-приём и максимальная полнота допуска не доказаны.

### Неактивный локальный владелец цели

`NewTargetCoordinator` требует проверенный self public key, сеть, явный signing profile и монотонный
start. Он создаёт собственные cache/calendar, не выдавая их изменяемые указатели. Новая работа получает
`LocalReservation`, дубликат — только WaitingExisting либо ReadyCopy. `TakeSignWork` списывает оба
фактических бюджета и выдаёт отдельный `ConsumedSigningPermit` с публичными network/target/challenge.
Каждый `Transcript()` возвращает новые байты; подпись выполняется снаружи после освобождения locks.
Fixture декодирует RawURL Base64 результата `identity.SignPayload` в ровно 64 байта и проверяет
итоговый ответ обычным protocol verifier. Сам coordinator не проверяет произвольные signature bytes
и не выдаёт фиксированные self/key/transcript за runtime admission.

Complete требует текущую Reserved-запись в Consumed и время **строго раньше исходного signBy**,
до установки expiry=completion+60с. Старое standalone поведение календаря на равенстве latest не
меняется. Повторные Take/Complete отказывают. Cancel отзывает pending/consumed authority, не возвращает
потраченные кредиты и оставляет прежний bounded tombstone. Законные expiry/pressure eviction могут
создать **новую отдельно оплаченную generation** того же challenge; старые tokens навсегда stale.
Дополнительной истории завершённых generation нет.

Optional signing record находится в прежней cache row: AliasPending владеет одним alias, Consumed и
Unsignable — ни одним. Удаление очищает authority/recipients до удержания detached row внешним token.
Временный fixed batch до 256 alias очищается без cache.mu и обнуляется между фазами. Alias вытесненной
жертвы отменяется **до** нового calendar Reserve. Нет второй jobs map, очереди, worker, signer callback
или глобального registry. Recipients/history ограничены 256×3 ссылками; session map не объявляется
ограниченной 256 элементами. Внешние tokens и crypto scheduling не входят в owner-retained bound и
не считаются прошедшими resource acceptance.

Reconciliation сканирует существующие session/recipient ссылки и применяет тот же idempotent close
к уже закрытым Sessions, в том числе замеченным через другого владельца. Исходный pending alias
снимается, closed TLS references удаляются; Consumed completion, Ready body и ConnID history сохраняются.
Это не новый timeout или источник authority. Production adapter всё ещё обязан раздать настоящий close
всем владельцам; такой интеграции здесь нет.

Порядок locks: coordinator.mu → cache.mu **или** calendar.mu → sessionState.mu (только через calendar).
Calendar никогда не вызывается под cache.mu. Под locks нет crypto/callback/I/O/worker; priority callback
выполнен заранее. Общий локальный atomic snapshot публикуется после cleanup; число физических calendar
alias равно AliasPending rows. Диагностический sequence насыщается с явным флагом и не управляет
authority/cleanup. Snapshot — последний sample, не автоматическая свежесть и не AdmittedAtT. При плохом
времени Reserve/Take/Complete не выдают authority; Close/Cancel выполняют обычный exact cleanup и
возвращают ErrClock с последним валидным sample time. Плохой Expire убирает только уже закрытые ссылки,
без time expiry. Close сохраняет Consumed, Cancel отзывает. Отказ с валидным временем уже мог истечь старые rows.

`TimingProfile` требует положительные границы target/network/runtime с суммой меньше шести секунд.
Неизвестная граница не становится нулём. `SigningProfile` требует явный интервал не меньше 250 мс,
без дополнительного узлового burst, и явный предел резервов до 256. Это **параметры предлагаемого
консервативного алгоритма**, а не утверждённые deployment defaults. Допуск R не резервирует бюджет
T и не гарантирует её ответ. См. [предложение допуска](d1-admission-budget.md).

До полного PR-4/5 остаются: полная композиция принятого варианта A на реальных путях и
утверждение профилей времени; degree/window и реальная source/distinct-target
композиция; срок запрета повторного source ID после завершения active pending; гейты named classifier /
strict codec **в обоих** диспетчерах и outbound command limiter; настоящая привязка proven connection;
композиция локального target owner с pending, ошибками writer, close и late answers; DTO/resource accounting;
end-to-end сценарии и полный regression. До PR-6/S6/G2 capability и socket writes не включаются.

Тесты используют настоящие v2 TLS-handshake и синтетическое монотонное время. Проверяются proof,
session/target/ID binding и общий предел по нескольким TLS-link цели, сохранение истории при
закрытии/отмене, одинаковое подписанное тело с разными ID, stale completion, все bounded
caps, защищённое/heaviest-group вытеснение, четыре старейшие сессии, точное освобождение, граница
3/30с, отмена без возврата токенов, опоздавший burst и конкуренция нескольких R за signer T.
Это функциональные тесты состояния, не замеры Tor/сети и не полный прогон 13b-D1.

### Неактивный core варианта A

Private candidate-reservation owner объединяет те же512 pending rows, принятый ledger256+one и
максимум512 canonical target calendars. Совместный actual/projected/remaining-alias trial идёт до
атомарного commit с непрерывной сериализацией outgoing ID. Fixed rounds, per-target четыре L2 из
полного membership, L1 cursor/watermark и RR cell/session — локальные механизмы состояния.
Private owner также атомарно объединяет source128/10min и pair3/30s с final-slot projected
списанием и совместным actual trial. Реальные source/pair/degree/role gates, C5 production
adapter, T admission и network caller не подключены.
Public standalone semantics прежние. [Точный контракт](find-node-option-a-core.md) описывает no-refund,
удержание dispatch, reset и пределы структурных оценок. Outcomes не являются полным AdmittedAtR/T.


Source history добавляет optional 128-row companion участвующей canonical Session: две fixed 3-event
истории на row и постоянный nonzero-sized owner token. Один fixed ref прежней pending companion
сохраняет row в AwaitingAnswer и unresolved TerminalDispatch. Только final release снимает ref;
Finish сначала сохраняет max(old, completion+600s). Reuse требует ref0, distinct expiry, истечения
обоих pair debt и checked generation без wrap. Global source map, дополнительной queue/worker нет.
Retained bound O(128×участвующие Sessions+512), а не7952 group ledger. На seal временно готовится
до 512 первых companions; caller-held ended Sessions, snapshots, transient allocations и полная
heap/CPU/RSS стоимость остаются дополнительными. Это локальные source-functional гарантии
неактивного core, без timing/resource admission.

### Неактивная proof-transport основа F1

Private неиспользуемая FindNode factory собирает memory-only transport adapter. Обычные конструкторы оставляют его nil и по-прежнему не объявляют обе overlay capability. Handler, admission, class provider, worker, signer и socket writer не устанавливаются. Genuine TLS фикстуры с явно согласованными capability не меняют production hello/welcome. Canonical `SessionSeed` сразу после Accept/Dial захватывает настоящие согласованные факты; копии разделяют одноразовый latch. Неверные Bind arguments его не расходуют. Seed и Session удерживают identity proof values, а не полный TLS ProvenSession/connection. End закрывает единственный canonical Session без refund и I/O.

Один `proofTransportLifetime.mu` упорядочивает исходный terminal, accepted/CM publication и независимый N-in companion. Raw R, decoded W, registry и outbound peer ссылаются на ТОТ ЖЕ cell. Atomic provisional terminal latch и bind-once CAS покрывают Read/Write/Close до attach и не создают proof. End очищает proof/socket/entry/peer/store chains ДО прежних блокирующих diagnostics. Accepted и dialled datagram захватывают исходную terminal-only arrival capsule до синхронного pipeline; outbound initial push захватывает исходного owner до background scheduling. Поиск текущего ID/address не подставляет другой transport.

Accepted publication следует за настоящим auth. Outbound publication идёт после CM generation-accepted callback, успешных setup/registration и legacy-pin recheck, непосредственно перед SessionInitReady. Независимый monotonic elapsed читается ПОСЛЕ ожиданий owner locks. Coverage — только **registered accepted и CM post-setup readers**: one-shot/unregistered readers исключены; это не полная overlay admission и не утверждение уже обработанного CM Active. CM deactivation/stale-success refusal завершает выбранную исходную session до последующих teardown diagnostics.

N-in остаётся listener-only с прежними исходными mandatory pin/incarnation, десятью минутами непрерывного lifetime, source-group journal/debit, strict ACK и owed-work semantics. Dialler не получает N-in companion. Отказ N-in clock не даёт классификацию и не запрещает независимо допустимую F1 publication. Лимиты, activation, resource admission и release gates не меняются. Один cell/optional companion на исходный transport и on-demand O(current registered readers) snapshot — структурные издержки; допубликационные TLS evidence, snapshot allocations, внешние delayed handles и полная стоимость heap/process учитываются дополнительно.


### Private bounded C5 observation / Private C5-наблюдение

The unused private observer uses explicit copied MaxMembers/MaxRegistryVisits inputs, complete-or-refused capture, genuine current F1 owner rechecks and current strict classes. Target links count toward M and are excluded from requester oldest-four selection. This historical observation is not AdmittedAtR/T, full online composition or a C3-immune permit. See [bounded class reader](find-node-priority-adapter.md). Deployment profiles, resources, G2 and activation remain separate.

Private observer читает настоящие F1 members и strict C5 через явные M/V; переполнение отказывает целиком. Это наблюдение, без допуска R/T, гарантии доступности или жёсткой задержки mutex/CPU.
