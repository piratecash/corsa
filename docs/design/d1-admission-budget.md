# D1: aggregate admission and conditional deadlines

## English

### Owner decision, 2026-10-18

The owner selected aggregate admission, following the [deadline counterexample](d1-forwarding-fairness.md).
Keep the proposed resource limits until changes are justified. Requests exceeding an aggregate budget
may be refused. For admitted requests, derive a checkable waiting-time bound; receiving an answer is
conditional on target admission/service and explicit network-delay bounds. Admission must be fair
between level-1 sessions and account for tokens and queue places used by lower levels. Remove the
unconditional claim that every per-session-legal request is served within six seconds. This narrows
the guarantee. It authorises neither G2 nor activation.

The formulas and scheduling details below are a **proposal for review before G2**. The illustrative
delay bounds are test inputs, not approved network or Tor parameters. No runtime default or wire
behaviour is changed by this document or its reference program.

On 2026-10-19 the owner approved [bounded intake option A](d1-bounded-intake-proposal.md) for inactive
implementation and light checks: 256 named groups plus one overflow, H≤7952, one provisional per
session per node round and atomic shared debit only at admission. The accepted sticky/overflow/H
limitations do not guarantee L1/L2 availability or complete resistance to budget monopolization.
Numerical timing profiles, full real-path composition, resource acceptance, G2 and activation remain
separate. The six- and twelve-second values below remain proposed timing examples.

### Events and existing limits

Distinguish `AdmittedAtR` (forwarding reservation), `AdmittedAtT` (signing reservation) and `Answered`.
The proposed responder deadline remains 6 s and the initiator deadline 12 s. R-to-T forwarding stays
at 1/s, burst 4, with at most 16 pending per target, 16 per source session and 512 per node. The
source-session/target limit remains 3 per 30 s. Target signatures remain at most 4/s per node and
1/s, burst 4 per incoming session. All input-rate and distinct-target gates still apply.

An R reservation promises local forwarding under its recorded scheduling assumptions; it cannot
reserve T's global signing budget. Other responders and direct callers can consume that budget.
Four responders each sending 1/s already consume T's entire 4/s. Merely calling T "available" does
not establish a signing-service bound.

### Forwarding admission

For a request received by R at monotonic time `a`, propose the terminal event **R has handed the
answer to the original session's writer**. Define:

```text
deadline      = a + 6 s
latestForward = deadline - deltaTarget - deltaNetwork - deltaRuntime
```

`deltaTarget` bounds receipt at T through signature readiness, **conditional on admission at T**.
`deltaNetwork` bounds R-to-T plus T-to-R delivery. `deltaRuntime` includes R scheduling, processing,
any admission-round delay, and writer handoff; delivery from R to A is separately covered by the
initiator's conditional 12-second deadline. The bounds must have explicit profiles and evidence.
An unknown bound does not silently become zero. If no forwarding slot fits, refuse admission.

Simulate the real 1/s, burst-4 bucket and **all existing reservations**. Atomically reserve the next
legal slot `s` only if `s <= latestForward` and all existing pending/rate/target limits pass. Queue
space alone is not permission to admit. Later arrivals cannot move an admitted level-1 request past
its promised slot. Crypto and socket writes run outside the state owner's lock; local scheduling
overrun is observable and invalidates the corresponding timing assumption.

For an otherwise empty calendar, current tokens `0 <= beta <= 4` and time budget
`W = latestForward - now >= 0`, an upper bound is:

```text
Nadmit <= min(floor(beta + (1/s)*W), freeTargetPlaces,
              freeNodePlaces, eligible source places)
```

With reservations, simulate their actual times instead of subtracting only their count. Tokens
already spent on lower levels cannot be refunded. A conservative first implementation may keep all
existing reservations; alternatively, it may evict only unsent lower-level reservations under the
agreed eviction policy and recalculate future slots. An evicted lower-level request has lost
admission; it is counted explicitly. Level-1 promises are retained.

### Fairness boundary

Level-1 admission rounds give each nonempty proven source session one turn before its second turn;
the cursor persists between rounds. Session, not source group or claimed identity, is the fairness
key. Servicing an already admitted queue fairly does not repair unfair admission.

An online implementation needs a **bounded intake/round policy** before this becomes a complete
contract. It must state when a round closes, account for its delay in `deltaRuntime`, use existing
16/512 pending caps rather than an unbounded staging queue, and define refusal when even the intake
cannot fit. An accepted level-1 promise cannot be revoked to make room for a later session. Fairness
cannot mean guaranteeing slots to as-yet-unseen arrivals. The owner approved the bounded intake
structure in option A; its implementation, complete composition and numerical timing profile remain
to be verified before G2.

### Target admission

T must reserve a slot satisfying the intersection of its node-wide signing budget, the incoming
session bucket, existing signing reservations and a deadline:

```text
latestSignature = receivedAtT + deltaTarget - targetRuntimeReserve
```

A conservative **proposal** is a node-wide signer spaced by at least 250 ms, with no extra node-wide
burst; the existing per-session burst remains 4. No fit means refusal before cryptographic work.
Reserved/Ready states may use the existing bounded dedup capacity (256/node, 8/session); they do not
justify a new uncounted queue. Cached answers need no new signature, while duplicate/copy limits
still apply. Fairness and contention with other R and direct clients must be checked at T too.

Without a new reservation acknowledgement on the wire, `AdmittedAtR` is not `AdmittedAtT`.
A saturated T can refuse a request R admitted: the answer guarantee's target-admission assumption
then failed. Neither local admission nor a test with an idle T proves unconditional discoverability.

### Historical group-accounting conflict and selected resolution

Before the 2026-10-19 decision, the draft's separate group-accounting rule said
both that at most 256 named groups are tracked and that groups containing level-1/2 sessions are always
included. More than 256 sessions from distinct groups, each older than ten minutes, satisfy the
level-2 age rule even though only four receive service priority for one target. Including all their
groups exceeds the stated registry bound. A bound on service priority is not a bound on this
metadata. Selected [option A](d1-bounded-intake-proposal.md) resolves the conflict with 256 named
groups plus one shared overflow cell and H≤7952. Session priority survives in overflow, but an
independent physical-group minimum does not. Initial named groups can retain an advantage and hard H
may refuse below-share requests. These accepted limitations still require implementation, complete
composition and real-path validation; they do not authorize larger resource limits or activation.

### Executable design witness

Run [d1_admission_reference.py](d1_admission_reference.py) with Python 3.9 or later. It uses exact
rational arithmetic and synthetic bounds `deltaTarget=1s`, `deltaNetwork=1s`, `deltaRuntime=0.25s`:

- `W=3.75s`: full bucket admits at most 7 requests (four now, one at 1, 2, 3 s); an empty bucket admits 3.
- A legal batch of 10 sessions times 3 calls admits 7 different sessions and refuses 23 calls;
  persistent round rotation prevents always choosing the same seven sessions.
- Lower-level reservations and spent tokens reduce available service; fractional tokens and deadline
  boundaries are replayed across 4,125 parameter combinations.
- Signing reservations satisfy both the 250-ms node spacing and the per-session token bucket.

This is a reference for the arithmetic and batched round-robin, **not** production PR-4, a bounded
online intake implementation, a network measurement or G2 acceptance. Its calendar is conservatively
append-only: new reservations follow the last prior reservation, without filling earlier gaps, so
it does not prove maximal admission. Rotation is tested for a fixed set of ten sessions; changing
membership and reconnects require separate online-policy tests. Before G2: approve timing
profiles, implement and validate selected A with full policy composition; test the maximum legal burst against the real implementation; include
lower-level occupancy, competing R, target saturation, cancellation, close, late answers and exact
release of slots. The [release sequence](../release.md) and external admission checks remain.

## Русский

### Решение владельца от 2026-10-18

После [контрпримера срока](d1-forwarding-fairness.md) владелец выбрал общий бюджет допуска.
Предлагаемые ресурсные лимиты сохраняются до обоснования изменений. Превышающие общий бюджет
запросы могут получать отказ. Для принятых запросов нужна проверяемая граница ожидания; получение
ответа условно и требует допуска/обслуживания на цели и явных границ задержки сети. Допуск должен
быть справедливым между сессиями уровня 1 и учитывать токены и очередь нижних уровней. Безусловное
обещание обслуживания всех разрешённых по сессиям запросов за шесть секунд снимается. Это сужение
гарантии; решение не утверждает G2 и не разрешает активацию.

Формулы и алгоритмические детали ниже — **предложение на ревью до G2**. Числа задержек в примере —
входы теста, а не утверждённые параметры сети или Tor. Документ и эталон не меняют дефолтов и провода.

2026-10-19 владелец утвердил [вариант A ограниченного допуска](d1-bounded-intake-proposal.md) для
неактивной реализации и лёгких проверок: 256 именованных групп и одно переполнение, H≤7952, один
provisional на сессию за общий раунд узла и атомарное списание общей квоты только при допуске.
Принятые ограничения sticky/overflow/H не дают гарантированной доступности L1/L2 или полной защиты
от монополизации бюджета. Численные временные профили, полная композиция на реальных путях,
ресурсная приёмка, G2 и активация отдельны. Шесть и двенадцать секунд ниже остаются примерами.

### События и прежние лимиты

Различаются `AdmittedAtR` (резерв пересылки), `AdmittedAtT` (резерв подписи) и `Answered`.
В предложенном численном примере таймаут ответчика — 6 с, инициатора — 12 с; эти профили не утверждены.
R пересылает к T 1/с, burst 4; ожидающих не больше
16 на цель, 16 на сессию источника и 512 на узел. На пару сессия-источник/цель остаётся 3 за 30 с.
Новые подписи цели — не больше 4/с на узел и 1/с, burst 4 на входящую сессию. Входные лимиты частоты
и различных целей также действуют.

Резерв R обещает локальную пересылку при записанных предпосылках планирования; он не резервирует
общий подписной бюджет T. Его расходуют другие R и прямые запросы. Четыре R с потоком по 1/с уже
занимают все 4 подписи/с цели. Одного слова «доступная» для границы обслуживания T недостаточно.

### Допуск пересылки

Для запроса, полученного R в монотонное время `a`, предлагается конечное событие **R передал ответ
в writer исходной сессии**:

```text
deadline      = a + 6 s
latestForward = deadline - deltaTarget - deltaNetwork - deltaRuntime
```

`deltaTarget` ограничивает путь от получения запроса T до готовности подписи **при условии допуска
на T**. `deltaNetwork` ограничивает доставку R→T плюс T→R. `deltaRuntime` включает планирование R,
обработку, задержку раунда допуска и передачу writer; доставка R→A отдельно входит в условный
таймаут инициатора 12 с. Границы требуют явного профиля и подтверждения. Неизвестная граница не
превращается молча в ноль. Не помещается ни один слот — отказ допуска.

R моделирует фактическое ведро 1/с, burst 4 и **все уже принятые резервы**. Следующий допустимый
слот `s` резервируется атомарно только при `s <= latestForward` и прохождении остальных лимитов.
Одного места в очереди недостаточно. Новые запросы не сдвигают принятому уровню 1 обещанный слот.
Криптография и сокетные записи идут вне мьютекса владельца состояния; локальное опоздание наблюдаемо
и означает нарушение соответствующей предпосылки времени.

Для пустого календаря, текущих токенов `0 <= beta <= 4` и бюджета времени
`W = latestForward - now >= 0` верхняя граница:

```text
Nadmit <= min(floor(beta + (1/s)*W), свободные места цели,
              свободные места узла, допустимые места источников)
```

При существующих резервах моделируются их фактические времена, а не только вычитается число записей.
Потраченные нижними уровнями токены не возвращаются. Консервативная первая реализация может
сохранять все резервы; другой вариант — вытеснять только ещё не отправленные резервы нижних уровней
по принятой политике и пересчитывать будущие слоты. Потеря допуска нижним уровнем считается явно.
Обещания уровня 1 сохраняются.

### Граница справедливости

В раунде допуска уровня 1 каждая непустая доказанная сессия получает один ход до второго хода
других; курсор сохраняется между раундами. Ключ справедливости — сессия, а не группа или заявленная
identity. Справедливое обслуживание уже принятой очереди несправедливый допуск не исправляет.

До полного контракта нужна **ограниченная политика приёма и закрытия раунда** в онлайн-реализации:
срок раунда входит в `deltaRuntime`; используются существующие пределы 16/512 без неограниченной
подготовительной очереди; невозможность даже предварительного приёма означает отказ. Принятое
обещание уровня 1 не отзывается ради более поздней сессии. Справедливость не может гарантировать
места ещё не появившимся запросам. Структура ограниченного допуска утверждена в A; реализация,
полная композиция и численный профиль времени ещё требуют проверки до G2.

### Допуск цели

T резервирует слот в пересечении узлового бюджета подписи, ведра входящей сессии, прежних резервов
и дедлайна:

```text
latestSignature = receivedAtT + deltaTarget - targetRuntimeReserve
```

Консервативное **предложение** — интервал узлового signer не меньше 250 мс, без дополнительного
узлового burst; прежний burst сессии 4 сохраняется. Нет слота — отказ до криптографии. Состояния
Reserved/Ready можно разместить в прежнем дедупе (256/узел, 8/сессию), без неучтённой очереди.
Готовый кэшированный ответ новой подписи не требует, но ограничения копий сохраняются. Справедливость
и конкуренция других R и прямых клиентов отдельно проверяются на T.

Без нового wire-подтверждения резерва `AdmittedAtR` не означает `AdmittedAtT`. Перегруженная T может
отвергнуть принятый R запрос: условная предпосылка допуска на цели не выполнена. Ни локальный допуск,
ни тест с незагруженной T не доказывают безусловную находимость.

### Историческое противоречие учёта групп и выбранное решение

До решения 2026-10-19 отдельное правило черновика
одновременно ограничивало число именованных групп 256 и всегда включало группы с сессиями
уровней 1–2. Более 256 сессий из разных групп, каждая старше десяти минут, проходят возрастное
правило уровня 2, хотя приоритет обслуживания к одной цели получают только четыре. Учёт всех их
групп превышает заявленную границу реестра. Ограничение приоритета обслуживания не ограничивает
эти метаданные. Выбранный [вариант A](d1-bounded-intake-proposal.md) разрешает противоречие:
256 именованных групп и одна общая overflow-ячейка, H≤7952. Приоритет сессии сохраняется,
но отдельный минимум физической overflow-группе не обещан. Первые named-группы могут сохранять
преимущество; hard H может отказать ниже текущей доли. Принятые ограничения ещё требуют реализации,
полной композиции и проверки на реальных путях; увеличения ресурсов и активации они не разрешают.

### Исполняемый эталон проекта

[d1_admission_reference.py](d1_admission_reference.py) запускается Python 3.9 или новее. В нём точная
рациональная арифметика и синтетические границы `deltaTarget=1с`, `deltaNetwork=1с`, `deltaRuntime=0,25с`:

- `W=3,75с`: полное ведро допускает 7 запросов (четыре сразу, по одному через 1, 2, 3 с), пустое — 3.
- Допустимая пачка 10 сессий по 3 вызова даёт 7 допусков разным сессиям и 23 отказа; ротация между
  раундами не позволяет всегда выбирать одни и те же семь сессий.
- Резервы и потраченные токены нижних уровней уменьшают доступное обслуживание; дробные токены и
  границы сроков перепроверяются на 4 125 комбинациях параметров.
- Резервы подписи соблюдают и интервал узла 250 мс, и ведро сессии.

Это эталон арифметики и пакетного round-robin, **не** production PR-4, не ограниченный онлайн-приём,
не сетевой замер и не прохождение G2. Календарь консервативен: новые резервы добавляются после
последнего прежнего, без заполнения ранних свободных окон; максимальная полнота допуска не доказана.
Ротация проверена для постоянного набора десяти сессий; изменение состава и переподключения требуют
отдельных тестов онлайн-политики. До G2 нужны утверждённые профили времени, реализация и проверка A с полной композицией,
тест максимального всплеска на настоящей реализации, нижние уровни, конкурирующие R, насыщение T,
отмена, закрытие, поздний ответ и однократное освобождение мест. [Порядок выпуска](../release.md)
и внешние условия допуска сохраняются.
