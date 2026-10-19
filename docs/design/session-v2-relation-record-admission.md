# Received-record admission for Relation pins — owner decision

## English

**Owner decision, 2026-10-19: approved for implementation and light functional checks.**
The owner approved the aggregate rolling limit, 60-second startup wait, bounded
queue and original seven-day candidate deadline below. Activation, G2, heavy
measurements and D1 release remain separate. The inactive R1a storage foundation
does not itself supply the live producer or admission implementation.

The existing allocation limits remain unchanged: 20,000 unique identities in
total, including K and unclassified pins; 8,192 non-absent Relation allocations,
including unconfirmed allocations and identities also holding K. K has no own
class quota. Adding K does not remove an R allocation from its count. N-sel
accounting remains unchanged. No eviction or automatic legacy fallback is added.

### Rate and restart choice

The approved rule is one aggregate trailing 60-second window allowing at most
30 non-K record admissions, with an initial **60 seconds of observed monotonic
uptime after every startup** before any charged admission. After that wait, up
to 30 may be admitted immediately. Exactly 60-second-old admissions leave the
half-open window. Wall-clock jumps do not create allowance.

The startup wait applies to fresh, cleanly restarted and crash-restarted nodes;
there is no inference from old pins or unverified downtime. It delays charged
record admission only. Ordinary key lookup/import, actual TLS proof publication,
already-installed protection and K's separately exempt record-rate path remain
independent. The guarantee assumes sequential owners of one node: the previous
owner has stopped before the new owner starts. It does not combine overlapping
replicas or cloned nodes into one distributed allowance.

Without that wait or a persistent history, a process can admit 30 records,
restart, then admit 30 more within the same real minute. An explicit alternative
is a rolling limit **per Service incarnation**, resetting at restart; that has
no rolling guarantee across restarts. A persistent history/time design is a
third, separate option, requiring its own failure model and implementation.
Neither alternative is silently adopted here.

### Shared queue and accounting

- One queue contains at most 256 candidates **including Processing**. Refuse a
  new candidate when full; do not evict another candidate or an installed pin.
- Use FIFO within a charged class and deterministic round-robin among nonempty
  charged classes. The first live producer is R only, so its order is FIFO.
  Service-order fairness does not reserve places for later classes in a full
  queue, or promise latency under arbitrary I/O or future exempt K work.
- K bypasses the charged **rate** allowance. This exemption grants no extra
  queue or capacity, cannot evict queued work and does not authorize unbounded
  execution. A future K admission consumer needs reviewed bounded ownership and
  strict persistence; the initial R1b consumer handles charged R only. Post-stage
  K2 earned-completion tickets are a separate lifecycle, not these admission candidates.
- Charge one admission only when a verified, current, uncancelled candidate
  actually stages protection evidence, after owner, class, capacity, deadline
  and revision checks. Queue reservation, rejection and expiry do not charge.
- A fresh accepted record from a **new valid attempt** costs one even for an
  existing R allocation or identical record bytes. Repeated settlement of the
  same exact label/claim costs zero because it is consumed once. Strict retry
  also costs zero. No growing digest history is introduced.
- A later strict-write failure does not refund a staged admission or allocation.
  Protection remains counted/unconfirmed; ordinary persistence is not a durable
  ACK. Key-resolution success remains distinct from pin confirmation.

### Approved lifetime for the solicited Window producer

Both the verified answer and accepted class-queue outcome must qualify within
the original 60-second lookup-attempt window. Only that exact claim, with its
original pre-attempt pin-owner observation, may transfer to the record queue.
Existing R0 soft diagnostics cannot be replayed as admission authority.

The decision lets that queued candidate wait until the **original resolution's
creation time plus its existing seven-day task lifetime**, without rebasing on
redraw, join, success, enqueue or a replacement resolution. Queue wait can exceed
60 seconds: 256 waiting candidates can require nine rate batches, up to 540
seconds of rate waiting, plus any remaining startup wait and storage delays.
This is an approved authority-residence rule, not an existing R0 guarantee or a
hard service deadline. Signed identity records have no record TTL; do not invent
one from informational `issued_at`.

Explicit matching action cancellation, runtime shutdown, deadline expiry or a
stale pin-owner observation revokes a candidate before stage, including after
ordinary lookup success removed its reason. Transfer and cancellation retain
one exact pending owner; no completed-action history or target-only rebind is
created. After stage, cancellation does not remove the installed protection.
Queued work is not replayed across restart. General unsolicited/K2 residence is
separate work; it does not inherit this seven-day rule.

Success, redraw, join and repeated addition cannot extend the original deadline.
Before stage, recheck the exact claim, original action, owner, cancellation and
deadline, including after lock waits. K's rate exemption does not authorize
unbounded execution. A candidate may occupy one bounded queue slot for days when
storage is delayed; seven days is the chosen policy, not a processing-time bound.
This document records the owner's decision, not an implementation or test result.

## Русский

**Решение владельца от 2026-10-19: утверждены реализация и лёгкие функциональные проверки.**
Приняты общий скользящий лимит, ожидание 60 секунд после старта, ограниченная очередь
и исходный семидневный срок кандидата. Активация, G2, тяжёлые измерения и выпуск D1
остаются отдельными. Неактивный фундамент R1a сам по себе не подключает live producer
и не реализует допуск.

Сохраняются пределы владельца: всего 20 000 уникальных identity, включая K и
неклассифицированные pin; 8192 non-absent Relation allocations, включая staged/
unconfirmed и identity, также имеющие K. У K нет собственной классовой квоты;
добавление K не вычитает R allocation. Учёт N-sel не меняется. Вытеснения и
автоматического legacy fallback нет.

### Лимит и перезапуск

Принято общее trailing window 60 секунд, не больше 30 non-K record
admissions, и **60 секунд наблюдаемого монотонного uptime после каждого старта**
до первого charged admission. Затем разрешён burst до 30. Событие ровно
60-секундной давности выходит из полуоткрытого окна; wall-clock не даёт токены.

Ожидание применяется при первом старте, штатном рестарте и восстановлении после
сбоя. Pin-файл и непроверенный downtime его не сокращают. Это ожидание только
charged record admission: обычные lookup/import ключей, настоящий TLS proof,
установленная защита и отдельное rate-исключение K не блокируются. Гарантия
предполагает последовательных владельцев одного узла: прежний остановлен до
старта нового. Перекрывающиеся реплики/клоны не получают общий распределённый
бюджет.

Без ожидания или постоянного журнала можно принять 30, перезапуститься и принять
ещё 30 в ту же реальную минуту. Явная альтернатива — rolling limit только внутри
одного запуска со сбросом при рестарте; across-restart гарантии у неё нет.
Постоянный журнал/время — третий отдельный вариант со своей моделью отказов и
реализацией. Ни один вариант не считается принятым автоматически.

### Очередь и списание

- Одна очередь максимум 256 кандидатов **вместе с Processing**. При заполнении
  новый получает отказ, без вытеснения другого кандидата или установленного pin.
- Внутри класса FIFO, между непустыми charged classes — deterministic round-robin.
  Первый live producer только R, для него это FIFO. Это порядок обслуживания,
  не резерв мест в полной очереди и не обещание срока при произвольном I/O/K.
- K освобождён от charged **rate**, без дополнительной очереди, вместимости,
  вытеснения ожидающих или неограниченного исполнения. Будущий K admission consumer
  требует проверенных bounded ownership и strict persistence; первый R1b consumer
  обслуживает только charged R. Earned-completion tickets K2 после stage — другой
  lifecycle, а не кандидаты этой очереди допуска.
- Одно списание — при действительном stage проверенной/current/неотменённой
  protection evidence после owner/class/capacity/deadline/revision проверок.
  Место в очереди, отказ и expiry ничего не списывают.
- Новый допустимый attempt с новой принятой записью стоит 1 даже для уже R и
  одинаковых bytes. Повтор settlement того же exact label/claim стоит 0, как
  и strict retry. Growing digest history не создаётся.
- Ошибка strict write после stage не возвращает admission или место класса.
  Защита учтена/unconfirmed; обычный save не даёт durable ACK. Успех получения
  ключей не означает подтверждение pin.

### Срок для solicited Window producer

Verified answer и accepted class-queue outcome должны квалифицироваться внутри
исходного 60-секундного attempt window. В очередь передаётся только exact claim
с исходным pre-attempt pin-owner observation. Старый soft diagnostic R0 не
становится authority задним числом.

Разрешено ждать до **исходного createdAt resolution + существующие 7 суток
task lifetime**, не пересчитывая срок по redraw/join/success/enqueue/новому B.
Очередь может ждать дольше 60 секунд: 256 кандидатов требуют до девяти rate
batches, то есть до 540 секунд rate waiting, плюс оставшийся startup wait и
задержки хранения. Это утверждённое правило residence authority, а не
прежняя гарантия R0 или жёсткий срок обслуживания. У signed identity record нет
TTL; из информационного `issued_at` он не выводится.

Явная отмена того же действия, shutdown, deadline или stale owner observation
отзывают candidate до stage, в том числе после обычного Success и удаления
reason. Transfer/cancel сохраняют одного exact pending owner без growing
history и target-only rebind. После stage отмена pin не снимает. Очередь после
рестарта не воспроизводится. General unsolicited/K2 residence согласуется
отдельно и не наследует 7 суток.

Success, redraw, join и повторное добавление не продлевают исходный срок. Перед
stage повторно проверяются exact claim, исходное действие, owner, отмена и deadline,
в том числе после ожидания locks. Исключение K из rate не разрешает неограниченное
исполнение. При задержке хранения кандидат может занимать одно место ограниченной
очереди несколько суток; семь суток — выбранная политика, не граница времени обработки.
Файл фиксирует решение владельца, а не завершённую реализацию или результаты тестов.
