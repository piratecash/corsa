# T5c functional timeline executor / Функциональный исполнитель T5c

## English

This increment adds a test-only, serial executor of the headless stand plan.
It is a functional lifecycle check, not a resource measurement or T5 completion.
The executor lives in `internal/core/node/*_test.go`. Shared typed lifecycle,
observation and user-traffic ports live in `internal/testutil/loadstand`.
Production node startup and measurement wiring are unchanged.

Plan version **3** originates DM and global-topic gossip events only at eligible
SDK edges. The DM probability applies per eligible edge. Version 1/2 digest
fixtures remain intact; version 3 gets new fixtures and rate expectations.
Version 3 workloads are not comparable with old full-node-origin workloads.
The executor refuses version 1/2 and unsupported roles before launching any
member; gossip additionally requires the ordinary SDK publisher and FULL observer
ports. [Global publication](sdk-global-publication.md) uses unsigned global claims,
local sender SQLite persistence and separate FULL runtime replica observations. It never substitutes
raw `sendMessage`/`publishNotice` frames or foreign signing keys.

The functional hybrid topology contains two real in-process full services with
reciprocal planned bootstrap and two real sandboxed SDK child processes. SDK
children have their own identity, DesktopClient and SQLite, and no peer listener.
Every bootstrap is nonempty, nonself and drawn from planned full endpoints.
The Service-only edge fixture is not presented as an SDK edge. Named loopback
hooks apply only to this functional profile and cannot establish acceptance.
The process adapter is currently the existing macOS sandbox; Linux requires a
separate namespace launcher. Linux proc sampler availability does not enable it.

Launch ownership precedes readiness. Lifetime and readiness contexts are separate;
a failed readiness wait retains the handle for cleanup. Initial local readiness
checks identity, fresh R1 and listener where applicable. After all owned launches,
the executor checks identity uniqueness and a global nonempty peer-health barrier,
then imports only planned public contacts, including each SDK child's real signed
box-key binding. No parent reads an SDK identity private file.

A separate **functional-only** read-only route barrier observes planned
sender-to-recipient entries through the cached `fetchRouteTable` projection.
It requires known nonwithdrawn/nonexpired snapshot flags and a planned next hop.
RPC start/read wall times are checked for backwards movement and future snapshots.
Effective TTL is `rawTTL - (readAt - wireSnapshotAt)` and must remain strictly
positive. RFC3339 second precision deliberately over-ages by less than one second.
This is a cached historical observation, not proof against a later withdrawal;
there is no new snapshot-age policy or route-convergence guarantee. Its explicit
budget remains 35 seconds. Startup convergence elapsed time is monotonic and
recorded separately from the 4.5-second schedule.
The barrier neither forces sync/reconnect nor inserts routes. A timeout refuses
the functional run. The initial functional attempt exposed a receive-baseline
lifecycle defect. With connection-owned receive state fixed, the source-frozen
four-member DM/restart test passes with the unchanged 4.5-second schedule and
8-second drain.

The scheduler uses monotonic elapsed offsets and absolute `WaitUntil`; journal
UTC timestamps do not determine dispatch. At equal offsets the existing order is
Stop, Start, DM, gossip, then sample, with node/recipient tie order retained.
Explicit baseline-zero and final-duration sample phases run exactly once. Action,
readiness, cleanup, route-ready, receipt-drain, lateness and trace bounds are
required profile inputs and journal parameters. A missed budget is a defect;
the executor never silently rebases, skips or duplicates planned actions.
There is one serial dispatcher and one joined, bounded wait observer over owned
incarnation completion channels, with no event worker queue.

Normal `sendDm` returns a pending queue acknowledgement, not a message ID or
proof of delivery. Event bodies contain a public execution nonce, plan digest
and event index. Delivery must match an actual SDK decrypted receipt by sender,
exact recipient and body digest, with a valid message ID. Duplicate/forged/stale
receipts are rejected. Bounded polling happens at samples and in a separately
classified final drain; it does not stretch the schedule. SDK receipts export
metadata only. The existing SDK child scans its public artifacts for own secrets.

Samples attempt every typed source family. Identity/incarnation, transport and
fresh ban observations are required for functional progress. Optional-family
failure remains explicitly unavailable, never a measured zero. The existing
strict sampler is unchanged. In particular RPC 503 does not prove datagrams are
disabled. `FunctionalLifecycleComplete`, `FullSampleAvailable` and
`AcceptanceEligible` are distinct; acceptance is always false here.

Each stop takes an explicit pre-stop sample. In-process final transport totals
are exact after drain. Supported clean SDK processes reduce a strict post-join report through their retained private FD; missing/invalid/forced tails stay unavailable. A pre-stop reading is not relabelled exact. See [final accounting](loadstand-final-accounting.md). Cancellation, unexpected exit,
start failure or action failure triggers independent bounded reverse-order cleanup.
Unconfirmed ownership is retained and reported. Completion requires successful
cleanup and receipt matching; verified journal return additionally requires source
fingerprints to remain unchanged. Resume reads verified records without launching
members, and completed results cannot be overwritten.

Remaining T5 work includes namespace orchestration,
Linux provenance/sampler integration, legacy/profile capability admission,
calibrated byte/resource bounds, T6 controls and separately authorized T7 runs.
No production capability, overlay frame, G2 activation or heavy run is enabled.

## Русский

Инкремент добавляет последовательный test-only исполнитель существующего плана.
Общие типизированные порты находятся в `internal/testutil/loadstand`, исполнитель
и адаптеры — в `internal/core/node/*_test.go`. Это проверка жизненного цикла;
измерительный production wiring и запуск обычного узла не меняются.

Версия плана **3** допускает DM/gossip только от SDK-edge; вероятность DM считается
на каждого допустимого SDK-отправителя. Старые digest-фикстуры v1/v2 сохранены,
v3 имеет новые фикстуры и ожидаемые количества. Старые и новые нагрузки нельзя
сравнивать как одинаковые. Исполнитель отклоняет старую версию и неподдерживаемую роль до запуска; gossip
требует обычного SDK publisher и FULL observer. Неподписанный global и локальный
SQLite commit отделены от наблюдения FULL-реплик (см. sdk-global-publication.md).
Готовые кадры, подмена Sender и подпись чужими ключами не используются.

Функциональная топология — два настоящих full Service с взаимным плановым
bootstrap и два SDK-процесса без listener, с собственными identity,
DesktopClient и SQLite. Bootstrap непустой, не собственный и только из плана.
Прежний Service-only edge не называется SDK. Loopback hooks явно относятся
только к функциональному профилю. Процессный запуск пока использует sandbox macOS;
наличие Linux proc-сэмплера не включает отсутствующий namespace launcher.

Владение запуском сохраняется до ожидания готовности; lifetime/readiness contexts
раздельны. После локальной готовности всех запусков проверяются уникальность
identity и общий барьер peer health, затем импортируются только публичные
контакты плана с настоящими подписанными box-ключами SDK. Приватные файлы identity
SDK-процессов родитель не читает.

Отдельный read-only барьер до расписания наблюдает плановые пары в кэшированной
проекции `fetchRouteTable`. Нужны известные nonwithdrawn/nonexpired флаги snapshot
и плановый next hop. Проверяются start/read wall time: обратный ход и будущий
snapshot отказываются. Эффективный TTL — `rawTTL - (readAt - wireSnapshotAt)`,
строго больше нуля. Секундный RFC3339 консервативно завышает возраст менее чем
на секунду. Это историческое наблюдение кэша, не доказательство отсутствия более
позднего withdrawal. Новая политика возраста snapshot и гарантия сходимости
не вводятся. Бюджет остаётся 35 секунд. Монотонное время сходимости отделено
от расписания 4.5 секунды.
Барьер не вставляет маршруты и не форсирует sync/reconnect. Начальный прогон
выявил отдельный дефект жизненного цикла receive baseline. После исправления
connection-owned receive state SDK-прогон на замороженных исходниках проходит
с прежними расписанием 4.5 секунды и drain 8 секунд.

Монотонные offsets независимы от UTC журнала. При совпадении времени порядок:
Stop → Start → DM → gossip → sample; начальный и финальный samples выполняются
ровно один раз. Все бюджеты и пределы trace заданы явно и входят в параметры
журнала. Просрочка — дефект; план не растягивается и события не пропускаются.
Один последовательный dispatcher не создаёт очередь скрытых workers.

`sendDm` подтверждает очередь. Доставку доказывает настоящая расшифрованная SDK
квитанция с точным sender/recipient/body digest и message ID. Публичный nonce
исполнения предотвращает совпадение со старым прогоном. Ожидание квитанций
ограничено samples и отдельным финальным drain, не меняющим расписание.

Недоступные семейства samples не превращаются в нули; строгий sampler не ослаблен.
RPC 503 не объявляется состоянием «datagram выключен». Успех lifecycle отделён
от полноты samples; `AcceptanceEligible` всегда false. Перед Stop снимается sample;
Точные transport totals после drain доступны in-process и у чистого поддерживаемого SDK child через strict post-join отчёт retained private FD. Missing/invalid/forced хвосты unavailable. См. [итоговый учёт](loadstand-final-accounting.md). Любая ошибка или отмена запускает
отдельную ограниченную очистку; неподтверждённое владение сохраняется в отчёте.
Успешный возврат журнала дополнительно требует неизменных исходников. Resume не
перезапускает участников и не перезаписывает уже подтверждённый результат.

Остаются namespace runner, интеграция Linux provenance/sampler,
проверки старых профилей, калибровка ресурсов/byte balance, T6 и отдельные T7.
Production activation, G2 и тяжёлые прогоны не включаются.


Functional scheduling retains each planned offset and records actual begin/end.
The explicit lateness allowance can cause consecutive overdue actions to run
back-to-back; this lifecycle fixture does not prove original traffic spacing.
A future measurement profile must classify such bunching as an invalid traffic
profile. Cleanup reverses the current incarnation start order, including handles
owned by a partial failed start. The order retains at most one entry per member.

Функциональный scheduler сохраняет offsets и actual begin/end. Явный допуск
просрочки может привести к последовательному выполнению нескольких просроченных
действий подряд; lifecycle-фикстура не доказывает исходные интервалы нагрузки.
Будущий измерительный профиль должен считать такое сгущение нарушением профиля.
Cleanup идёт в обратном порядке запуска текущих incarnations, включая владение
после частично неуспешного Start. Хранится не более одной записи на участника.


Receipt metadata uses the existing pinned-parent public artifact reader with an
explicit byte bound. It accepts regular files only, checks descriptor/name inode
identity, refuses symlinks and replacements, and opens nonblocking/no-follow on
Unix so a substituted FIFO cannot wait for a writer. Context is checked around
reads and during record parsing. This cannot interrupt a regular filesystem
syscall stuck inside the kernel; no stronger hard-I/O-deadline guarantee is made.

Метаданные квитанций читаются общим public artifact reader с закреплённым
каталогом и явным пределом байтов. Разрешены только regular files; сверяются
inode дескриптора и имени, ссылки и подмена отказываются. Unix open использует
nonblocking/no-follow, поэтому подменённый FIFO не ждёт writer. Context проверяется
вокруг чтения и при разборе строк. Обычный файловый syscall, зависший внутри
ядра, этим не прерывается; более сильная гарантия I/O deadline не заявляется.


## C1 fixed known-load functional profile / Известная добавленная нагрузка

The optional `c1-global-32k-96` gossip payload profile is plan version4. Empty
payload profile retains all existing version3 params, canonical digest and short
`t5d/<plan>/<execution>/<event>` bodies. Version4 additionally binds
`gossip_payload_profile=c1-global-32k-96`, `gossip_body_bytes=32768` and
`gossip_publications=96` and
`gossip_sender_setup=original-sdk-contact-before-measurement` to the plan digest/config key. The deterministic ASCII
body preserves that unique prefix, padded to exactly32768bytes; the existing
ordinary SDK `PublishGlobal` path performs canonical/wire validation and local
SQLite commit. No raw-frame sender or fake counter is used.

The bounded scenario is two original Fulls plus one original SDK child, quiet,
30seconds, settle2seconds, gossip every250ms capped at96 publications (2.25..26s),
and the existing periodic sampling every1second, including zero and30s. This is
3145728 authored body bytes and192 exact Full runtime-replica observations, not a
predicted wire/heap value. There are31 complete network cuts,93 periodic member
readings and4 fresh process-group preStop readings;127 scheduled entries. Existing
256action/128sample/4MiB trace limits and operation/lateness/readiness/cleanup/drain
bounds remain unchanged. Encoded trace size and throughput still require actual
verification. This is a functional exercise, never T5 memory/CPU acceptance.

An observation's optional `ObservedSweep` is its FIRST successful periodic poll.
A drain observation has no index and is never retagged by a later poll. Global
polling follows the traffic read, so only observations from an EARLIER periodic
poll cover the selected later traffic edge. `CompletedActions` binds a traffic
edge to the original sequential trace append order; scheduled or wall timestamps
cannot prove a publication preceded it. Positive C1 requires all96 unique
publications and192 exact observer/hash/incarnation matches before the final
edge, an actual judged no-defect C3 result with unchanged default tolerance,
original cleanup/finals, and the original completed journal. Lack of genuine
volume/power or pre-edge delivery is INCONCLUSIVE, not PASS, and never silently
increases payload/time/tolerance or triggers a retry.

A later separately retained Source mutant may duplicate only the actual socket
meter's `addSent(n)` and rebuild both measured test and SDK child from it. It is
not applied by this profile. Expected arithmetic RED requires the typed
`byte_balance` defect, actual computed volume/share/imbalance, and at least one
genuine completed known-load dispatch inside the judged interval. It can happen
before96/192 completion: retain the actual prefix, not invented future receipts.
Original journal records one failed attempt with sanitized `ErrMemberState`
reason, no completed Result; caller error/report retain the exact arithmetic
cause. Setup, source-invalid, undefined and timeout errors are different outcomes.
The baseline must be actually judged first; no mutant credit is transferred from
synthetic math tests. Default tolerances remain BurstFactor2,64KiB per-node
in-flight and10percent max allowance share. These provisional defaults are not
new measured thresholds.

Профиль включается явно, только для тихой SDK global-нагрузки. Обычный профиль
v3 сохраняется. Все тела, ACK локальной SQLite и replica observations идут через
исходные Runtime/Desktop/Service; «stored locally» не доказывает доставку, а Full
runtime replica не объявляется подписью или устойчивой SQLite-копией получателя.
Результат C1/C3 засчитывается только при реальном judged interval; undefined
остаётся отдельным inconclusive. Проверяемое окно заканчивается на genuine
ReadAt последней существующей periodic-границы. PreStopOf не является границей
всей сети; drain/cleanup/terminal tail явно unjudged, все original Ended/final
receipts сохраняются. Новых sampling calls или изменений tolerances нет.

C1's fixed two-Full/one-SDK quiet graph keeps DM `Contacts=0`. Before the
measurement clock is created, the existing contacts stage imports the one
successful original SDK StartReceipt.Public (including its genuine BoxSignature)
into each original Full through normal Member.ImportContacts. The retained
successful incarnation and original RuntimeProcess custody are checked first.
No Full row without a signed box key is imported. At the existing clock-factory
frontier the C1 test reads each Full's exact SDK public key and normal verified-
sender predicate with no relay shortcut; silent rejected imports therefore fail
before measurement. No map writes, new barrier, sleep or additional sample is
introduced. Other profiles retain their old contacts behavior and version3
params. This explicit known-publisher fixture avoids treating a lawful unknown-
author drop as guaranteed global delivery; it does not weaken sender admission.

The initial Contacts0/no-introduction real attempt remains a failed predecessor:
96 publications produced191 of192 Full replicas; the first forwarded message was
explicitly dropped as an unattributed non-DM author on one Full. That observation
is not a rate-limit or arithmetic-power verdict. Failed C1 attempts now log a
bounded metadata-only JSON summary before fatal assertions: actual execution
phase/action index, dispatch and missing replica IDs/hashes, first observation
sweep indexes, last actions/checks, actual judged result and original cleanup
counts. No raw body, key material, environment or SQL is logged. Diagnostic size
is capped at64KiB without changing any acceptance bound. Receipt-drain timeout
is explicitly inconclusive delivery and still fails; it never expands the run.

Для C1 отдельно закреплён известный SDK-издатель до измерения: genuine Public
квитанции успешного исходного Start с BoxSignature проходит обычный ImportContacts
обоих Full. DM Contacts0 и роли не меняются. Read-only проверка исходных ключей
и isVerifiedSender выполняется до возврата прежних часов. Это явная подготовка
фикстуры, не обход production admission. Неуспешный исходный прогон с191/192
репликами сохраняется; метаданные ошибки не превращают timeout/undefined в PASS
и не увеличивают нагрузку, время или tolerance.
