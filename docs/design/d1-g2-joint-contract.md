# D1: joint 18a / 21a-min review record / Совместная сверка 18a / 21a-min

## Current verification scope / Текущая область проверки — 2026-10-23

The [later owner decision](../operations/d1-release-decision-proposal.md#measurement-scope-2026-10-23) defers real Windows/DACL and Tor measurements while work proceeds on macOS/Linux. Earlier unconditional Windows/Tor release prerequisites below are superseded only in that scope; the historical results are retained. Windows and Tor remain UNVERIFIED/UNKNOWN, never PASS. Schema 2 retains all 17 conditions: 7 done, 2 partial, 7 not_done and 1 deferred (`tor_phase_measurement`). The unsafe-store composite remains partial; clearnet/real-old-release first-contact timing and timeout derivation remain required. Proxy/no-direct-fallback, authentication, independent crypto review, other tests, G2 and separate release decisions remain required. The gate is closed. Any Tor observation role remains deferred; macOS/Linux data establishes no Tor threshold or guarantee.

[Последнее решение владельца](../operations/d1-release-decision-proposal.md#measurement-scope-2026-10-23) переносит реальные Windows/DACL и Tor замеры; работа идёт на macOS/Linux. Прежние безусловные требования Windows/Tor ниже заменены только в этой области, исторические результаты сохранены. Фактический статус — UNVERIFIED/UNKNOWN, не PASS. Schema 2 сохраняет 17 условий: 7 done, 2 partial, 7 not_done и 1 deferred (`tor_phase_measurement`). Условие небезопасного хранилища остаётся partial; clearnet/настоящие старые выпуски, замеры первого контакта и вывод timeout остаются обязательными. Proxy/no-direct-fallback, аутентификация, независимая криптоэкспертиза, остальные проверки, G2 и отдельные решения о выпуске сохраняются. Gate закрыт. Tor-роль наблюдения отложена; данные macOS/Linux не доказывают пороги или гарантии Tor.

## Original R assignment readout / Исходное назначение R — 2026-10-25

The inactive relay observation now carries `AssignmentKnown`, `ReservationAtNS`, `AssignedSlotNS` and `LatestForwardNS`. They copy the original successful forward receipt's `at`, `scheduled` and `latest`, under the existing locks after all original Take validation/debits. Matching the original answer freezes value scalars with the existing generation/source/target/request tuple before pending-slot release. No receipt/calendar pointer, lookup authority, new Node work handoff, clock sample or operational default is introduced. The existing getter copies its reader once; history remains independent of later slot reuse, calendar retirement and End.

A known assignment additionally requires `0≤a≤reservation≤s≤f≤latest` in the same R owner domain. Missing or inconsistent passive history leaves only the new assignment unknown; it neither refuses an otherwise valid Match nor erases existing relay/chain points. Refused or legacy caller-timed Take does not invent an owned assignment. The three stored times and known bit increase existing fixed structures; original `SizeOfAll` accounting reflects this growth, without a zero-overhead or resource-acceptance claim.

The assigned slot `s` is not an observed timer-arm/wake point. Conditional on a valid original ledger and `0≤Q≤s−a`, it supplies `f−s≤P_pre≤f−a`. It does not measure exact Q, isolate wake cost or prove physical rates/delivery. The post-assertion record has a new marker `D1_P0_R4_T3_S1_V1` and schema `corsa.d1.p0.R4-T3-S1.v1`; the historical V1 decoder/records remain unchanged. The strict record bound remains less than8KiB. The S1 decoder checks the entire closed Node73 stream, one original parent record, exact new fields/types/order, the declared `latest=a+1s` profile, original wire tuple joins and terminal parent/package PASS. The original functional assertions retain authentication/signature/debit proof; the wrapper and independent actual qualification separately require completed cleanup/custody and a normal final ROOT handle. It retains signed interval residuals and conditional R bounds; raw Instants from different owners are not subtracted.

Composed functional QA joins the original six Linux CGO1 Go runs with fresh lint of the final annotated source. Each mode passed 293 events and 71 parents: loadstand55/11, findnode165/45 and Node73/15, with zero FAIL/SKIP/stderr. The Go executions used source `ebfc467e`; final source `b878c188` differs only by one 95-byte trailing lint comment on the intentional monotonic-data comparison. Exact comment deletion restores the tested bytes, with unchanged executable tokens/newlines, tests and source-reader behaviour; those Go results are carried, not presented as new executions. ROOT11965 remains STOP_NOT_PASS because its original Linux lint refused QF1009. The new continuation, tool session92297/terminal22b874, closed normally with exit0: fresh Darwin/Linux arm64 CGO0 three-package loading and lint returned0, lint62.322/70.064 seconds. Final source/MAIN/index/input/performance-seed/log preservation and each fresh lint original telemetry-FD check completed. Complete original Node73 ordinary/race RAW passed strict S1 qualification in this continuation. This is focused functional QA, not a full-repository lint claim or runtime/resource acceptance. Local evidence: `/private/tmp/corsa-D1-helper8-G2S1-composed-QA-ROOT-v3` and `/private/tmp/corsa-D1-helper8-G2S1-lint-continuation-QA-ROOT-v1/result.json`; ROOT terminal observations are separate from independent review.

Составная функциональная QA объединяет шесть исходных Linux CGO1 Go-прогонов и свежий lint финальной редакции. На каждый режим:293 PASS/71 родитель — loadstand55/11, findnode165/45, Node73/15;0FAIL/SKIP/stderr. Go исполнялся на source `ebfc467e`; финальный `b878c188` отличается только95-байтовым trailing lint-comment у намеренного сравнения монотонных данных. Точное удаление комментария восстанавливает проверенные байты; executable tokens/newlines, тесты и смысл source-readers прежние. Go-результаты перенесены по этой эквивалентности, новых Go-исполнений не заявляется. ROOT11965 сохраняет STOP_NOT_PASS исходного Linux lint/QF1009. Новое продолжение session92297/terminal22b874 завершилось normal0: свежие Darwin/Linux arm64 CGO0 loading и lint трёх пакетов дали0, lint62,322/70,064с. Итоговые проверки source/MAIN/index/входов/performance-seeds/старых логов и исходных telemetry FD свежего lint завершены. Полные исходные Node73 RAW ordinary/race прошли строгую S1-квалификацию. Это focused функциональная QA, не full-repository lint или runtime/ресурсная приёмка. Локальные свидетельства указаны в английском тексте; ROOT terminal observations отделены от независимой приёмки.

| Owner-local expression / Выражение | Ordinary, ns | Race, ns |
| --- | ---: | ---: |
| R f−a | 104320500 | 102185917 |
| R reservation−a | 104276833 | 101715375 |
| R s−a | 104276833 | 101715375 |
| R s−reservation | 0 | 0 |
| R f−s | 43667 | 470542 |
| R latest−f | 895679500 | 897814083 |
| T c−r | 109333 | 742041 |
| signed (h−f)R−(c−r)T | 1267083 | 445500 |

For these two small2 functional cases, conditional `P_pre` bounds are [43,667;104,320,500] ns ordinary and [470,542;102,185,917] ns race, under the stated ledger/Q premises only. Here `s=reservation`; the records do not demonstrate an intentional future wait or exact Q. They are two executions, not a distribution or worst-case/physical guarantee.

В этих двух small2 случаях условные границы `P_pre`: [43 667;104 320 500]ns ordinary и [470 542;102 185 917]ns race, только при указанных предпосылках ledger/Q. Здесь `s=reservation`; намеренное ожидание будущего слота и точный Q этими записями не измерены. Это два исполнения, не распределение и не worst-case/физическая гарантия.

Queue/wake decomposition, clock accuracy/suspend premises, all timed/cached/coalesced paths, writer/return tails, useful512/512/1024 coverage, maximum admitted burst, resources, G2, activation and release remain open. Default constructors remain inactive.

Неактивный relay readout теперь содержит `AssignmentKnown`, `ReservationAtNS`, `AssignedSlotNS`, `LatestForwardNS`. Это копии исходных receipt.at/scheduled/latest успешного forward Take под прежними locks после всех прежних проверок/debits. Match исходного ответа фиксирует значения вместе с прежним generation/source/target/request tuple до освобождения pending-слота. Pointer календаря/receipt, lookup authority, новый Node handoff, sample часов или operational default не вводятся; getter один раз копирует прежний reader. История переживает reuse слота, retirement календаря и End.

Известность требует `0≤a≤reservation≤s≤f≤latest` в одном R owner clock. Отсутствующая/непоследовательная пассивная история оставляет UNKNOWN только новое назначение; успешный Match и прежние relay/chain-точки не меняются. Refused/legacy caller-timed Take не создаёт owned assignment. Структуры увеличены на скаляры/known bit; прежний `SizeOfAll` учитывает это, нулевая стоимость и ресурсная приёмка не обещаются.

Назначенный слот `s` не является наблюдённой точкой timer arm/wake. При корректном исходном ledger и `0≤Q≤s−a` следуют условные границы `f−s≤P_pre≤f−a`; точный Q, wake-cost, физическая скорость или доставка этим не доказаны. Новые marker/schema S1 указаны выше, исторический V1 сохранён. Предел записи прежний: строго меньше8KiB. Decoder проверяет весь замкнутый Node73 stream, исходного родителя/единственную запись, поля/типы/порядок, объявленный профиль `latest=a+1s`, прежний wire tuple и terminal parent/packagePASS. Подлинность/подпись/debit проверяют исходные функциональные assertions; wrapper и независимая actual-квалификация отдельно требуют завершённых cleanup/custody и normal0 всего ROOT handle. Signed interval residuals сохраняются; сырые Instants разных owners не вычитаются.

Разложение очереди/wake, точность/сон часов, остальные timed/cached/coalesced пути, writer/return tails, полезный допуск512/512/1024, максимальный принятый всплеск, ресурсы/G2/активация/выпуск открыты. Обычные constructors остаются неактивными.

## Bounded original R4/T3 record / Ограниченная запись исходных R4/T3 — 2026-10-24

The existing genuine two-Service test now logs one bounded versioned JSON value after its original assertions, copying its retained original R a/f/h/u and T r/Take/c snapshots and exact wire request. Only that test and its closed raw-hash guard change. No production path, clock sample, stored state, callback or default changes. Qualification reconstructs same-parent test2json fragments, requires one complete record below8KiB, exact tuple/codec/known integer fields, parent and package PASS after cleanup, final source/MAIN/original-FD custody and whole ROOT normal exit0. A complete=true field alone never qualifies a record; duplicate/missing/truncated data and cleanup failure reject it. Six passive RAW control methods passed separately; their first invocation failed before tests because isolated Python omitted the local module directory, then only the reviewed entry-point flags were corrected, preserving Source bytes and the earlier failure.

ROOT22077 passed **11 ordinary and 11 race events**, four parents per mode, no FAIL/SKIP/stderr; scoped Node Darwin/Linux CGO0 lint returned zero issues (55.068/58.795 seconds). These are two small functional executions, not a latency distribution or performance acceptance. The table contains exact integer differences in the owning clocks; the final row is a signed algebraic difference of two owner-local durations, not a direct subtraction of cross-owner Instants or a measured network delay.

| Owner-local expression / Выражение | Ordinary, ns | Race, ns |
| --- | ---: | ---: |
| R f−a | 101225625 | 102206125 |
| R u−h | 25167 | 245708 |
| R h−f | 381625 | 1455334 |
| R u−a | 101632417 | 103907167 |
| T c−r | 146833 | 994416 |
| T c−Take | 21791 | 381708 |
| T Take−r | 125042 | 612708 |
| signed (h−f)R−(c−r)T | 234792 | 460918 |

Physical interpretation and Q remain **UNKNOWN**. A conservative f−a comparison could cover one pre-forward case only under independently proved clock/endpoint premises; this record supplies no such physical proof, no exact queue/wake cost and no worst-case bound. Cached/coalesced/untimed paths, return/writer tails, population512/512/1024, maximum admitted burst, resource acceptance, G2, activation and release remain open. Local QA/RAW: `/private/tmp/corsa-D1-P0-R4-T3-value-log-QA-ROOT-v1`; availability elsewhere is not assumed.

Существующий настоящий fixture двух Service теперь выводит один ограниченный versioned JSON после исходных assertions: копии сохранённых R a/f/h/u, T r/Take/c и точный wire request. Меняются только test и его closed rawSHA guard; production, чтения часов, состояние и defaults прежние. Проверка собирает фрагменты test2json того же parent, требует ровно одну полную запись менее8KiB, точный tuple/codec/известные integer поля, parent/packagePASS после cleanup, итоговую сохранность source/MAIN/originalFD и wholeROOT normal0. Одного complete=true недостаточно; missing/duplicate/truncated, неверный tuple и cleanupFAIL отклоняются. Шесть отдельных RAW controls прошли; первая попытка отказала до тестов из-за -I/local import, затем исправлен только проверенный argv, Source и старый FAIL сохранены.

ROOT22077: **11 ordinary+11 race PASS**, по4 родителя,0FAIL/SKIP/stderr; scoped Darwin/Linux Node CGO0 lint —0 замечаний. Таблица содержит точные ns-разности в часах соответствующего owner и знаковую алгебраическую разность двух длительностей; это не вычитание Instants разных owner и не измеренная задержка сети. Два функциональных запуска не являются распределением задержек или ресурсной приёмкой. Физическая интерпретация и Q — **UNKNOWN**: предпосылки часов/endpoint, exactqueue/wake, tails, cached/coalesced/untimed, coverage512/512/1024 и максимальный допустимый всплеск не доказаны. G2/активация/выпуск не утверждены.

## Inactive P0 endpoint evidence / Неактивное наблюдение точек P0 — 2026-10-24

The inactive P0 slices retain original R receive `a`, successful original forward Take `f`, matched original answer receive `h` and successful original answer Take `u`. The matched answer freezes historical scalars and the exact generation/session/request tuple before releasing its pending slot. A fresh reused slot does not inherit an earlier forward point. The private readers return copies, known bits and Runtime/Fixture/Unknown provenance; they add no clock sample, lookup authority, callback or admission/default change. A writer-reported OK/failed outcome is recorded separately: neither that report nor `u` proves byte completion or delivery. Refusal, cancellation and replay do not invent missing endpoints; local/cache/coalesced answers do not acquire a relayed `h`.

The receive/forward slice passed 64 ordinary and 64 race events with zero scoped two-package lint issues. The receive/answer successor passed 189 ordinary and 189 race events (44 parents per mode: findnode 149/38 and Node 40/6), with no failures/skips and zero scoped two-package lint issues. The initial test-cleanup failure is retained separately; only its three-line session-End publication was corrected. Proposed functional mutations remain unexecuted. These are functional custody checks, not physical timing measurements. In particular `a→f` includes calendar/queue/lock waits and does not isolate the 400ms pre-forward allowance; the projected queue interval must not be charged twice.

The internal-package `OwnedAnswerWork.RelayEndpointObservation()` exposes a fixed value copy of the existing R points a/f/h/u, known/provenance bits and exact original/forwarded tuple. It calls the unchanged private reader once, adding no clock sample, lock, storage or operational authority. Local/cache/coalesced answers can report their own u/writer outcome without a known relay chain; history survives later End and slot reuse. The API-only host suite passed 76 ordinary and 76 race events (15 parents each, no FAIL/SKIP); scoped Darwin/Linux CGO0 lint passed with zero issues (ROOT67545). This supplies no automatic six-point trace, physical clock bound or population proof.

The finite `OwnedLocalSignWork.TargetEndpointObservation()` now exposes original T receive `r`, successful Complete sample `c`, their original request/session tuple and explicit known/provenance bits. It copies the existing target-mutex snapshot, adds no persistent clock/storage/authority and preserves the original Take/Complete paths. Its `SpanNS` is `c−r`; the earlier private Take→Complete observation measures the separate signing reserve. Cancellation, missing signing completion and regressed Complete leave `c`/pair unknown. A waiting or cache recipient does not acquire another job's signing work. The earlier Node test manually retains the original handoff, then uses the original worker/sign/Complete/writer; it is not a full automatic dispatch trace.

The same target reader now also projects `TakeKnown` and `TakeNS` from its existing timing snapshot, without another clock sample, lock or stored record. `SpanNS` remains `c−r`. When the original points are known, the caller can distinguish `Take−r` from `c−Take` in that T owner's domain; neither is isolated cryptographic CPU time. A genuine zero Take stays known, a refused Take invents no endpoint, and a later failed Complete does not erase an earlier genuine Take. The automatic Node fixture reads the same original T work, checks `r≤Take≤c` and the decomposition of `c−r`, and still verifies the original signed upstream answer and debits. Host QA passed 90 ordinary and 90 race events (18 parents per mode: findnode54/10 and Node36/8), with no FAIL/SKIP/stderr and zero scoped Darwin/Linux CGO0 lint issues (ROOT31044). These are readout/custody checks: no 500ms deadline is enforced or physically proved; queue/wake decomposition, clock uncertainty, return/writer tails, coverage and G2 remain open.

A separate small2 fixture negotiates F1 through genuine v2 clients and the original `Service.Run` listener, captures a real two-member cut and observes committed/taken forwarding at the target wire. A finite private per-Service bit is enabled only by its `_test.go` factory before Run; ordinary and pre-existing Ready constructors remain inactive. The fixture also checks an unnegotiated source, ended original target and complete-or-refuse at a two-member/one-member-cap boundary. This proves neither a T response/delivery nor stock activation or 512-population coverage. Joint scoped QA passed 165 ordinary and 165 race events (42 parents per mode: r/c96/21 and pilot69/21), without failures/skips; Darwin/Linux CGO0 lint reported zero issues. These are host functional tests and platform lint, not Linux-kernel execution or resource acceptance.

The automatic two-Service fixture now correlates one signed attempt through the original R listener/CM membership, intake/round/forward, T receive/self-reservation/worker/sign/Complete/writer, and R answer-match/Take/writer to a verified upstream answer. A fixed one-shot per-Service holder is selected only by its closed `_test.go` factory before Run; ordinary and pre-existing Ready constructors retain nil. It retains the original work handles, without extra clock samples, callbacks, session minting or public activation. Exact original/forwarded request fields, authenticated identities and per-owner session tuples join R a/f/h/u with T r/c; ordering and elapsed differences stay inside each owner's domain. Host QA passed 94 ordinary and 94 race events (27 parents per mode, no FAIL/SKIP/stderr), with zero scoped Darwin/Linux CGO0 lint issues (ROOT33248). The first attempt failed before the query at the fixture's outbound-proof wait; only the direction-correct original proof read and its test hash were repaired, preserving the 2s wait and historical failure. This establishes automatic functional custody, not physical clock accuracy, queue/reserve decomposition, writer byte completion, a numerical P0 bound or 512-population coverage.

For **one genuinely correlated causal attempt** with `f≤r≤c≤h`, the combined outward/return residual can be expressed as `B=(r−f)+(h−c)=(h−f)_R−(c−r)_T`. Absolute clock origins cancel; raw Instants from different clock domains must still never be subtracted directly. If independently justified physical-duration intervals are `[L_R,U_R]` and `[L_T,U_T]`, then `B∈[L_R−U_T,U_R−L_T]`. This identity supplies no clock-rate, unit, suspend or endpoint-error bound and does not identify either one-way delay. Preserve signed uncertainty: a negative point estimate is not a measured zero; an entirely negative interval or invalid attempt binding leaves the interpretation UNKNOWN. P0 already assigns the complete T post-Complete return tail to `D_N=2s`; that allocation is not a measured bound.

The automatic fixture supplies a genuine correlated `a/f/r/c/h/u` record for its small profile. Timed-target applicability across the required paths, queue/reserve decomposition and physical clock/error proof remain open. Go monotonic Runtime provenance alone does not establish a physical duration through suspend. Cached/coalesced/untimed paths require separate recipient and service-bound evidence. No six-second delivery guarantee, G2, resource acceptance, activation or release follows.

The 512-active/512-member/1024-registry-visit envelope has **not** been exercised with that real population or shown to admit useful traffic. The existing 1024/1025 nil-key boundary test proves complete-or-refuse bookkeeping only. The ordinary loopback inbound cap of 64 prevents simply scaling the small fixture to 512; production limits are unchanged. A genuine external-address fixture needs exact alias-set/network verification and source-bound dials before population/resource admission. Separate active/member and registry-visit populations must not be conflated. A cap hit refuses the whole cut; zero admission remains a profile limitation.

Неактивные участки P0 сохраняют исходные точки R: приём `a`, успешный forward Take `f`, приём сопоставленного исходного ответа `h` и успешный answer Take `u`. До освобождения pending-слота ответ фиксирует исторические скаляры и точный tuple generation/session/request. Новый слот при повторном использовании не наследует прежнюю точку пересылки. Закрытые readers возвращают копии, признаки известности и происхождение Runtime/Fixture/Unknown; новых чтений часов, полномочий lookup, callback, изменений допуска/defaults нет. Сообщённый writer исход OK/failed учитывается отдельно: ни он, ни `u` не доказывают завершения передачи байтов или доставки. Отказ, отмена и replay не создают отсутствующие точки; local/cache/coalesced ответы не получают relayed `h`.

Участок приёма/пересылки прошёл 64 ordinary и 64 race события, scoped lint двух пакетов — ноль замечаний. Successor приёма/ответа прошёл 189 ordinary и 189 race событий (44 родителя на режим: findnode 149/38 и Node 40/6), без FAIL/SKIP; scoped lint двух пакетов — ноль замечаний. Исходный сбой cleanup теста сохранён отдельно; исправлены только три строки публикации завершения сессии. Предложенные функциональные мутации не запускались. Это функциональные проверки принадлежности исходной работе, не физические временные замеры. В частности, `a→f` включает календарь, очередь и ожидания locks и не выделяет отдельно 400мс до пересылки; полный интервал очереди нельзя списывать повторно.

Внутрипакетный `OwnedAnswerWork.RelayEndpointObservation()` возвращает конечную копию прежних R-точек a/f/h/u, известности/происхождения и точного original/forwarded tuple. Один вызов неизменённого private reader не добавляет sample часов, lock, хранилища или полномочий. Local/cache/coalesced могут сообщать собственные u/writer outcome без известной relay chain; история сохраняется после End/reuse. API-only host suite:76 ordinary+76 race PASS, по15 родителей,0FAIL/SKIP; scoped Darwin/Linux CGO0 lint —0 замечаний (ROOT67545). Автоматическая шеститочечная запись, физические границы часов и population этим не доказаны.

Конечный `OwnedLocalSignWork.TargetEndpointObservation()` теперь возвращает исходные receive `r` цели, sample успешного Complete `c`, исходный request/session tuple и явные признаки известности/происхождения. Он копирует snapshot под прежним target mutex, не добавляет постоянных часов/хранилища/полномочий и сохраняет исходные Take/Complete. Его `SpanNS` равен `c−r`; прежнее private Take→Complete наблюдение измеряет отдельный резерв подписи. Отмена, отсутствие завершённой подписи и откат Complete оставляют `c`/pair неизвестными. Waiting или cache-получатель не получает signing work другой задачи. Прежний Node-тест вручную сохраняет исходный handoff, затем использует прежние worker/sign/Complete/writer; это не полный автоматический dispatch trace.

Тот же target reader теперь возвращает также `TakeKnown` и `TakeNS` из прежнего timing snapshot, без нового sample часов, lock или сохраняемой записи. `SpanNS` остаётся `c−r`. При известных исходных точках caller различает `Take−r` и `c−Take` внутри домена owner T; ни один из них не является выделенным CPU-временем криптографии. Подлинный нулевой Take остаётся известным, отказ Take не создаёт endpoint, а последующий отказ Complete не стирает прежний подлинный Take. Автоматический Node fixture читает тот же исходный T work, проверяет `r≤Take≤c` и разложение `c−r`, сохраняя проверку исходного подписанного upstream-ответа и дебетов. Host QA:90 ordinary+90 race PASS (по18 родителей: findnode54/10 и Node36/8),0FAIL/SKIP/stderr, scoped Darwin/Linux CGO0 lint —0 замечаний (ROOT31044). Это проверка readout/принадлежности исходной работе: deadline500мс не вводится и физически не доказывается; разложение очереди/wake, погрешности часов, return/writer tails, coverage и G2 остаются открытыми.

Отдельный малый fixture с двумя клиентами согласует F1 через настоящие v2-клиенты и исходный listener `Service.Run`, получает подлинный cut двух участников и наблюдает committed/taken пересылку на wire цели. Конечный закрытый per-Service bit включает только его `_test.go` factory до Run; обычные и прежние Ready constructors остаются неактивными. Проверяются также несогласовавший capability источник, завершённая исходная target-сессия и complete-or-refuse на границе двух участников при cap=1. Это не доказывает ответ/доставку T, активацию по умолчанию или coverage популяции 512. Совместная scoped QA прошла 165 ordinary и 165 race событий (42 родителя на режим: r/c96/21 и pilot69/21), без FAIL/SKIP; Darwin/Linux CGO0 lint — ноль замечаний. Это host-функциональные тесты и platform lint, не выполнение Linux kernel или ресурсная приёмка.

Автоматический fixture двух Service теперь сопоставляет одну подписанную попытку через исходные R listener/CM membership, intake/round/forward, T receive/self-reservation/worker/sign/Complete/writer и R answer-match/Take/writer с проверенным upstream-ответом. Фиксированный одноразовый per-Service holder выбирает только закрытый `_test.go` factory до Run; обычные и прежние Ready constructors сохраняют nil. Сохраняются исходные work handles без дополнительных samples часов, callback, создания сессий вручную или публичной активации. Точные поля original/forwarded запроса, аутентифицированные identity и per-owner session tuples связывают R a/f/h/u с T r/c; порядок и разности длительностей проверяются только внутри домена каждого owner. Host QA:94 ordinary+94 race PASS (по27 родителей,0FAIL/SKIP/stderr), scoped Darwin/Linux CGO0 lint —0 замечаний (ROOT33248). Первая попытка отказала до запроса при ожидании outbound proof в fixture; исправлены только чтение исходного proof в правильном направлении и хеш теста, прежний FAIL и ожидание2с сохранены. Это подтверждает автоматическое функциональное сохранение исходных событий, а не точность физических часов, разложение очереди/резерва, завершение передачи байтов writer, численную границу P0 или coverage популяции512.

Для **одной подлинно сопоставленной причинной попытки** с `f≤r≤c≤h` сумма прямого и обратного остаточных участков выражается как `B=(r−f)+(h−c)=(h−f)_R−(c−r)_T`. Абсолютные начала отсчёта сокращаются; непосредственно вычитать сырые Instants разных clock domains по-прежнему нельзя. При независимо обоснованных интервалах физических длительностей `[L_R,U_R]` и `[L_T,U_T]` получается `B∈[L_R−U_T,U_R−L_T]`. Это тождество не даёт границы погрешности хода/единиц часов, suspend или конечных событий и не выделяет односторонние задержки. Знаковая неопределённость сохраняется: отрицательная точечная оценка не становится измеренным нулём; целиком отрицательный интервал или неверная привязка попытки оставляют интерпретацию UNKNOWN. P0 уже относит весь хвост T после Complete к `D_N=2с`; распределение бюджета не доказывает его физическую границу.

Автоматический fixture даёт подлинную сопоставленную запись `a/f/r/c/h/u` в своём малом профиле. Применимость timed-target на требуемых путях, разложение очереди/резерва и физическое обоснование часов/погрешностей остаются открытыми. Одного монотонного происхождения Go Runtime недостаточно для физической длительности через suspend. Cached/coalesced/untimed пути требуют отдельных доказательств получателя и service bound. Гарантия доставки за шесть секунд, G2, ресурсная приёмка, активация и выпуск из этого не следуют.

Область 512 active/512 members/1024 registry visits **не проверялась** на такой реальной популяции и не показала полезного положительного допуска. Прежний nil-key тест границы 1024/1025 доказывает только учёт complete-or-refuse. Обычный loopback inbound cap 64 не позволяет просто увеличить малый fixture до 512; production-лимиты прежние. Подлинному fixture с внешними адресами нужны точная проверка набора aliases/сети и dials с привязанным source до допуска популяционных/ресурсных прогонов. Популяции active/members и registry visits нельзя смешивать. Достижение лимита отклоняет весь cut; нулевой допуск остаётся ограничением профиля.

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

### English — working hypothesis selected, deployment unselected, no activation

On 2026-10-23 the owner selected the exact P0 values below as a working hypothesis for inactive verification. Defaults and active behaviour must remain unchanged. This selects numerical goals, not a deployment profile, measured physical bounds, a six-second delivery guarantee, G2, a resource budget, workload runs, activation or release. Existing evidence must be checked for applicability; physical durations, clock errors and coverage remain open. Zero admission is a result and a limitation, not evidence of practical suitability. If the premises fail, propose a concrete justified correction rather than treating them as proved. This ledger retains the corrected split of R's runtime allowance and the full target tail.

**Parameter checks and proposed availability scope (2026-10-24).** The inactive [`p0_parameter_binding_test.go`](../../internal/core/findnode/p0_parameter_binding_test.go) checks the selected values with original TLS fixture sessions: five R requests at the common receive point produce four admissions and one refusal without extra debit; six T projections fit strictly before the start deadline, the seventh is refused, and an actual Take at the boundary is refused. The corrected run passed three ordinary and three race test results, with scoped lint reporting zero issues. The first ordinary attempt failed because its test oracle incorrectly required the diagnostic publication sequence to remain unchanged on refusal; that attempt remains FAIL. These are logical fixture checks, not six completed signatures, a physical 500ms measurement, or qualification of coverage.

No numerical increase is justified by the registry counterexample: any finite visit limit still has a larger counterexample when the registry population is unbounded. A concrete **proposal, not an owner-selected deployment**, is to retain 512/512/1024 and consider three lifetime bounds as one possible availability premise: complete registry population ≤1024, eligible original members ≤512, and simultaneously observed proof-transport lifetimes ≤512, including accepted transports before registration. Registry accounting must include non-F1, initialization, unregister and detached teardown; observer capacity and `Budget.Used` alone do not prove these bounds. Exceeding capacity must preserve refusal or UNKNOWN, never a successful partial snapshot. Even within these bounds, fresh authentic witnesses, a complete consistent cut, available tokens/places and the physical timing and clock premises remain necessary. This proposal removes specific capacity counterexamples only; it does not promise positive admission or change configuration, defaults, active behaviour or G2.

**Quantifiers.** Conditional structural safety concerns work actually admitted through the original complete/fresh producer cut and final owner/debt/deadline checks. A global cap is not required for that claim; 1025-key refusal is lost availability, not unsafe partial admission. Caps alone promise neither tokens nor service. Physical clocks/tails and constructor-specific timed bounds remain open; no 6s delivery, G2 or configuration change follows.

| Selected P0 hypothesis | Value | Status |
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


**Static coverage finding, not a run.** In `internal/core/node/conn_registry.go`, `forEachConnIDLocked` walks primary `s.conns` keys. `findNodePriorityMembers` charges each visit before eligibility; session/CM views do not add a second visit for the same transport, but distinct transports, non-F1 and initializing registered entries count. `MaxActive=512` bounds observer cells and does not cap sockets. The current default shared connection ceiling and incoming limit are off, so 512 eligible members does not establish a maximum of 1024 registry keys. A source-level counterexample is 1025 registered non-F1 entries with zero eligible members: the bounded walk must refuse with no partial snapshot. This is not an executed connection experiment. Even a future positive connection budget needs a registry-lifetime argument covering unregister and detached CM teardown; `Budget.Used` alone is insufficient. Practical capture availability for unrestricted populations is not established. A bounded supported envelope is one possible premise, not a new safety condition for already admitted work. Complete-or-refuse and the separate bounds for untimed/cached/coalesced paths remain required.

Aggregate admission remains bounded option A. Empty-calendar beta4/W1 gives an upper bound5; collection at a+100ms gives upper bound4, not a guaranteed count. Existing lower-level debt/reservations, occupied places, other R/direct clients and actual elapsed time can reduce admission to zero. Preserve atomic debit only on acceptance, one provisional/session/round, rotating L1 admission, cancellation debt and first256/overflow limitations. Four idle target projections at0/.25/.5/.75s fit strictly before latestSignStart=r+1.5s, but calendar fit proves neither physical500ms completion nor T admission for each R admission.


**Applicability audit of the selected P0 hypothesis (2026-10-23).** The existing [`find_node_priority_test.go`](../../internal/core/node/find_node_priority_test.go) exact-visit/non-F1 tests and [`find_node_owned_capture_test.go`](../../internal/core/node/find_node_owned_capture_test.go) original raw-coverage tests check refusal without a partial result at their small boundary profiles. They support the complete-or-refuse mechanism, not a measured 512/512/1024 deployment envelope. The accepted Ready/chain receipts contain genuine local Runtime samples alongside explicitly controlled fixture clocks; they do not retain a correlated physical a/f/r/c/h/u trace. The target timing fixture uses 375ms/62.5ms, not P0's 2s/500ms. Successful strict checks against separate Take and Complete deadlines do not by themselves enforce Take→Complete≤500ms when Take starts early. The successful Complete sample also precedes publication, unlock, queue and writer work, all of which must remain in the full c→h tail.

The missing observation scope is therefore specific: retain the original six owner samples and immutable attempt correlation, T Take and assigned-slot/wake brackets needed to distinguish reserve time and intentional Q, plus owner/boot identity and completeness status. Existing traffic capture fires after Write/Finish or later receive classification and can lose bounded-ring events; it cannot substitute exact endpoint samples. Cross-owner elapsed values cannot be subtracted; same-read UTC metadata does not validate clock rate, offset, suspend behaviour or error. Missing endpoints or clock premises remain UNKNOWN. Finite observed durations may falsify P0 or qualify the observed case, but cannot establish a universal OS/network upper bound. This audit adds no collector, code, default, workload or timing guarantee. The unrestricted-registry counterexample above remains a limitation; no visit-limit increase or restricted deployment has been selected.

Only these evidence gaps remain for this proposal; no new framework or run is authorised:

| Existing suitable scoped evidence | Necessary missing evidence |
|---|---|
| QA355/A/calendar/overflow, and original membership/raw-capture coverage controls in `find_node_connection.go` | Match maximum burst, occupied lower-level debt and genuine session rotation to this profile. For practical512/1024 availability, justify the supported visited registry population, including nonmembers/registered/accepted/CM-post-setup entries;512 eligible sessions alone does not prove1024 visits sufficient. Preserve complete-or-refuse, not first-M acceptance |
| N-in→Ready142-event composition; original A→R→T→R→A chain5 events/mode (0a580eb9/7b78af2f) | Reuse original TLS/F1/read/writer/target-signature custody. These fixture-clock receipts do not supply one physical six-event duration record or this profile's clock/error bound |
| Explicit target timing25 (c9ebf811/c7700734) | Qualify actual Take→Complete500ms, Complete→original target writer→R read tail and R pre/post work under the declared R/direct contention and supported environment. Inventory existing observations first; add only an indispensable missing observation after separate review |
| Existing bounded constructors and degree/capacity/hint checks | Map512/512/1024 to actual session/connection limits and maximum registry multiplicity; inventory retained/transient allocations and obtain separately agreed CPU/RSS/churn evidence. Structural integer checks and d_win≤64 are not whole-node resource acceptance |

No existing receipt is relabelled as a fresh P0 measurement. S6 Windows/crypto/resource admission, release/observation, final13b-D1, PR6 activation and D1 release remain separate. No typedlookup/Ready/N-in permutation is added as a new G2 gate.

**Inactive original T sample source (2026-10-24).** The timed original work now retains its existing successful Take and Complete samples in one bounded per-work record; a package-private reader returns values under the original target lock. The immutable original work supplies owner/source/attempt correlation. There are no additional clock calls, admission checks, timers or active defaults. Runtime/Fixture marks clock provenance only, not accuracy or boot calibration. Missing or failed endpoints remain UNKNOWN; replay cannot replace a successful pair. The Complete sample is before cache/publication/writer tails, even though it is retained only after successful completion.

Directed checks passed 62 ordinary and 62 race events (9 new observation events and 53 existing timing/admission/source-guard regressions), with scoped lint reporting zero issues. A controlled fixture Take at100ms and successful Complete at850ms retains the legal original outcome and reports an observed750ms span above the500ms reserve; equality is not an excess. Thus the reserve is not silently enforced as a new deadline. A span within reserve does not prove a physical bound. The full six-event trace, target tail, R pre/post, clock-error model, coverage envelope and G2 remain open. RAW: `/private/tmp/corsa-D1-P0-target-span-QA-ROOT-v1/result.json`.

**One successful Runtime observation (2026-10-24).** The same Runtime child now retains its earlier no-pair-before-completion assertion, then executes the original Take, a genuine signature and successful Complete. The selected child produced two Go RUN/PASS events including its parent in each of ordinary and race mode; scoped lint reported zero issues. The original owner-clock spans were 54,000 ns and 261,500 ns respectively, both recorded without imposing a 500 ms test deadline. These are two observations of one in-process owner fixture path with logical session inputs, not clock calibration, Node/CM/shared-writer or contention evidence, an upper bound or G2 approval. Complete still precedes the target tail. RAW: `/private/tmp/corsa-D1-P0-Runtime-Tspan-QA-ROOT-v1/result.json`; original samples: `observed-spans.json` in that directory.

**Exact P0 registry-visit boundary (2026-10-24).** `TestOwnedRawP0RegistryVisitBoundaryHasNoPartialCoverage` uses one existing owned-service fixture with `MaxMembers=512` and `MaxRegistryVisits=1024`. With exactly 1025 adversarial nil registry keys, both original readers return `ErrCapacity`, zero priority coverage and no partial raw facts. Removing one key permits a complete traversal with empty eligible output and member-vector capacity 512. This checks bookkeeping at the chosen visit limit, not 1025 live connections, 512 active transports or positive admission; the fixture observer still has `MaxActive=2`. The new parent passed once in ordinary and once in race mode, with no FAIL/SKIP and zero scoped node lint issues. Complete-or-refuse is preserved; practical availability for unrestricted registry populations remains unproved. RAW: `/private/tmp/corsa-D1-P0-registry-visit-boundary-QA-ROOT-v1/result.json`.

### Русский — рабочая гипотеза выбрана, эксплуатационный профиль не выбран, активации нет

**Проверка параметров и предложение области доступности (2026-10-24).** Неактивный [`p0_parameter_binding_test.go`](../../internal/core/findnode/p0_parameter_binding_test.go) проверяет выбранные числа на исходных TLS fixture-сессиях: пять запросов R с общей точкой получения дают четыре допуска и один отказ без лишнего дебета; шесть проекций T укладываются строго до срока начала, седьмая отклоняется, фактический Take на границе также отклоняется. Исправленный запуск дал три ordinary и три race результата PASS, scoped lint — ноль замечаний. Первая ordinary-попытка упала из-за ошибочного ожидания теста, что диагностический publication sequence не меняется при отказе; та попытка остаётся FAIL. Это логические fixture-проверки, не шесть завершённых подписей, не физический замер 500мс и не квалификация coverage.

Контрпример registry не обосновывает произвольного увеличения числа: при неограниченной population у любого конечного visit-limit есть больший контрпример. Конкретное **предложение, ещё не выбранное владельцем**, — сохранить 512/512/1024 и рассмотреть три lifetime-предела как одну возможную предпосылку доступности: полная registry population ≤1024, подходящие исходные members ≤512 и одновременно наблюдаемые proof-transport lifetimes ≤512, включая accepted до регистрации. Учёт registry должен включать non-F1, initialization, unregister и detached teardown; observer capacity и одного `Budget.Used` недостаточно. Превышение сохраняет отказ либо UNKNOWN, никогда успешный частичный snapshot. Даже внутри области нужны подлинные свежие witnesses, полный согласованный cut, доступные токены/места и физические предпосылки времени и часов. Предложение устраняет только конкретные capacity-контрпримеры; положительный допуск не обещается, конфигурация, defaults, активное поведение и G2 не меняются.

**Кванторы.** Условная structural safety относится к работе, действительно допущенной через исходный полный/свежий producer cut и финальные owner/debt/deadline checks. Глобальный cap для этого не нужен: отказ на1025 ключах теряет доступность, а не допускает unsafe partial. Сам cap не обещает токенов или сервиса. Физические часы/хвосты и timed bounds конкретного конструктора открыты; гарантия доставки за6с, G2 или изменение конфигурации не следуют.

Владелец 2026-10-23 выбрал точные значения P0 из таблицы как рабочую гипотезу для неактивной проверки: R 2/2/1с, внутри runtime 400/600мс; timed T 2с/500мс, Round/Slack 100/100мс, signer 250мс/8, начальные 4 токена, coverage 512/512/1024. Defaults и активное поведение менять нельзя. Выбраны численные цели, а не эксплуатационный профиль, доказанные физические границы, гарантия доставки за 6с, G2, ресурсы, нагрузочные запуски, активация или выпуск. Применимость прежних свидетельств, физические длительности, погрешности часов и достаточность coverage остаются открытыми. Нулевой допуск — результат и ограничение, а не подтверждение практической пригодности. Если предпосылки окажутся несостоятельными, нужна конкретная обоснованная корректировка. Единый бюджет сохраняет исправленное разбиение R-runtime и полный хвост T.

`P_pre≤400мс` включает сбор раунда, планирование, обработку, ожидание application-lock и передачу исходному forward-writer до Take;100мс Round и100мс Slack входят в эти400мс, остальные pre-операции получают не более200мс. Исключено только намеренное ожидание будущего назначенного token/calendar slot (`Q`). `P_post=u−h≤600мс` включает исходное чтение/capture ответа R, обработку, locks и answer-writer Take. Итого `D_R=400+600=1000мс`, а не по секунде каждой стороне. Это пока недоказанные физические предпосылки, не новые enforced subdeadlines.

Разбиение `f−a=P_pre+Q` учитывает всю очередь до forward Take. Scheduler lateness и locks относятся к `P_pre`; повторно прибавлять `P_pre` после полного `f−a` нельзя. `D_N=2с` покрывает оба остаточных интервала `(r−f)+(h−c)`, включая весь хвост T после sample Complete: publication/unlock/return, marshal/queue, общий writer с backlog/Take/Write и обратную сеть/исходное чтение R. `c−r<2с` требует успешного допуска и подписи применимым timed T. При фактическом `f≤a+1с` формула выше даёт `u−a<5,6с`.400мс до6с — только номинальный арифметический остаток, не принятый error budget и не измеренный запас.

Для разных owner-clock нужен явный переход к физическим длительностям: rate/offset/error для каждого слагаемого и forward deadline. Положительная совокупная ошибка должна была бы строго поместиться в номинальные400мс; такого доказанного/выбранного предела здесь нет. Raw Instants разных owners не вычитаются как свидетельство. Без предпосылок физическая гарантия не доказана. Answer Take не равен завершению Write/доставке инициатору; untimed/cached/coalesced случаи требуют собственного applicable-bound доказательства.


**Статический вывод о coverage, не прогон.** `forEachConnIDLocked` в `internal/core/node/conn_registry.go` обходит ключи основного `s.conns`; `findNodePriorityMembers` списывает visit до проверки пригодности. Session/CM views не удваивают один транспорт, но отдельные транспорты, non-F1 и зарегистрированные промежуточные состояния считаются. `MaxActive=512` ограничивает ячейки наблюдателя, а не сокеты. При нынешних выключенных по умолчанию общем и входящем потолках 512 подходящих участников не доказывают максимум 1024 registry keys. Контрпример на уровне исходников: 1025 зарегистрированных non-F1 записей и ноль подходящих участников — обход обязан отказать без частичного snapshot. Это не эксперимент с 1025 соединениями. Даже будущий положительный бюджет требует учесть unregister и detached teardown CM: одного `Budget.Used` недостаточно. Практическая доступность capture при неограниченной population не установлена. Ограниченная область — возможная предпосылка доступности, не новое условие safety уже допущенной работы. Complete-or-refuse и отдельное обоснование untimed/cached/coalesced сохраняются.

Вариант A сохраняется: предел5 при пустом календаре beta4/W1 и предел4 после100мс сбора — верхние оценки, не обещание допуска. Уже занятые lower-level токены/места, другие R/direct клиенты и время могут дать ноль. Списание атомарно при принятии; одна provisional/сессия/раунд, L1 rotation, долг после отмены и ограничения первых256/overflow прежние. Четыре idle signing slots0/.25/.5/.75с укладываются до r+1.5с, но это не замер500мс и не обещание T admission каждому принятому R запросу.

Остаток свидетельств ровно из таблицы: привязать существующие QA355/coverage к профилю; не считать512 подходящих сессий доказательством достаточности1024 visits с nonmember/registered/accepted/CM-post-setup; сохранить complete-or-refuse. Принятые N-in/Ready142 и chain5 подтверждают исходные TLS/F1/read/writer/signature пути, но при fixture clocks не дают единого физического журнала шести событий. Target timing25 не заменяет физическое доказательство500мс, хвоста T, pre/post R при объявленной конкуренции и модели часов. Сначала инвентаризуются готовые observations; только необходимый отсутствующий observation обсуждается отдельно. Для coverage нужны реальные session/connection bounds, максимальная кратность registry, retained/transient allocation и отдельно согласованные CPU/RSS/churn данные. Новый framework, прогоны или permutation-тесты этим приложением не назначаются. Старые receipts не становятся свежими P0-замерами; S6/Windows/экспертиза/наблюдение/13b/PR6/выпуск D1 остаются отдельными.

**Применимость свидетельств к выбранной гипотезе P0 (2026-10-23).** Существующие тесты exact-visit/non-F1 в `find_node_priority_test.go` и исходного raw-coverage в `find_node_owned_capture_test.go` проверяют отказ без частичного результата на малых граничных профилях. Они подтверждают механизм complete-or-refuse, а не измеренную пригодность 512/512/1024. В принятых Ready/chain есть настоящие локальные Runtime samples вместе с явно управляемыми fixture clocks; единого физического журнала a/f/r/c/h/u нет. Target timing fixture использует 375мс/62,5мс вместо P0 2с/500мс. Отдельные строгие проверки сроков Take и Complete сами не ограничивают Take→Complete 500мс при раннем Take. Sample успешного Complete предшествует publication/unlock/queue/writer; весь этот хвост остаётся в c→h.

Конкретно недостаёт исходных шести owner samples с неизменяемой привязкой к попытке, T Take и границ assigned-slot/wake для отделения reserve и намеренного Q, owner/boot identity и признака полноты. Нынешний traffic capture срабатывает после Write/Finish либо позднее receive-classification и допускает потери bounded-ring, поэтому не заменяет точные точки. Elapsed разных owners не вычитаются; same-read UTC не доказывает rate/offset/suspend/error часов. При отсутствии точек или предпосылок часов остаётся UNKNOWN. Наблюдённые длительности могут опровергнуть P0 либо подтвердить отдельный случай, но не универсальный верхний предел ОС/сети. Аудит не добавляет collector, код, defaults, нагрузку или гарантию времени. Контрпример неограниченного registry выше остаётся ограничением; увеличение visit-limit или ограниченный deployment не выбраны.

**Неактивный источник исходных samples T (2026-10-24).** Timed original work сохраняет уже существующие успешные samples Take и Complete в одной ограниченной записи на work; package-private reader возвращает копию значений под исходным target lock. Привязку owner/source/attempt задаёт неизменяемый исходный work. Новых clock calls, проверок допуска, таймеров или активных defaults нет. Runtime/Fixture обозначает только происхождение часов, не точность и не boot calibration. Отсутствующие или неуспешные endpoints остаются UNKNOWN; replay не заменяет успешную пару. Sample Complete предшествует cache/publication/writer tails, хотя сохраняется лишь после успешного завершения.

Направленные проверки дали62 ordinary и62 race PASS-события (9 новых observation и53 прежних timing/admission/source-guard регрессии), scoped lint — ноль замечаний. Управляемый fixture с Take на100мс и успешным Complete на850мс сохраняет разрешённый исход и показывает наблюдаемый интервал750мс выше резерва500мс; равенство превышением не считается. Резерв не превращён в новый enforced deadline. Интервал в пределах резерва не доказывает физическую границу. Полный журнал шести событий, хвост T, R pre/post, модель ошибки часов, coverage envelope и G2 открыты. RAW: `/private/tmp/corsa-D1-P0-target-span-QA-ROOT-v1/result.json`.

**Один успешный Runtime-замер (2026-10-24).** Тот же Runtime-подслучай сохраняет прежнюю проверку отсутствия пары до completion, затем выполняет исходный Take, настоящую подпись и успешный Complete. В ordinary и race выбранный подслучай дал по два Go RUN/PASS-события вместе с родителем; scoped lint — ноль замечаний. Исходные интервалы owner clock составили 54 000 нс и 261 500 нс соответственно, без введения тестового deadline 500 мс. Это два наблюдения одного внутрипроцессного owner fixture-пути с логическими session-входами, не калибровка часов, свидетельство Node/CM/общего writer или конкуренции, верхняя граница либо утверждение G2. Complete по-прежнему предшествует хвосту T. RAW: `/private/tmp/corsa-D1-P0-Runtime-Tspan-QA-ROOT-v1/result.json`; исходные samples — `observed-spans.json` в той же папке.

**Точная граница registry visits P0 (2026-10-24).** `TestOwnedRawP0RegistryVisitBoundaryHasNoPartialCoverage` использует один прежний owned-service fixture с `MaxMembers=512` и `MaxRegistryVisits=1024`. При ровно 1025 adversarial nil-ключах реестра оба исходных считывателя возвращают `ErrCapacity`, нулевой priority coverage и отсутствие частичных raw facts. После удаления одного ключа обход завершается с пустым eligible output и ёмкостью member-векторов 512. Это проверка учёта на выбранном visit-limit, а не 1025 живых соединений, 512 active transports или положительного допуска; observer fixture по-прежнему имеет `MaxActive=2`. Новый родитель прошёл по одному разу в ordinary и race, без FAIL/SKIP; scoped node lint — ноль замечаний. Complete-or-refuse сохранён, практическая доступность при неограниченной registry population не доказана. RAW: `/private/tmp/corsa-D1-P0-registry-visit-boundary-QA-ROOT-v1/result.json`.
