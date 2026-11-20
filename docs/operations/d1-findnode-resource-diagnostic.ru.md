# Закрытое диагностическое построение FindNode

17 ноября 2026 прошли закрытые проверки small2, churn8 и churn32 на `dev`; 20 ноября — Full512 active/mixed и standalone churn512 на закреплённых диагностических срезах. Обычные сборки сохраняют штатное поведение. [English version](d1-findnode-resource-diagnostic.md)

## Small2 · полезная работа и память

На macOS arm64 прошли `TestD1OriginalInitiatorReadyRelayReadyTargetChain` и `TestOwnedRawP0VisitOverflowPreservesOriginalMembersOnRecovery`. Первая проверка охватывает три завершённых запроса, ограничение четвёртого, отмену и таймаут. Вторая проверяет отказ при 1025 посещениях и восстановление полного обхода на 1024 в тестовом реестре.

Для замера на Linux arm64 собрали `cmd/corsa-node` с тегом `d1_resource_probe` из того же среза с локальной заменой конструктора на `NewD1FindNodeResourceDiagnostic` и чтением счётчиков intake раз в 250 мс. Драйвер использовал реальные loopback TCP и TLSv2. Клиент цели подписывал тестовый ответ. Генератор находился в отдельной cgroup и разделял с демоном только loopback namespace.

| Проверка | Результат |
| --- | --- |
| Полезная работа | Два ответа прошли проверку подписи, target, challenge и привязки к исходному запросу. Один до добавления третьей доказанной сессии и один после её закрытия. |
| Отказ в перегрузке | Во время третьей сессии запрос не был переслан за шестисекундное окно наблюдения. Итоговые счётчики владельца `Offered/Committed/Taken/Answered = 2/2/2/2`. Третий запрос не дошёл до intake. |
| Группа демона | 1 CPU, 512 000 000 байт, 64 PID, `memory.swap.max = 0`. Ядровый `memory.peak = 24 408 064` байт после штатного выхода демона. Пик включает keeper и чтение intake. События `max/oom/oom_kill = 0/0/0`. |
| Драйвер и очистка | Драйвер завершился с кодом 0 при отдельном лимите 256 MiB без swap. Пик его памяти не измеряли. Оба собственных контейнера удалены, heavy-lock освобождён. Использован имеющийся образ Alpine arm64. |

Первый проход завершил полезную работу, но валидатор подменил `memory.max` счётчиком `memory.events:max` при разборе двух одноимённых полей. Его результат сохранён отдельно. Принятый повтор использовал те же бинарники и нагрузку, с отдельными именами для лимита и счётчика. Пик характеризует только small2 и эту диагностическую сборку. Полная нагрузка D1, верхнее покрытие, физические сроки P0 и публичное включение остаются отдельными проверками.

- SHA-256 диагностического демона `1e90e811e69905fd79f9d91a930663b0203a44a1149018dac53148eb6cf0dfff`
- SHA-256 драйвера `e3e0696d1a5c0adc0dc9df481e67d7cef7d5579dbf68644481e3052e42094d4b`

[Журналы и хеши стенда](../_internal/d1-artifacts/2026-11-17/d1-small2-4fcc2f7/SHA256SUMS) · [Результат повторного запуска](../_internal/d1-artifacts/2026-11-17/d1-small2-4fcc2f7/run-3290b05a0403/result.json)

## Churn8 · смена соединений и память

Замер на Linux arm64, исходный коммит `d5d4934a4cf9de1ec2ccdd839b7a820a19719640`. В отдельном снимке исходников диагностический `cmd/corsa-node` выбирал Ready-конструктор с границами **8 active / 8 members / 16 visits**. Профиль small2 на штатном дереве и обычная сборка узла сохранены. Параметры времени, relay tokens и signing остались прежними. Сборка Go 1.27.4, `CGO_ENABLED=0`, тег `d1_resource_probe`.

| Проверка | Результат |
| --- | --- |
| Смена соединений | Демон подтвердил **8 → 4 → 8 → 0** зарегистрированных TLSv2/F1-сессий. За прогон прошло 12 разных identity. У четырёх оставшихся сессий ConnID сохранились, у четырёх замен — новые ConnID. |
| Полезная работа | **14 проверенных подписанных ответов FIND_NODE**. Четыре до закрытия, два на оставшихся соединениях и восемь после подключения новых. Каждая новая identity участвовала в полезной работе. Счётчики intake `Offered/Committed/Taken/Answered = 14/14/14/14`. |
| Группа демона | **28 385 280 байт** по ядровому `memory.peak` при лимите **512 000 000 байт**, 1 CPU, 64 PID и `memory.swap.max = 0`. В пик входят keeper и считывание исходных membership/intake до финального среза после выхода демона. События достижения лимита, OOM и OOM kill — по нулю. |
| Драйвер | Отдельный пик **31 608 832 байта** при лимите 256 MiB без swap. Сеть только loopback, образ Alpine arm64 использован локально. Обе роли завершились штатно, тестовые контейнеры удалены, heavy-lock освобождён. |

На первом проходе рабочие ответы и срезы состава сессий были сохранены. Ошибка шаблона проверки строки OOM драйвера прервала снятие финального пика демона. Повтор прошёл с прежними бинарниками и явной записью событий cgroup. Churn8 подтверждает работу под лимитом в этом сценарии. Покрытие 512/512/1024, полная нагрузка D1 и завершение всех retirement callbacks остаются отдельными проверками.

- SHA-256 диагностического демона `dfaf6952ab31012cff317d3a9463255c21b5f792c06d6a6b2e9c98579bbae7bd`
- SHA-256 драйвера `88a6ea6580ac2e213b86deebc4c9b13b7b28a968c99905fd7b977b174d976f40`

[Журналы, обвязка и хеши](../_internal/d1-artifacts/2026-11-17/d1-churn8-d5d4934/SHA256SUMS) · [Квитанция замера](../_internal/d1-artifacts/2026-11-17/d1-churn8-d5d4934/run-fc02f47b15b5/result.json)

## Churn32 · восстановление после смены 16 соединений

Linux arm64/CGO0, Go 1.27.4, тег `d1_resource_probe`. Исходный срез — `dev` `5a4713c1a1c3d3ca60dc35de2fc3ad7500985c21` с локальным исправлением допуска по identity. Диагностическое наложение задаёт границы 32 active / 32 members / 64 visits. [Исходники и хеши](../_internal/d1-artifacts/2026-11-17/d1-churn32-after-g2-identity/SHA256SUMS)

| Проверка | Результат |
| --- | --- |
| Смена соединений | Демон подтвердил **32 → 16 → 32 → 0** TLSv2/F1-сессий и **48** разных identity. Оставшиеся сессии сохранили ConnID, 16 замен получили новые ConnID. |
| Полезная работа | **56** подписанных ответов прошли проверку подписи, target, challenge и исходного request ID. По этапам получено **16 → 8 → 32 → 0**. Intake зарегистрировал по 56 `Offered/Committed/Taken/Answered`, по нулю `Refused/Replaced/Violations`. |
| Группа демона | **42 528 768 байт (40,56 MiB)** по `memory.peak` после выхода демона, вместе с keeper. Лимит **512 000 000 байт**, 1 CPU, 64 PID, `memory.swap.max=0`. События `max/oom/oom_kill` по нулю. |
| Драйвер | Пик отдельной cgroup **39 698 432 байта (37,86 MiB)**, лимит 256 MiB без swap. Обе роли завершились с кодом 0. Собственные контейнеры удалены, heavy-lock освобождён. |

Пик относится к этому сценарию и границе финального чтения. Поздний хвост keeper и атрибуция shared pages остаются UNKNOWN. Верхние границы покрытия проверены отдельно в следующем разделе. [Квитанция замера и хеши ELF](../_internal/d1-artifacts/2026-11-17/d1-churn32-after-g2-identity/run-7888ec0d26ac/result.json)

## Full512 · границы и восстановление

20 ноября два отдельных прогона NativeVZ подтвердили покрытие указанных верхних границ. Оба использовали один Linux arm64/CGO0 test ELF на Go 1.27.4: база `5a4713c1a1c3d3ca60dc35de2fc3ad7500985c21` и девять точных локальных файлов из [манифеста](evidence/d1-full512-2026-11-20/SOURCE-CHANGES.sha256). Это фиксированный Service с тестовыми TLSv2-клиентами, а не произвольный текущий HEAD или standalone corsad.

| Проверка | Active | Mixed |
| --- | --- | --- |
| Граница | 512→513→512 | TLSv2/registry 1024→1025→1024; F1 members 512, без F1 512→513→512 |
| Работа и переполнение | Два подписанных ответа; запрос при 513 сессиях отклонён до intake | При 1025 visits полный состав пуст, forwarding запрещён, счётчики не меняются. Исходный запрос получает подписанный ответ во время overflow; после восстановления admitted counters увеличиваются ровно на один |
| Квота | 1037 frames / 38 186 bytes, превышения нет | 1035 frames / 37 699 bytes, превышения нет |
| Go-тест | PASS, 1,13 с | PASS, 2,58 с |
| VM/worker | Штатная остановка и reap за 1,84 с | Штатная остановка и reap за 3,25 с |

Каждый гость получил **2 vCPU и 1536 MiB**, без NIC, дисков и host sharing. Это **не замер под лимитом 512 000 000 байт**: в госте одновременно работают Service и генераторы. Host work/cleanup/fences — 360/380/398/400 с; console ≤4 MiB, native ≤16 KiB. Общие host hard caps и запрет host swap не обеспечены. FD preflight в обоих случаях: soft/hard 4096, observed 6. Полные RAW, точные SHA, штатный lifecycle и отсутствие собственных процессов независимо сверены ROOT/TESTER/REVIEWER. Оба собственных lock сняты после свежих проверок; физическое освобождение всех XPC-ресурсов остаётся UNKNOWN.

Подготовительные отказы сохранены отдельно. Первый выявил отсутствующий `/dev/null` в initramfs; затем supervisor достиг срока до первого Go-вывода. Узкая диагностика подтвердила неготовность гостевого RNG (`getrandom` EAGAIN) при отсутствующем entropy-устройстве. Добавлено штатное virtio entropy: следующий гость сразу подтвердил готовность RNG и запустил тот же тест. Криптографический RNG и продуктовый код не подменялись. Точный стек первоначально заблокированного процесса не получен. Исправлена также привязка preflight к точному caller-файлу `find_node_p0_full_coverage_linux_test.go`: оба промежуточных вызова используют `t.Helper()`. Проверки неправильных provenance, FAIL, неполного RAW и overflow сохранены; 21 QA-проверка v8 прошла.

[Результат и ограничения](evidence/d1-full512-2026-11-20/RESULT.json) · [Active replay](evidence/d1-full512-2026-11-20/active/replay.json) · [Mixed replay](evidence/d1-full512-2026-11-20/mixed/replay.json) · [RAW и хеши](evidence/d1-full512-2026-11-20/SHA256SUMS). Пути в исторических HANDOFF — происхождение входов, не команда повторного запуска.

## Churn512 · полезная работа под общим лимитом 512 МБ

20 ноября standalone диагностический corsad прошёл сценарий на Linux arm64/CGO0, Go 1.27.4. Исходный коммит `93eb8fe75d2a90710d21409c535276c6c7ed28f2` дополнен точным закрытым наложением; исходники и SHA ELF сохранены в [пакете свидетельств](evidence/d1-churn512-2026-11-20/README.md). Это диагностический срез, обычные конструкторы остаются выключенными.

| Проверка | Результат |
| --- | --- |
| Соединения | **512→256→512→0** TLSv2/F1, **768** разных доказанных identity. У оставшихся соединений ConnID сохранились; замены получили новые ConnID. |
| Полезная работа | **896** проверенных подписанных ответов: 256 + 128 + 512. Все 768 identity участвовали как отправители; **512** разных identity подписывали ответы о себе. Go PASS за **101,23 с**. |
| Группа corsad + keeper + sampler | Общие **1 CPU / 512 000 000 байт / 64 PID**, swap 0. Финальный ядровой пик **229 769 216 байт (219,13 MiB)** после reap демона и sampler; inclusive CPU **8 782 556 мкс**. Ничего не вычиталось. |
| Полнота | **201** sample, максимальный разрыв **0,539 с**. Все четыре состава подтверждены. Intake Offered/Committed/Taken/Answered по 896, Refused/Replaced/Violations по нулю. Memory max/OOM/OOM-kill и PID-limit events — по нулю. |
| Драйвер | Отдельные **1 CPU / 256 MiB / 32 PID**, swap 0. Пик **94 429 184 байта**, CPU **1 305 476 мкс**. Обе роли — normal exit0 и полный EOF. |
| Завершение | Удалены четыре собственных контейнера и три тома; собственный heavy-lock снят. ROOT, TESTER и REVIEWER независимо сверили сохранённые данные. |

Секретный preparer и topology-helper работали последовательно, каждый с 0,25 CPU / 128 MiB / 32 PID и единственной своей capability; до полезной работы оба удалены. Максимум два работающих контейнера, offline, прежние 180 с работы с подготовкой +30 с очистки. Нужный локальный образ сохранён для последующих проверок; чужие ресурсы не удалялись.

Первый полноценный v6 сохранился как FAIL/INCOMPLETE: 512 ответов, затем отказ при 513-й новой цели одной source group. История целей живёт десять минут и не сбрасывается закрытием соединения. В новом драйвере изменена только направленность первых 256 запросов: те же 896 операций и 768 участников теперь используют 512 разных целей. Защита, таймеры и бюджеты сохранены; валидатор дополнительно проверяет точное множество целей. Отдельно сохранён отказ пересборки из-за гонки счётчика с удалением временных файлов Go; узкое исправление обвязки прошло 19 проверок, runtime — 61.

Это подтверждённый ресурсный результат **выбранной F1/churn-нагрузки**. Поздний keeper tail, shared-page attribution, RSS и расходы подготовки остаются UNKNOWN. Он не доказывает память mixed1024, все возможные нагрузки D1, публичную роль Responder или готовность финального PR‑6. [Закрытая ролевая авторизация получила отдельный source/QA-результат](d1-findnode-local-role.ru.md) и повтор на текущем срезе ниже. Остаются О‑1а, применимость свидетельств и оставшиеся проверки 13b-D1 на финальном кандидате, решения G2/публичного включения. Наблюдение S6 идёт параллельно и требуется для публичной активации.

[Полные RAW в XZ, точный source patch и хеши](evidence/d1-churn512-2026-11-20/SHA256SUMS) · [Результат](evidence/d1-churn512-2026-11-20/result.json) · [Независимая сверка](evidence/d1-churn512-2026-11-20/REVIEWER-DATA.json)

## Churn512 текущего закрытого ролевого среза · 20 ноября

Одна текущая попытка на подписанном Go-коммите `08c3c5ea0c7d6bc34c634060d1a5990d0ca745a6` плюс пять приватных диагностических файлов прошла **те же** нагрузку, драйвер, topology и лимиты: **512→256→512→0**, **768 участников**, **896 проверенных ответов**, **512 разных подписывающих целей**. Типизированная закрытая роль FullResponder сохранена; обычные конструкторы остаются Off. Полный сборочный срез2498 файлов сохраняет вспомогательные файлы от базы `1b97da9`; точные before/after manifests и CLI SHA `4010d590…` закреплены в [переносимом пакете](evidence/d1-churn512-current-2026-11-20/README.md).

| Измерение без вычитаний | Сохранённый старый | Текущий | Разность |
| --- | ---: | ---: | ---: |
| Пик service-group, байт | 229 769 216 | **210 948 096** | −18 821 120 |
| Общий CPU service-group, мкс | 8 782 556 | **8 653 066** | −129 490 |
| Пик драйвера, байт | 94 429 184 | 92 295 168 | −2 134 016 |
| CPU драйвера, мкс | 1 305 476 | 1 336 855 | +31 379 |

Это два отдельных наблюдения; старую попытку не повторяли. Причинную экономию, статистическое изменение CPU или стоимость одной сессии они не доказывают. Сохранены все202 sample, максимальный разрыв0,538766084 с; подтверждены четыре состава/ConnID и STATE-фазы. Intake-счётчики по896, отказы/нарушения и memory max/OOM/PID-limit events — ноль. SHA исполняемого файла проверен через настоящий `/proc`. Финальные inclusive peak/CPU прочитаны после reap демона и sampler, в группе остался keeper `[1]`. Обе роли завершились normal0 с полным EOF. Четыре собственных контейнера, три тома и heavy-lock удалены; ROOT/TESTER/REVIEWER приняли входы, RAW и свидетельства очистки.

До исполнения исправлена только передача публичной копии CLI0444: исходный build-файл, остальные восемь входов, CHOWN-only preparer и oracle не изменены. Расходы preparer/topology, RSS, shared-page attribution и поздний keeper tail — UNKNOWN. Результат относится к выбранной закрытой F1/churn-нагрузке; О‑1а, остальные сценарии13b-D1, G2/публичная активация и параллельное наблюдение S6 остаются отдельно. [Результат](evidence/d1-churn512-current-2026-11-20/result.json) · [Хеши и полные RAW](evidence/d1-churn512-current-2026-11-20/SHA256SUMS).

## Выбор и принадлежность

Конструкторы Core и headless App `NewD1FindNodeResourceDiagnostic` существуют только под точным build tag `d1_resource_probe`. Нет config flag, изменяемого capability setter, общей callback-фабрики, caller clock или экспорта clock authority. Обычные Core/App конструкторы и stock `cmd/corsa-node` остаются F1-off, в том числе при наличии тега. В обычном отслеживаемом срезе small2 нет диагностического CLI и внешнего request driver; описанные выше приватные наложения для замеров существуют отдельно.

Закрытый конструктор использует существующие реальные RuntimeDomain → Ready construction → исходные checked bindings → исходный Seal. F1 выбирается до выхода нового Service наружу или начала handshake. Не выбираются FixtureDomain, signing test gate, loaded N-in, contact reconciliation или own-DM runtime. Требуются full node, включённый listener, буквальные127.0.0.1 listen/bootstrap адреса, не более одного bootstrap endpoint, отсутствие proxy/connect-only; неподходящая конфигурация отклоняется, а не переписывается незаметно. Само построение не запускает Run/listen.

Фиксированная гипотеза политики:

| Параметр | Значение |
| --- | --- |
| Пределы coverage | 2 active / 2 members / 4 registry visits |
| Бюджеты relay target/network/runtime | 2с / 2с / 1с |
| Round/slack | 100мс / 100мс |
| Начальные relay tokens | 4 |
| Target service bound/runtime reserve | 2с / 500мс |
| Signing spacing/reservations | 250мс / 8 |

Это фиксированные диагностические входы, а не доказанные физические задержки, полное coverage512/512/1024, гарантия обслуживания восьми запросов, G2 или решение об активации.

`D1FindNodeResourceReadout.SnapshotForService(expected)` до чтения owner проверяет точные исходные Service, capture/intake/terminal/consumer/publisher, identity и done lifetime. App-обёртка `SnapshotForApp(expected)` дополнительно проверяет, что исходный App всё ещё содержит тот исходный Service. Nil, чужая, изменённая или закрытая принадлежность возвращает UNKNOWN до owner reads. Последняя проверка может увидеть закрытие во время чтений, но не делает lifecycle и snapshot атомарными. Readout не сохраняет сам Ready object или отдельную копию peer-state; same-owner handle удерживает исходный Service. Он не выдаёт admission/mutation handle.

Возвращается значение существующих `ResourceSnapshot` и `OwnedRoundPending`, без нового collector. Это независимые последние публикации и operational reads: ноль, pending hint или структурный byte floor не являются согласованным heap/RSS/cgroup измерением. `ExternalClockBound` и `PhysicalTimeBoundsProven` остаются false. Изменение возвращённой копии не меняет owner.

## Checkpoint исходного membership

Тот же tagged readout теперь предоставляет `CheckpointForService(expected)` и App `CheckpointForApp(expected)`. Это fixed-value checkpoint **только membership**: не более двух active исходных proven listener lifetimes, двух F1 members и четырёх registry visits. Возвращаются отсортированные исходные ConnID/identity и признак публикации каждой точной исходной Session как F1. У proven listener без F1 identity берётся из его исходного genuine degree witness, а не из cached hello/address label. Outbound scope, отсутствующее/противоречивое исходное свидетельство, poisoned/unknown coverage, overflow, изменившиеся owner/lifetime или generation отклоняют **весь** checkpoint: UNKNOWN и нулевые rows/counts; частичный результат не становится complete.

Копируемые изменяемые поля защищены исходными registry/common/degree mutexes. Private original capture stamps до/после должны обозначать одну завершённую shared-W generation; registry rows соединяются с degree rows по точным исходным lifetime **и witness** pointers, original ConnID и identity. Clock не измеряется и не экспортируется; не добавлены admission, Seal, degree publication, collector, сохраняемый Session handle или caller callback. DTO содержит только scalar/fixed-value. Ожидание существующих mutex может блокироваться: cardinality bound не является no-wait/physical-time гарантией. Результат описывает registered original listener membership этой generation, а не устойчивое обещание будущего membership. Ended registry tombstones расходуют visits; ноль active rows не означает пустой registry.

`RetirementStatus` и `DrainStatus` безусловно остаются UNKNOWN. Прекращение published membership не доказывает завершение deferred intake/local retirement callbacks или request lanes. Существующий resource snapshot читается отдельно и в этот DTO не входит. Равенство counts, `Intake.Refused==2`, молчание и exit драйвера не являются oracle завершения/admission/RAM.

### Свидетельства membership GoQA

Новый отдельно разрешённый offline job завершился exit0 со статусом `SCOPED_GOQA_SUCCESS_NOT_RUNTIME` и известным scoped process closure. Обе native Darwin/arm64 test-команды прошли с `d1_resource_probe` и без него. Статический inventory —35 выбранных функций с тегом/28 без, включая original raw guard baseline/negative families; это выбор функций, а не выдуманный subtest/full-suite census. Все17 записанных команд завершились0:159055 байт в34 RAW files, stderr0. Новые тесты используют genuine sessionv2/TLS по собственным **in-memory net.Pipe**, исходные proof/auth/registry/publication/End, causal generation-change barrier и negatives для exact-pointer join. Test-only quiescence background admission изолирует барьер; это не production drain claim. End probes допускают гонку, но не доказывают принудительное пересечение scheduling. Service.Run/App.Run/listener, diagnostic CLI, внешний request driver и memory workload демона не запускались.

Linux/arm64 CGO0 Core/App test ELF скомпилированы и проверены статически, **не исполнялись**. В том же scoped job проверены Go1.27.3 used-dependency metadata, invariance Source/modules/SDK и `go mod verify`, с той же составной h1-границей, описанной ниже. Subject — сохранённый Source8afa7f5 плюс закрытый22-файловый overlay: constructor16 и checkpoint11 с пятью exact chained overlaps, всего2193 скопированных source files. Это проверка конкретного original-membership construction/test subject, а не всего современного HEAD или обработки полезных подписанных FindNode requests.

| Свидетельство membership | SHA-256 |
| --- | --- |
| Checkpoint11-файловый Source | `58f5b99ee7b1846a59628708962a56305f057a5d468d7d9b8d2c0cc8bdb0b062` |
| Merged22-файловый build overlay | `a5a573e0429c5b6585e38487aa432e9095db8920e1a7d78c6e5a9f3f7675665a` |
| Manifest скопированного Source | `331bd72bfcddcb1c88697af5bafdd5ab5483dcba26d7e14aa986160f128c32d2` |
| Manifest скопированных модулей | `4b761388c2f3b52c3e18c720ef4ed7285c146edff2e0b4662ee154bd6a1c8e13` |
| Неисполненный Core Linux test ELF | `79b599f32661e399307ce84fd30848a8c7decd60bb03ce15c253d28e5999877e` |
| Неисполненный App Linux test ELF | `ac483086559a5beffca3c3ba01116b2a9183bd7df8324bf752fb1f61e1e8d45e` |

Описанные ниже soft host controls и clean-source reproducibility limitations сохраняются. Это не standalone512000000-byte qualification, согласованное RAM-наблюдение, physical P0/G2, активация или разрешение выпуска.

## Прежняя проверка только конструкторов и происхождение

Исправленный job завершился exit0 со статусом `SCOPED_GOQA_SUCCESS_NOT_RUNTIME` и известным scoped process closure. Все17 записанных команд, включая ps-проверки helper, завершились0. Фиксированный native Darwin/arm64 выбор прошёл с тегом и без него. Inventory —28 выбранных функций с тегом/25 без: actual constructor/readout/default-off и исходные Source closure/negative families. Это число выбранных функций, а не выдуманные subtest/full-suite counts. Service.Run/App.Run/listen/TLS workload не выбирались.

Linux/arm64 CGO0 Core/App test binaries скомпилированы и проверены как static ELF, но **не исполнялись**. Metadata Go1.27.3/build settings и h1 используемых зависимостей сопоставлены с source go.sum вместе с успешным `go mod verify` и неизменными pinned source/module/SDK trees. Один `go mod verify` сам по себе не гарантирует upstream h1. Stock CLI не пересобирался.

| Свидетельство | SHA-256 |
| --- | --- |
| Закрытый16-файловый Go overlay | `5622cb959a652e8896497d9500c6208ef200ec8d4a04970fc750fdd2fdc05171` |
| Manifest скопированного Source | `8befc9226535171616d996c64b5d837c2edc660cd019df12afec6325f0cc9da3` |
| Manifest скопированных модулей | `6347fce1db91ab7d047cfa18633cc370b79fe9480abb8df392ed6e9caf8176c9` |
| Неисполненный Core Linux test ELF | `5567a72a3d0fff4e5cfff3353baeaaafb606e1b24532eaf385093cd41ff0fcde` |
| Неисполненный App Linux test ELF | `6a1578a5880d1d27817347770f1d887a2f0ba4d7292df42275ef0b0394d524a3` |

Для воспроизведения Source guards нужен чистый snapshot tracked Source без ignored artifact/cache trees. Существующий whole-tree bridge-importer scanner не исключает `docs/_internal`: при прямом запуске в рабочем каталоге он может посчитать находящиеся там ignored Go-копии артефактов. Эти копии не входят в проверенный subject. Scanner не расширялся и не обходился ради них; граница чистого snapshot — условие воспроизводимости, а не заявление о PASS всей рабочей папки.

Subject — сохранённый Source8afa7f5 плюс точный16-файловый overlay, а не весь современный HEAD. Прежний настоящий Degree guard FAIL и журналы сохранены: в all-Node inventory не хватало семи точных строк для девяти `origin` identifiers существующего K5 reader. Исправление добавляет эти literal roles и четыре causal missing/duplicate/unlisted/alias negative cases; все исходные92 строки, scanner/predicate и production-конструкторы неизменны. Успешный job использовал свежий собственный work root и отдельный one-attempt grant.

Native Go settings2/-p1/GOMEMLIMIT768MiB были мягкими host controls, а не измерением демона512000000 байт или hard host RAM/CPU/PID/no-swap/network sandbox. Прежний stock SG32 memory result относится к другому бинарнику/сценарию и этот выбранный F1 path не измеряет.

## Исходное предложение ограниченной границы

До ресурсного вывода отдельно проверить закрытый tagged diagnostic entry point, канал same-App value-only readout и внешний driver с настоящими TLS/F1 publication и подписанными wire answers. Предлагаемый малый сценарий: две identity и одна более поздняя без negotiation, не более двух одновременных сессий, четыре разных request ID/challenge — один принятый remote и один local запрос, одна попытка к ended target и одна unnegotiated. Отклонённая попытка может остановиться до intake, поэтому не предполагать `Intake.Refused==2`: отсутствие нового admission/commit подтверждается фазовыми owner evidence и protocol behavior. Молчание или успешный exit драйвера сами по себе не доказывают отказ/завершение.

Эта граница, точные CLI/driver builds и один ресурсный профиль требуют отдельной подготовки/допуска. Сохранить complete-or-refuse; не выводить задержки, fairness, full-load512MB, G2 или готовность выпуска из проверки конструкторов.
