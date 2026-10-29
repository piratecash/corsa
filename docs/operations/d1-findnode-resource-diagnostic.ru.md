# Закрытое диагностическое построение FindNode

Статус: конструкторы, исходный membership checkpoint и Source-проверки прошли отдельные ограниченные offline GoQA. Это явно выбираемый диагностический путь; он не включает FindNode в stock CLI, не исполняет полезную FindNode-нагрузку, не измеряет память демона, не утверждает G2 и не разрешает выпуск. [English version](d1-findnode-resource-diagnostic.md).

## Выбор и принадлежность

Конструкторы Core и headless App `NewD1FindNodeResourceDiagnostic` существуют только под точным build tag `d1_resource_probe`. Нет config flag, изменяемого capability setter, общей callback-фабрики, caller clock или экспорта clock authority. Обычные Core/App конструкторы и stock `cmd/corsa-node` остаются F1-off, в том числе при наличии тега. Диагностического CLI и внешнего request driver в этом срезе нет.

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

## Следующая ограниченная граница

До ресурсного вывода отдельно проверить закрытый tagged diagnostic entry point, канал same-App value-only readout и внешний driver с настоящими TLS/F1 publication и подписанными wire answers. Предлагаемый малый сценарий: две identity и одна более поздняя без negotiation, не более двух одновременных сессий, четыре разных request ID/challenge — один принятый remote и один local запрос, одна попытка к ended target и одна unnegotiated. Отклонённая попытка может остановиться до intake, поэтому не предполагать `Intake.Refused==2`: отсутствие нового admission/commit подтверждается фазовыми owner evidence и protocol behavior. Молчание или успешный exit драйвера сами по себе не доказывают отказ/завершение.

Эта граница, точные CLI/driver builds и один ресурсный профиль требуют отдельной подготовки/допуска. Сохранить complete-or-refuse; не выводить задержки, fairness, full-load512MB, G2 или готовность выпуска из проверки конструкторов.
