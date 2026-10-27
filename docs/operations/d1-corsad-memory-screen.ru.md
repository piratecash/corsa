# C1/C2a/C2b32: замеры памяти настоящего демона

[English version](d1-corsad-memory-screen.md)

Последний ограниченный результат — C2b32 текущего кандидата в конце страницы. Предыдущие разделы сохраняют исходные предметы и историю.

2026-10-25. **Ограниченный замер завершён без наблюдённого OOM и превышения бюджета. Пригодность полной нагрузки D1 в512МБ остаётся UNKNOWN.** Это наши собственные измерения, не sizing из внешней документации. Остальные случаи описаны в [контрольных точках D1→DHT](../design/d1-dht-resource-checkpoints.ru.md).

## Предмет и исполнение

Настоящий headless `./cmd/corsa-node` собран Go1.27.3/CGO1 для Linux arm64/musl из `6421c8110a4c668fcf688bec6eb4c957ae37b852`; SHA256 ELF `9ba93241a3f717fb50a6221957e4e3146f982ef5fc7b68a37f90bfe7a49e55bf`,21627143байт. Source domain `1f3a92ee80123f59502b4fe39cc5119c367e4b37a247ec047550d4441b811519`. Более поздняя неактивная ready-cache правка, подготовленная в index, в этот бинарник не входит.

Исполнение — native arm64 в Linux VM Docker Desktop на macOS, без QEMU TCG. Точный локальный образ: `sha256:910cfcaf707e2051587b72b0b3baaa9ca1e05d3a6289de9e8b05f9e292b5b97e`. Это не замер bare-metal Linux, native macOS или amd64.

Демон работал в full/listener режиме с новыми закрытыми HOME/temp/data каталогами, listener127.0.0.1:64646, аутентифицированным RPC127.0.0.1:46464 и явным bootstrap127.0.0.1:64647, исключающим fallback к production seeds. Сеть — только loopback, внешних клиентов и нагрузки application-message не было. Установлен GOMAXPROCS1; GOMEMLIMIT/GOGC не переопределялись. Не проверены SQLite workload, Desktop, стоимость SDK-клиента, рост persistent state и реальные популяции peers.

Утверждённый профиль: сервису1CPU/512000000байт/no swap/64PID, внешнему измерителю0,25CPU/134217728байт/no swap/32PID;100секунд работы вместе с подготовкой и30секунд очистки, existing heavy-run lock и свежие10GiB плюс входы. Один собственный tmpfs≤1MiB и непривилегированный preparer занимали бюджет измерителя до его запуска. Краткое prep/service перекрытие сохраняло tmpfs; prep штатно завершился0 и был удалён до создания observer. Одновременно работали максимум два контейнера. Проверены UID501:20,0600credentials, read-only consumer mounts, cap-dropALL, no-new-privileges и отсутствие внешней сети. `piratecashd` не запускался, не исследовался и не использовался для sizing.

## Результаты и охват

| Метрика | Байты | Область |
|---|---:|---|
| Kernel peak cgroup сервиса |35913728|Новая группа демона, Python keeper и дочерних процессов сервиса; финальное чтение после drain демона до release keeper. Ничего не вычтено. |
| Максимум прочитанных VmHWM демона |19533824|Наблюдения kernel HWM процесса; полный lifetime VmHWM остаётся UNKNOWN. |
| HWM команды из wait4 |20766720|Может включать descendants/pre-exec; не заменяет полный HWM только демона. |
| Пик cgroup измерителя |35758080|Отдельная группа до последнего peak-read перед final record; полный post-exit пик UNKNOWN. |
| Пик preparer / расположение charge shared tmpfs |UNKNOWN|Не вычитаются из пика сервиса и не объявляются включёнными в него.1MiB — граница хранения, не измеренная RAM. |

Пик сервиса —35,913728 десятичныхМБ (34,25MiB), ниже512000000байт в этом случае. Charge cgroup, RSS/HWM и optional sampled PSS имеют разный смысл; их не суммируют в физический total. Расход измерителя не вычитают из сервиса ради меньшего результата.

Полный поток содержит286records:61обычный sample,122process records,93аутентифицированных RPC результата (каждый из трёх методов один раз при старте и30раз во время RPC-работы), один отклонённый запрос без аутентификации, listener milestone, stop/drain, initial/final network и final-scope records. Milestone связывает PID/startticks демона с inode/FD существующего listener, не означает полную готовность сервиса. От первого до последнего cost-read измерителя прошло60,118204152секунды; это не весь lifetime демона.

В63наблюдениях events сервиса (initial и62deltas) все OOM/oom_kill и события достижения лимита остались нулевыми. Identity cgroup и held FD сохранены до завершения демона и финального чтения; host flush предшествовал release keeper. Все три контейнера завершились0 и удалены, затем удалён собственный volume. Независимые запросы по точной ownership-метке не нашли контейнеров/тома. Пропуски и потеря final scope не засчитывались как успех.

## Доказательства и границы

Nonce попытки `4138adbe26d1aa3b5a17153df155cedd`; SHA256 host receipt `a7a5a242d112c877e1b584cc296f7b8afcf7ab6f4eb203636dfede94c6e57cec`, полного NDJSON `5dfa6e86384932a9a2961cb2c24d0a80c8c77205401259d3bed93add7980d85a`. Локальная сводка: `/private/tmp/corsa-D1-C1-native-secret-ROOT-package-v1/ATTEMPT-9-TERMINAL.json`, SHA256 `e9885261525738740682d15fb4d9f1b025a83fdbedc9a16f3b041076a13213d3`. Sourcev13/CODE `b782bb27…`/`4d4e87a5…`, pure controls12methods/118subtest events. Owner receipt `4ed5b722…` фиксирует утверждённый native-tmpfs профиль и необходимые проверенные повторы. Доступность этих временных файлов за пределами workspace не обещается.

Прежние попытки остановились до замера демона: обнаружены преобразование владельца host bind-файла и дополнительные Linux-интерфейсы, а не большой расход RAM. Итоговая C1-проверка допускает только девять перечисленных name/type пар приDOWN, без адресов и маршрутов; все loopback/socket проверки сохранены, original_T5_guard_qualified=false. Исходный T5 lo-only этим не закрыт. Статические ссылки на Linux объясняют типы/драйверы, не расход RAM или точную сборку установленного ядра. [UAPI types](https://github.com/torvalds/linux/blob/master/include/uapi/linux/if_arp.h), [GRE drivers](https://github.com/torvalds/linux/blob/master/net/ipv4/ip_gre.c).

Подтверждён только заявленный старт/простой/локальные RPC. Не доказаны полезный FindNode-допуск под нагрузкой,512/512/1024, максимальный всплеск, большое состояние, churn, доставка, долгосрочное удержание памяти, безопасность соседнего сервиса или итоговая стоимость DHT. Далее нужен конкретный отдельно допущенный C2 с настоящей работой и отдельными drivers. C3–C5, повтор на финальном кандидате, G2, активация и выпуск открыты; Windows/Tor непроверены/отложены.

## Более поздняя сборка текущего кандидата — 2026-10-25

Отдельный production CLI собран из текущего индексированного кода: база `6421c811` плюс восемь изменённых и два добавленных выбранных Go-файла (правка ready-cache и source/tests наблюдения общего бюджета соединений). Вход1836файлов/54894920байт имеет domain `936d030d38ad88e799de8351a895b907855f591a3cbc494f3b436c0f429f6ba3`, связан с input index seal `971349dc01af28aa1a9c21c29bff3277ea70c859315f28dd702ae605024da4ba`. Это не неизменённое committed HEAD tree. Дополнительного drift Go/embed/module/build inputs нет.

Production `./cmd/corsa-node` Linux arm64/Go1.27.3/CGO1 собран:23команды штатно завершены, статические ELF/buildinfo/musl closure сохранены. ELF21627143байта имеет SHA256 `e651bb6c71419af826d0d268b61ae2b6552c267e4f5cef3172bd2f49387c55c6`. Результат: `/private/tmp/corsa-D1-current-corsad-compile-runtime-ROOT-v1/result.json`, SHA256 `1075e26013d463d2e76f06338fe10bb65454db13ba405eb2d49093a648827cf6`. Выполнена compile/static проверка; приложение не запускалось.

Прежний пик сервиса35913728байт относится только к `9ba93241…`. Совпадение размера ELF не переносит этот замер памяти на `e651bb6c…`. Pending-пакет C2 сохраняет свой явно указанный бинарник `9ba93241…`, подмена не выполнена. Текущему кандидату нужны собственные допущенные runtime/resource проверки. Установка наблюдения, полная D1/512MB, G2, активация и выпуск остаются открытыми.


## C2a: восемь TLSv2-сессий и смена четырёх identity — 2026-10-25

Attempt 3 завершил согласованный офлайн-сценарий C2a на Linux arm64 с тем же production CLI, что C1: `9ba93241…`, база `6421c811`, source domain `1f3a92ee…`. Драйвер `63782d07…` использовал двенадцать различных peer identity в четырёх фазах: восемь доказанных TLSv2-каналов; четыре прежних после закрытия peers 1–4; восемь после подключения четырёх новых identity; затем ноль после drain. Точные ordinal: 1–8 → 5–8 → 5–12 → ни одного. Каждая непустая фаза включала успешный аутентифицированный `get_peers`; cached session diagnostics не использовались как oracle приёмки. Этот stock-бинарник не объявляет F1.

| Наблюдаемая область | Пик kernel cgroup, байт | Граница результата |
|---|---:|---|
| Группа сервиса | 35 516 416 | Включает Python keeper и descendants; keeper, baseline и shared cost не вычитались. |
| Группа драйвера | 21 585 920 | Финальное чтение удержанной cgroup после завершения Go child; keeper включён. Более поздний расход выхода keeper не подтверждён. |
| Группа observer | 41 746 432 | До последнего чтения peak перед final record; полный post-exit lifetime peak — UNKNOWN. |
| Preparer и shared tmpfs charge | UNKNOWN | Учитываются отдельно; настроенный потолок tmpfs 1 MiB не является измеренным расходом. Из пика сервиса ничего не вычитается. |

Наблюдённый VmHWM демона — 20 586 496 байт; waited-command rusage HWM — 23 830 528 байт, с другой областью. Полный lifetime VmHWM демона и непрерывный process-tree RSS остаются UNKNOWN. Пики ролей нельзя складывать в мгновенный физический расход; одиночные C1/C2a не доказывают улучшение потребления памяти.

Лимит демона: 512 000 000 байт/1 CPU/64 PID; observer: 128 MiB/0,25 CPU/32 PID; драйвер: 256 MiB/1 CPU/32 PID, без swap. Preparer использовал бюджет observer и был удалён до его запуска; одновременно работало не более трёх ролей. Предел работы — 120 секунд с подготовкой, очистки — ещё 30 секунд. Сохранённые scope checks не выявили gaps, OOM или превышения бюджета. Полный NDJSON из 105 записей содержит 22 service sample, 22 process observation сервиса, 20 driver sample, 20 process observation драйвера и все четыре protocol milestones. Драйвер учёл 336 application frames/10 304 байта; это не TLS wire-byte totals. Все четыре собственных контейнера штатно завершились с кодом 0/OOMKilled=false и были удалены; secret volume удалён. Свежие запросы по nonce не нашли собственных контейнеров/volume; heavy-run lock снят.

Attempt 1 и 2 остановились до создания контейнеров: в команде не был передан обязательный heavy-lock env, затем использовано неверное фиксированное имя владельца wrapper. Оба сохранены как LOSTRESULT/UNKNOWN — это не измеренные отказы и не успешные нулевые расходы. Перед Attempt 3 прошла отдельная readonly-проверка контекста; runtime-код, ресурсные лимиты и бинарники не менялись.

Локальные доказательства: `/private/tmp/corsa-D1-C2a-eight-peer-ROOT-package-v1/ATTEMPT-3-TERMINAL.json`, SHA-256 `63a977a7fa18acf8a1ad89fe9d12be5d718a540a47e9c7d096ecc5882c85db25`; `runs/corsa-c2a-90e20c6d45317c03a57ae579932d028a/host.json`, SHA-256 `07b19dfbfe21daf0f3d8624a423a1cd1703b4951d300b4bc99e1e3594596ae58`, внутри того же пакета. RAW samples и оба предстартовых отказа сохранены; доступность вне workspace не предполагается.

Результат относится только к зафиксированному бинарнику и ограниченному TLS/get_peers/churn-сценарию C2a. Он не подтверждает overload admission, FindNode/F1, full512, длительное удержание памяти, память текущего post-K12 кандидата, SQLite/DesktopClient, Tor/Windows, G2 или готовность выпуска. Более поздняя статическая сборка CLI `e651bb6c…` и staged K12 — другие области. Полный C2 и остальные ресурсные checkpoints D1 остаются открытыми.


## C2b32: текущий кандидат, 32 TLSv2 peers и churn16 identity — 2026-10-26

Первая попытка C2b32 завершила отдельно допущенный владельцем случай: **32 → 16 → 32 → 0**,48 различных identity. Доказанные каналы имели ordinal1–32, исходные survivors17–32, восстановленные каналы17–48, затем пустой набор. Каждая непустая фаза проверила настоящие TLSv2/session-v2 identity/каналы и успешный `get_peers`;16 новых identity не пересекались с исходными32. Четыре ожидания по5с включали дополнительное время последовательных обменов. Driver учёл **1344 application frames / 41216байт**, не меняя пределы2048frames/8MiB; это не TLS-wire bytes. Cached live census оставался только диагностикой.

Финальный0 — **source-bound свидетельство свежего аутентифицированного Node RPC**, а не вывод из выхода driver или пустого списка peers. После close/join всех собственных каналов pinned compiled driver потребовал свежие `getNodeStatus`/`fetchRouteSummary`: тот же server incarnation и enforced instantaneous loopback `active==0`, до публикации `zero-drained`. Тело финального RPC-ответа/atomic field/read timestamp не сохранено отдельным RAW-ответом; независимое review связало исполненный source/artifact с упорядоченным protocol record. Последующие drain/reap driver и drain демона — отдельные факты lifecycle.

| Наблюдённая область | Байты | Ограничение |
|---|---:|---|
| Пик cgroup сервиса |36 233 216|Включает Python keeper и потомков сервиса; ничего не вычтено. |
| Максимум sampled VmHWM демона |25 358 336|Наблюдённый HWM процесса; полный lifetime VmHWM UNKNOWN. |
| HWM команды из wait4 |26 140 672|Может включать descendants/pre-exec; отдельная область от daemon-only HWM. |
| Пик cgroup driver |24 952 832|Группа keeper+child до финального чтения после Go drain; дальнейший exit keeper не измерен. |
| Пик cgroup observer |41 172 992|До последнего peak-read перед final record; полный post-exit lifetime UNKNOWN. |
| Расход preparer/shared tmpfs |UNKNOWN|Ничего не вычтено;1MiB — предел хранения, не измеренная RAM. |

Пик сервиса36,233216 десятичныхMB ниже512000000байт **в этом случае**. Пики разных областей не являются одновременной суммой физической памяти. Continuous process-tree RSS и полный lifetime HWM демона UNKNOWN. Из старых8-peer/`9ba93241…` запусков нельзя выводить RAM на дополнительный peer или экономию: изменились код, CGO build variant и нагрузка. Opt-in K9/K10/K12 были OFF; стоимость их активного сбора этим не измерена.

Production CLI SHA256 `ae61fc926e7f5c2353a21075041300fb722f2c8346389344a6e2e145931632c7`,21627040байт — обычный `./cmd/corsa-node`,Go1.27.3/Linuxarm64v8.0/CGO0 без tags/benchmark wrapper. Source: база `e50f97511d88d7ffd7a08daa3490bee7d63f0f6a` **плюс19 staged paths**, snapshot `1fcca8012a39015fcc377472d2e56cdb82cbedef0562f07ee6acf8f6895f9f29` (2119файлов/70186204байт), не чистый committed HEAD. Driver SHA256 `90d1c2d68524a5507aa09d4e443cb70cf3c52fb6fe936a1d9280917bad7f7aa7`,6750368байт: тот же current source плюс4 приватных component post и actual CLI literal, manifest `2806e1feef432c56b4db0824ea5cfe6b9e6694178c4df7d37a890b24599cd734` (2119файлов/70197026байт). Оба ELF имеют static self-only closure без interpreter/needed DSO. Cross-build на macOS и последующее native исполнение в том же arm64 Linux Docker image910cfcaf… — разные факты. Исходный metadata-only qualifier FAIL CLI сохранён; отдельный static readback квалифицировал существующий ELF без пересборки.

Пределы прежние: сервис1CPU/512000000байт/64PID, observer0,25CPU/128MiB/32PID, driver1CPU/256MiB/32PID, все no swap;120с работы с подготовкой плюс30с очистки, максимум3 измеряемые роли. Сохранены собственный native tmpfs credentials≤1MiB,501:20/0600/read-only consumers, prep-remove до observer/driver, offline loopback namespace и held cgroup/FD guards. Полный поток105records содержит22service samples,22service process records,20driver samples,20driver process records и4protocol milestones. Initial absolute events и дальнейшие deltas не показали OOM/overrun; gaps не приняты. Final held reads и host flush предшествовали release keeper; все4 собственных контейнера завершились0/noOOM и удалены, затем удалён свой volume. Свежие exact-owner queries пусты, heavy lock освобождён.

Локальный proof: `/private/tmp/corsa-D1-C2b32-current-ROOT-package-v1/runs/corsa-c2b32-f81167849ca5419213464d803dc85831/host.json`, SHA256 `e2ce81ab4e0dfa096673bb1ac1480f139f7997d0248b2c82f89eb9afe95a3b68`; SHA256 полного NDJSON `68d63989d4bc432c1560801d55c250317db30a6e4cc85835d4c98a2b614e618c`. Независимые TESTER/reviewer приняли ограниченную область и ограничение source-bound zero witness. Файлы локальны workspace; доступность снаружи не предполагается.

Закрыт только этот32-channel TLS/get_peers/churn screen. Full C2/512-peer workload,F1/FindNode,overload,persistent/long-duration state,co-tenants,активные наблюдения,T5/Member4,G2 и выпуск D1 остаются открытыми. Финальная стоимость DHT и универсальная пригодность512MB этим не доказаны; Windows/Tor UNVERIFIED/deferred. `piratecashd` не запускался и не использовался для sizing.


## Подготовка инструмента S6 EMPTY64 — 2026-10-27

Частный инструментированный CLI собран для Linux arm64/CGO0: SHA-256 `9bea7a6ed0c50bc293f105bfbf495295770e2daf8b59fd0d187d0a0873b62d3c`, 31 355 161 байт. Явный профиль наблюдения не меняет defaults проекта. Статическая сборка и шесть интеграционных групп ниже не переносят прежние замеры памяти на этот бинарник.

Полный контроллер OFF→ON прошёл шесть pure-интеграционных тестовых групп одной Python-командой closed30 (штатный exit 0, дочерний процесс собран). Проверялись реальные helpers расписания/метрик с искусственными часами и RPC, временные counter/store-файлы, синтетические данные монтирования/процессов и RAW/EOF keeper→receiver. Покрыты полное расписание 50 секунд и 51 вызов каждого метода, пропуски метрик/границ, присутствие демона, поздний возврат, неблагоприятные исходы и ошибки очистки, запрещающие ON. Это не 50 секунд работы настоящего демона и не проверка kernel counters/прав доступа.

Ревью исправило декодирование Linux device number на macOS Host, финальную нормализацию результата после ошибки удаления тома и начало бюджета попытки. Предложенный физический сценарий использует один инструментированный бинарник в обоих режимах, новые утилизируемые stores, последовательные 64 Capture только в ON и одинаковые RPC/измерители. Полный Source остаётся неактивным и частным; runtime binding, ресурсный допуск и физический OFF/ON-замер ещё предстоят. Новых результатов памяти, CPU или дисковых затрат нет. Полная D1/512 MB, квалификация K5, G2 и выпуск открыты.

Локальные свидетельства: Source `7d1018c4…`; CODE `0e31c369…`; `/private/tmp/corsa-D1-C2-S6-empty64-fullcase-QA-ROOT-v1/result.json`, SHA-256 `e85a373010de57691209844da9b3a5be057b503eaa22fb08caf00241e5e6b9b3`. В этой pure-проверке контейнеры Docker, демон и VM не запускались.


## S6 live EMPTY64: допущенная попытка остановлена в OFF — 2026-10-27

Единственная заново разрешённая пара OFF→ON завершилась с outer exit 2 (`83f7fd` / session `27510` → `5a6a04`). В OFF демон был запущен, но observer отказал с `time-clock-reader-domain`; полное 50-секундное окно аутентифицированных RPC и ресурсный замер не получены. Host сохранил статус `LOSTRESULT`, measurement `partial`, adverse outcome `FAIL`, runtime custody `INCOMPLETE/UNKNOWN`. Строка отказа — наблюдённый исход, а не квалифицированная первопричина. ON пропущен. В OFF selector наблюдения отсутствовал: 0 начатых/завершённых/прерванных Capture и 0 marker bytes. Для ON наблюдений Capture нет; это не успешное сравнение с нулевой стоимостью.

Входом был частный инструментированный Linux arm64/CGO0 `./cmd/corsa-node`, **31 430 248 байт**, SHA-256 `64c0b82bd51842326cb279c2a7d400a8097284e4f952ec5557f6c8eff031df90`, source domain `98ce7427d3c15ddba5e19065cb0876af0b8cc0906e314dfa32cbdd14fc0468ed` (база `e50f97511d88d7ffd7a08daa3490bee7d63f0f6a` с private instrumentation). Это другой объект, чем прежняя подготовка `9bea7a6e…` и последующие изменения Main. Статическая связь build/source не квалифицирует неудавшийся runtime-замер. Пик сервиса, стоимость observer и требуемое сравнение RAM/CPU/дисковых затрат остаются **UNKNOWN**, включая пригодность для цели 512 000 000 байт.

Очистка удалила три собственных контейнера и оба собственных volume, локальные дочерние процессы собраны; свежие exact-owner запросы контейнеров/volume пусты, heavy lock отсутствовал. Host сохранил `known_service_forced_kill=true`, поэтому очистка не доказывает штатный CLI exit или успешный drain. Утилизируемый store удалён, не архивирован: его содержимое недоступно для последующего K5 readback; результата архивирования/удержания нет. Разрешение одной пары израсходовано; повтор не разрешён. Полные S6/K5, D1/512 MB, G2 и выпуск остаются открытыми. `piratecashd` не запускался и не использовался для sizing.

Локальные свидетельства: `/private/tmp/corsa-D1-K5-live-empty64-owner-package-ROOT-v1/ATTEMPT-1-ROOT-TERMINAL.json`, SHA-256 `59a379a13683b809c69a99a7fac3ad4d558df4170d73b469f86f6c4accb8ffdf`; `ATTEMPT-1-OUTPUT-MANIFEST.json` в том же пакете, SHA-256 `9bd1818ef942bac8bc5d4adf035a12d6fecb1a5793ffb1907fa0523eb1ca447f`. Manifest связывает сохранённые Host, keeper/observer RAW, пустой marker RAW, samples и точные копии входов. Это сохранённые свидетельства отказа, не resource PASS.
