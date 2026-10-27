# C2a/C2b32 wire driver: воспроизведение компонента

[English](c2a-wire-driver-component.md)

Исходный recipe C2a от2026-10-25 ниже сохранён как история. Текущие два профиля и inventory45 описаны в датированном разделе в конце.

Четыре файла [c2driver](../../internal/testutil/loadstand/c2driver/main.go) сохраняют компонент фиксированного опыта C2a. Через production API identity/session-v2 он доказывает восемь TLS-каналов, обменивается `get_peers`, закрывает четыре, возвращает четыре с новыми identity и завершает каналы. Это не восемь SDK/storage-клиентов, не F1/FindNode, overload или полный тест 512 узлов.

Runtime-код byte-exact соответствует испытанному приватному driver. В `main_test.go` изменены только два stock-source SHA для проверенной K12-интеграции App/Service. Фиксированный server artifact остаётся `9ba93241a3f717fb50a6221957e4e3146f982ef5fc7b68a37f90bfe7a49e55bf`; замер нового MAIN-демона не заявляется. Испытанный native driver artifact — `63782d0722ac1cacace1b3caf97d552d07c916734d94d95bedf9f11aff4c4322`. Повторная сборка может дать другие байты.

## Проверки компонента

Запускать из корня репозитория с Go 1.27.3 и уже доступными offline dependencies. Для race нужен рабочий локальный race/CGO toolchain. Команды проверяют только пакет, не исполняют `main` опыта и не устанавливают контейнерные лимиты ресурсов.

```sh
GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off GOMAXPROCS=2 GOMEMLIMIT=768MiB GOGC=50 \
  go test -mod=readonly -count=1 -timeout=2m -p=1 -json ./internal/testutil/loadstand/c2driver
GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off GOMAXPROCS=2 GOMEMLIMIT=768MiB GOGC=50 \
  go test -mod=readonly -count=1 -timeout=2m -p=1 -race -json ./internal/testutil/loadstand/c2driver
```

Фиксированный inventory:8 родителей и 24 подтеста, 32 RUN/32 PASS на режим без FAIL/SKIP. Контроли включают настоящее production-доказательство `sessionv2.Dial/Accept`, отказ чужой identity, frame bounds, partial accounting, владение intentional close и один outstanding exchange. Это не замер памяти сервера. `TestStockSourceClosure` намеренно связывает восемь stock-файлов; дальнейшая легитимная правка требует смыслового review до обновления pins, а не удаления guard.

Необязательная native Linux/arm64 сборка компонента использует уже настроенный CGO compiler. Заменить `FRESH_PRIVATE_OUTPUT` на собственный новый каталог результата:

```sh
GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off CGO_ENABLED=1 GOMAXPROCS=1 GOMEMLIMIT=768MiB GOGC=50 \
  go build -mod=readonly -p=1 -o "FRESH_PRIVATE_OUTPUT/c2driver" ./internal/testutil/loadstand/c2driver
```

Это пример сборки компонента, не исходный pinned compiler/provenance recipe и не допуск исполнения. Принятый native recipe и его caps сохраняют отдельную область. Новый public builder target или module dependency не добавляется.

## Фиксированная runtime-область

Настоящее исполнение — только Linux. `credentials_other.go` намеренно отказывает на других системах. Linux открывает `/run/c1/auth.json` без следования links, требует стабильный regular-файл 0600 UID501/GID20/nlink1 на read-only mount и принимает не более 1024 байт. Credentials не передаются в argv или activation JSON.

Исполнение инертно без `CORSA_C2A_RUN=1`, отсутствия лишних argv и прошедшего проверку закрытого stdin activation record. Он связывает `C2A8-v1`, nonce/approval SHA, фиксированный CLI SHA, точную 40hex server identity, исходный `started_at` и residual work≤80с; cleanup ограничен 10с. Эти поля — декларации, не доказательство enforcement ресурсов или допуска. Готового approval record и runtime-команды без ресурсной обвязки здесь нет.

Endpoints остаются loopback TCP 64646/RPC 46464, сеть `gazeta-devnet`. Свежие instantaneous loopback active gauges подтверждают 8→4→8→0; cached live census — только диагностика. Bounds прежние: 64KiB на frame, 2048 application frames и 8MiB application bytes, не TLS-wire accounting. Четыре ожидания по 5с включают дополнительное время обменов; это не ровно 20с wall-clock.

Recipe публикует только Go-компонент. Для воспроизведения [ограниченного замера памяти](d1-corsad-memory-screen.ru.md) по-прежнему нужна приватная цепочка host/keeper/observer/preparation и held-cgroup/flush/cleanup. Сборка или тест пакета не воспроизводит весь замер, изоляцию ресурсов, полную 512MB-нагрузку, G2 или допуск выпуска. Доступность локальных proof за пределами исходного workspace не предполагается.


## Текущий retained компонент с двумя профилями — 2026-10-26

Reviewed extension сохраняет `C2A8-v1`/8→4→8→0/12identity и добавляет только `C2B32-v1`/32→16→32→0/48identity. Свободного N нет. Два неизменных credential-файла и qualified main/test сохраняют production API identity/session-v2 и bounded reader/write/close ownership. Точный Source2806/CLI-literal binding: `/private/tmp/corsa-D1-C2b32-wire-driver-QA-ROOT-v1/BINDING.json`, manifest `2806e1feef432c56b4db0824ea5cfe6b9e6694178c4df7d37a890b24599cd734`. Доступность локальных artifacts/proof вне workspace не предполагается.

Текущий пакет прошёл **45 RUN/45 PASS в ordinary/race** (10родителей/35подтестов), без FAIL/SKIP; scoped Darwin/Linux CGO0 lint —0issues. Сохранены32исходных события и добавлены13контролей закрытых профилей/activation/receipt: размеры фиксированных фаз и порядок ordinal после восстановления, present-empty old flag и bounded records. `TestStockSourceClosure` сохраняет8точных stock pins reviewed K9 App/Service; guard не удалять и не обновлять слепо. Эти тесты и component build не являются resource-runtime acceptance.

Legacy8 по-прежнему связывает CLI9ba93241… и `CORSA_C2A_RUN=1`. Профиль32 требует `CORSA_C2B32_RUN=1` **и полного отсутствия CORSA_C2A_RUN, не present-empty**, с отдельным activation `C2B32-v1`. Его compiled CLI — `ae61fc926e7f5c2353a21075041300fb722f2c8346389344a6e2e145931632c7`; прежние8flags/grants не включают32. Не менялись Linux-only/RO501:20/0600 credentials, loopback endpoints, точный40hex server binding,80с residual+10с cleanup,2с операции,4ожидания по5с плюс обмены,64KiB/frame и2048application frames/8MiB. Activation-декларации сами не дают разрешения; unbudgeted launch command не предлагается.

Current driver ELF `90d1c2d68524a5507aa09d4e443cb70cf3c52fb6fe936a1d9280917bad7f7aa7` (6750368байт) и CLIae61… (21627040байт) — Go1.27.3/Linuxarm64v8.0/**CGO0 static self-only**, без interpreter/needed DSO. Source CLI: базаe50f97511d88d7ffd7a08daa3490bee7d63f0f6a **плюс19 staged paths**, snapshot1fcca8012a39015fcc377472d2e56cdb82cbedef0562f07ee6acf8f6895f9f29 (2119файлов/70186204байт); driver2806 добавляет4component post и actual CLI literal (2119файлов/70197026байт). Host cross-build и последующее native Linux Docker execution — разные факты; пересборка может изменить байты. Static-вариант приведённой выше component build использует `GOOS=linux GOARCH=arm64 GOARM64=v8.0 CGO_ENABLED=0`, `-trimpath -buildvcs=false -mod=readonly -p=1 -ldflags='-s -w'`; он не исполняет artifact и не воспроизводит приватную ресурсную обвязку.

Принятый C2b32 завершил32→16→32→0/48различных identity,1344application frames/41216байт. Свежие authenticated `getNodeStatus`/`fetchRouteSummary` проверили тот же server incarnation и instantaneous atomic `active==0` **до** `zero-drained`; выход child не был zero witness. Тело финального RPC/atomic field/read timestamp не сохранено отдельным RAW; квалификация source-bound к исполненному driver. Cached live census — только диагностика. [Точные области памяти и proof](d1-corsad-memory-screen.ru.md). Opt-in K9/K10/K12 были OFF; память old9ba не является current baseline/per-peer delta. Для полного воспроизведения по-прежнему нужны private host/keeper/observer/secret/cgroup custody. Full512/F1/overload/G2/release открыты; результат не разрешает следующий запуск.
