# C2a wire driver: воспроизведение компонента

[English](c2a-wire-driver-component.md)

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
