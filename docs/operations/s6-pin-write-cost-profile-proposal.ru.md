# Pin-write cost: подготовка одной симметричной компонентной A/B-пары

Дата: 2026-11-02. Статус: **предложение подготовки, не READY и не допуск исполнения**.

Подготовительный QA завершён на обоих source: **15/15 RUN-PASS, 0 FAIL/SKIP**
на каждом. Исправлена только private fixture: format2 seed теперь содержит
обязательную явную unclassified metadata для каждой identity; отрицательный
контроль сохраняет отказ старой некорректной форме. Первичный FAIL сохранён.
Два Linux arm64/CGO0 test ELF с resource tag скомпилированы и проверены
статически: current `f130b813…`, baseline `709faed1…`, по 26 BuildInfo-зависимостей.
**ELF не исполнялись, nearcap не измерялся.** Результат QA `b1128308…`,
TESTER `198a4df5…`, REVIEWER `9491f34e…`; ordinary QA использует только seed0/3
и один некорректный seed для проверки отказа. Runtime adapter, fixed paths,
seed/file-size receipts и численные cost-результаты остаются открытыми.

Следующий измеримый пробел `load_run_on_candidate` — стоимость сохранения pin.
Common32 уже выполнил одинаковую полезную работу на rebuilt v2.5.71 и ac53899;
его group CPU даёт консервативную верхнюю границу node-charged handshake CPU
под этим churn, а память относится к corsad+keeper+sampler с выключенными
восемью новыми sources. Этот проект добавляет только компонентную оценку
pin-store. Он не требует отдельного isolated-TLS bench, DHT/full512 или collector.
Ни одно условие release-admission этим документом не получает DONE/PASS.

## Проверенные входы и отличия будущего артефакта

Baseline: `f8139aa949df89e3f51f0a240eaf440b6f171255` (rebuilt v2.5.71),
current: `ac53899e47970217662f6e45fe199196ea184072`. Один и тот же private
`_test.go`/instrumentation накладывается на оба точных snapshot; будущие Linux
arm64/CGO0 test ELF собираются одинаковым локальным Go1.27.3/flags. Это новые
компонентные test ELF, не неизменённые опубликованные бинарники и не прежние
Common32 CLI e156/3232. SHA/BuildInfo двух QA ELF уже получены выше;
финальные артефакты исполнения закрепляются после закрытия runtime-пробелов.

Основа — принятый private harness
`docs/_internal/d1-artifacts/2026-10-28/s6-pin-store-write-harness-source-v1`,
точный test SHA `dc645efd26b522b9b2645959fda1c3f74573ad9ebfff6f64f7780a26681409db`.
Функциональная применимость того же overlay к baseline подтверждена native
QA (8 top, 11 RUN/PASS, результат `59dabab9…`); это не Linux runtime/performance.
Обычный current functional QA и три RED-контроля сохраняются как отдельные
предпосылки. Будущие measurement wrappers должны пройти новое CODE/QA;
переписывать старые принятые receipts и source не требуется.

## Почему нужны три состояния, а не заполнение 20k последовательными записями

В активном `secure_session_store.go` общий admission-предел — 20 000 identity;
обычный endpoint tier — 16 384, protected tier — 32 768, protected на identity — 4.
`persist` копирует весь snapshot под `mu`, затем `writeSnapshot` валидирует и
сериализует весь JSON и выполняет original main writer. Поэтому цена одной
новой identity зависит от уже сохранённого состояния. Тайминг на маленьком
store не характеризует запись у верхнего pin-предела.

Первый профиль имеет 0 endpoints в seed и во всех целевых операциях. Он не покрывает заполненные
endpoint tiers, большие non-canonical/migrated records, audit/recovery history
или произвольный старый over-capacity store. Эти сохраняемые данные не дают
объявить 20k универсальным пределом байтов JSON/heap. K2/R1b/R2/N-in и новые
наблюдатели остаются выключенными; никаких классов fixture не выдумывает.

Три свежих собственных store на каждое плечо, без concurrency и injected failure
в cost-окнах. A — существующая identity только в тёплых seed; в C0 A новая. B/C — новые. Предлагаемый точный объём:

| Case | Начальное состояние | Последовательные целевые операции | Успешные original callbacks |
| --- | --- | --- | --- |
| C0 | Store/marker отсутствуют | inbound(A), durable duplicate(A) | main 1, marker 1; duplicate 0/0 |
| W48 | 48 pins, подтверждённый marker | duplicate(A), inbound(B) →49 pins | main 1, marker 0 |
| L19999 | 19 999 pins, подтверждённый marker | duplicate(A), inbound(B) →20 000; inbound(C) →явный full-refusal; duplicate(A) | main 1, marker 0; refusal/duplicates 0/0 |

Итого на плечо: 8 целевых operations, 3 main и 1 marker successful original
вызов; обязательны все операции и проверка итогов. Это по одному измеряемому
примеру каждого указанного случая, без статистического p95 или сравнения
«регрессия/не регрессия». Recorder limit64 сохраняется; overflow — отказ.

Seed48/19999 — воспроизводимые синтетические файлы формата, принятого обоими
snapshot, с фиксированными identity и pin timestamps; production state clock задаётся
одинаковым private fixed logical fixture clock, а latency остаётся настоящим
Go monotonic time. Это синтетическая политика fixture, не измерение trustedclock. Подготовка обязана
проверить генератор/размер/валидность и seed SHA. Seed проходит настоящий
constructor, исходный audit и его startup main/marker confirmation. Нельзя
вставлять map entries, `changes/written`, обходить capacity или заменять
original callbacks. Seed preparation и constructor — отдельная SETUP-фаза,
не samples целевых операций; 20k последовательных disk-save для seed нет.

До установки wrappers подтверждаются точные counts, marker, canonical path,
startupRefusal==nil, disk-backed callbacks; maps не меняются. Одинаковые пути
внутри контейнера (`/work/<case>/store.json`/`marker.json`), глубина каталогов и
размеры seed на обоих плечах. После каждой серии — реальный JSON/marker/reload
сверяется вне cost-окон; readback подтверждает состояние, не power-loss proof.
Setup/reload работа внутри измеряемой группы и её память не скрываются из
общего group peak; pre-group подготовка учитывается отдельно. Нет
принудительного GC, сброса page cache или заявления cold-device I/O.

## Точные окна и цена инструмента

Whole-operation span: непосредственно до вызова исходного
`noteProvenInbound` и непосредственно после его возврата.
Он включает locks, snapshot, validation, JSON и originals; main success не
заменяет успех всей main+required-marker операции. Error/refusal — отдельный
результат с ограниченным enum, не latency успешной записи.

У существующего recorder `start` берётся до mutex/append/SHA-256 входа;
этот inclusive wrapper span **не является временем original writer**.
Минимальный successor сохраняет его и добавляет tight original span: timestamp
непосредственно перед `original(raw)` после recorder unlock и сразу после
возврата. Raw и original передаются ровно один раз, error сохраняется. Два
временных поля, исходные callback counts, bytes и success проверяются QA на
обоих snapshot. Оригинальный POSIX writer включает Open, SyncAncestors,
DurableWrite, post-SyncAncestors и Close: это не latency отдельного fsync.

Recorder фиксированно ограничен; его mutex/record allocation, timestamps и
SHA-256 проходят внутри whole-operation, а хеширование всего JSON также может
менять cache context до tight original span. Это реальная инструментированная
нагрузка. Оба плеча имеют одинаковые wrappers; время/CPU/память recorder,
keeper/sampler не вычитаются и не именуются нулевыми. Отдельные duplicate/no-op
операции дают диагностику, но не универсальную поправку на observer overhead.

Время operations/callbacks — monotonic Go clock одного процесса. Cgroup
cpu.stat до/после фиксированных серий даёт inclusive charged CPU группы с
погрешностью границ/дискретностью счётчика; не CPU отдельного fsync или чистого
store. В отчёте — сырые counters и brackets, без вычитания setup/control CPU.
Для будущего runtime group CPU имеет два отдельных интервала: lifetime от
initial snapshot до ELF/setup до final read после ELF+sampler reap, и
series-only counter brackets вокруг целевых operations. В первый входят
setup внутри этой группы и instrumented work; генерация seed вне группы или
preparer до initial в него не включаются. Их CPU/память сообщаются отдельно.
Ни одно окно не расширяется задним числом по метке READY.
Main/marker bytes — длина входа original, не physical I/O, число fsync или
записанных страниц. Все raw store contents/identity/секреты не публикуются.

## Предлагаемые ресурсы и граница следующего допуска

Текущий timing-only private fixture использует `t.TempDir()` и пока публикует
callback input bytes, не отдельные seed SHA/final file.Stat lengths. Это допустимо
для ordinary small QA, но не реализует обещание одинаковых `/work/<case>`
путей actual пары. Fixed-path adapter, точные seed/hash/file-size receipts и
корректные CPU/peak endpoints — обязательные будущие проверяемые пробелы
перед runtime READY, а не уже полученные свидетельства.

После CODE/QA предполагается reuse минимальной связки test ELF + keeper +
cgroup sampler в **одном общем бюджете 1 CPU /512 000 000 байт /64 PID, no-swap**.
Это компонентная группа, не standalone corsad. Дополнительного observer-бюджета
нет. Предварительный срок — 180 с с подготовкой +30 с cleanup на плечо, максимум
420 с для baseline→current; current запрещён после неполного baseline.

Реализация keeper/sampler branch ещё отсутствует; до проверки timing-only
Go-fixture она не создаётся. Ordinary QA исполняет только small-cardinality
контроли seed0/3. Nearcap entry находится под отдельным tag
`s6_pin_cost_resource`: его можно compile-only проверить, но native QA его
не запускает и не подменяет 20k меньшей capacity. Если корректный reuse memory oracle не получится
сохранить узким, первый результат содержит timings/serialized bytes, а память
явно остаётся UNKNOWN; RSS/JSON length не заменяют cgroup peak. Она должна использовать
существующие Cgroup/child-reap/SHA/PID-birth/clock/STOP/HOLD механизмы, без RPC,
TCP/TLS, driver или generic collector. Проверить, что конечный kernel charged
memory.peak прочитан после test ELF и sampler reap, с тем же неизвестным поздним
keeper-only tail/shared-page attribution. Пик включает seed/setup/serialization, которые действительно выполняются в
этой группе, все три store и проверку результатов; preparer до создания
группы в него не входит. Per-operation isolated memory неизвестна.

При необходимости только короткий собственный preparer: .25 CPU/128 MiB/32 PID,
CapDropALL+CHOWN для свежих volumes, удалён до рабочих ролей. Не более двух
контейнеров одновременно при подготовке; preparer завершается и удаляется до рабочей группы.
Секретный handoff из SG32 здесь не нужен. Нет секрета,
RPC auth tmpfs, внешнего driver или запуска corsad/piratecashd. Offline, без
pull/скачиваний/privileged/host mounts, под heavy-lock; ≥10 GiB hostfree плюс
подготовка. Hostfree не доказывает Docker-store free. До предложения запуска
нужны точные volume/image/ELF/происхождение и checks свободного места.

Предлагаемые фазовые пределы: собственный state≤64 MiB, RAW≤4 MiB на плечо;
размеры seed и worst transient temp+main+marker до запуска проверяются против
этой границы. Это фазовые проверки, не hard disk quota. Software deadlines не
гарантируют остановку зависшего FS/syscall/backend. Ошибка/OOM/timeout/неполная
работа/неполные обязательные измерения — STOP, без автоматического повтора,
роста лимитов или fallback на memory-only. UNKNOWN удерживает lock до recovery.
Только собственные ресурсы, сохранение доступных журналов перед cleanup.

**Следующее разрешённое действие по этому проекту — подготовка и CODE-review
узкого measurement-source, затем ordinary функциональные QA.** Компиляция двух новых Linux test ELF только для статической QA допустима в
согласованной ordinary test-области после review. Их ресурсное исполнение —
отдельный будущий шаг с конкретным проверенным пакетом и новым допуском. Здесь
исполнения нет. После чисел получим
pin-write cost evidence именно для трёх состояний/двух реализаций на данном FS;
этого недостаточно для всего `load_run_on_candidate`, полного S6, 512MB/D1, G2,
активации или выпуска без оставшихся условий и решения владельца.
