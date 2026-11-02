# Pin-write: пересобранный v2.5.71 и DEV, 2 ноября 2026

**Одна разрешённая офлайн-пара завершилась `PIN_COMPONENT_PAIR_COMPLETE`.**
На каждом плече выполнены три сценария, восемь API-операций, три main-записи
и одна marker-запись через исходные callbacks. Проверены повторы без записи,
отказ новой identity при заполнении 20 000 мест и сохранность прежних pins
после reload. Current запущен после полного успеха и очистки baseline.

Это компонентная проверка pin store в Node test ELF, **не запуск standalone
corsad и не полная приёмка S6 или памяти corsad под полной нагрузкой**.

## Происхождение и условия

| Плечо | Исходный commit | SHA-256 test ELF |
|---|---|---|
| Rebuilt v2.5.71 | `f8139aa949df89e3f51f0a240eaf440b6f171255` | `88f55bc9e501c9ba7844c8161c6559b3c3849d54bfc2c87cc00bd45a9e127d70` |
| Current | `ac53899e47970217662f6e45fe199196ea184072` | `8344d23767d61dfdf9591d699f32e7eeab23f2200745f342dd18720925c4ae61` |

Одинаковые Go 1.27.3, Linux arm64/v8.0, CGO0 и tag `s6_pin_cost_resource`;
по 26 фактических BuildInfo module h1 сверены с собственным go.sum. Одинаковые
три private test overlays добавлены к каждому исходному snapshot. Baseline —
пересборка с instrumentation, не опубликованный бинарник и не baseline K11.
Main `4d9284b3a4af0e3fa06aa1672c0e9f6f175d1f56` отличается от ac53899 только
тремя документами; это unchanged-source carry, а не сборка ELF из нового SHA.

Docker 29.9.2, Linux/aarch64, kernel 7.0.14-linuxkit, cgroup v2; существующий
образ `sha256:910cfcaf707e2051587b72b0b3baaa9ca1e05d3a6289de9e8b05f9e292b5b97e`.
Test ELF, keeper и sampler вместе делят **1 CPU /512 000 000 байт /64 PID,
без swap**. Дополнительного бюджета измерителю нет, расходы не вычитаются.
Preparer отдельно: 0,25 CPU /128 MiB /32 PID, только CHOWN; normal exit0,
reap и подтверждённое удаление предшествуют рабочей группе. На каждом плече
два свежих собственных Docker volumes, inputs RO и state RW; одновременно
не более одного контейнера. Rootfs RO, сеть none, no-new-privileges; без
privileged, host mounts, скачиваний, дополнительных VM, corsad или piratecashd.

Пределы: 180 секунд работы с подготовкой +30 секунд очистки на плечо,
до 420 секунд пары; heavy-lock. Host hard caps/no-swap и жёсткой дисковой
квоты нет; hostfree не доказывает Docker-store free. Сроки программные и
не гарантируют остановку зависшего FS/Docker. Фактический запуск завершён
в пределах сроков; доступные RAW сохранены до удаления собственных ресурсов.

## Выполненная работа и размеры

Все fixtures имеют endpoints0, фиксированные identity и logical store clock,
format2 с явной unclassified metadata. Latency использует настоящий Go
monotonic clock. Seed проходит исходный constructor/audit/confirmation;
live maps, capacity и original writer не подменяются. Seed/setup и reload
вне целевых API-spans, но их работа внутри группы входит в lifetime/peak.

| Сценарий | Целевые операции | SHA-256 исходного seed | Итоговый main / marker, байт |
|---|---|---|---:|
| C0 | Новый pin 0→1, duplicate | Файл отсутствовал | 219 /45 |
| W48 | Duplicate, новый pin 48→49 | `cb7995fc793a4eb088a1c5d207c7e62fc3dbca3d8d45a675009df2b8df77a9fe` | 7 659 /46 |
| L19999 | Duplicate, новый pin →20 000, отказ следующему, duplicate прежнего | `205826e2e1292f70e713bd2d2ae2e2f2ac764308dc6679a763a919a762573d2e` | 3 100 064 /52 |

Seed W48 — 7 504 байта, L19999 — 3 099 909 байт; на обоих плечах hashes,
размеры и итоговые file-stat receipts совпали. Canonical paths одинаковы:
`/work/{cold0,warm48,nearcap19999}/store.json` и `marker.json`, depth3,
UID501:GID20, mode0600, nlink1. Все исходные pins и добавленная identity
пережили reload; отказанная identity отсутствует. Итоговый bounded state walk:
3 108 085 логических байт, 12 entries. Это не расход памяти или physical I/O.

## Результаты

| Показатель | Baseline | Current |
|---|---:|---:|
| Kernel charged group peak, байт | 106 508 288 | 102 076 416 |
| То же, десятичные MB | 106,508288 | 102,076416 |
| Inclusive group lifetime CPU, мкс | 478 872 | 412 054 |
| C0 новый pin: whole API, мс | 5,795208 | 3,019334 |
| W48 новый pin: whole API, мс | 1,279625 | 1,913000 |
| L19999 новый pin: whole API, мс | 53,618625 | 44,649208 |
| L19999 tight original main writer, мс | 3,704292 | 2,564167 |

Пик current ниже на 4 431 872 байта; lifetime CPU — на 66 818 мкс.
В W48 наблюдаемая задержка, наоборот, выше. **По одному наблюдению каждого
случая нельзя установить статистическую регрессию, её отсутствие, p95 или
постоянную стоимость одного pin.** Одинаковые store/writer bodies не исключают
различий окружения, остальных исходников, зависимостей и вариативности FS/cache.

Whole API включает locks, полный snapshot/validation/JSON, wrappers и original
callbacks. Tight span непосредственно окружает original writer после wrapper
hashing, но включает его audit/ancestor barriers/DurableWrite/cleanup; это не
время отдельного fsync. Для C0 main: 1,643167→1,853833 мс, marker:
3,980083→0,946625 мс; для W48 main: 1,183292→1,823458 мс.
При 20 000 pins whole API current занял 44,649208 мс, tight writer — 2,564167 мс.
Точную долю JSON, GC, locks и инструмента по этим данным не выделяем;
ничего не вычитаем и не называем physical-device write cost.

Series CPU: C0 2 287→2 472 мкс; W48 2 032→2 563 мкс; L19999
53 834→44 913 мкс. BEGIN после constructor/attach → sampler pre-read → ACK →
operations → END → post-read → ACK; интервалы включают работу Go/control/
observer между counter reads. Это group CPU, не изолированный writer CPU.
Go monotonic offsets не переводятся в BOOTTIME.

Lifetime начинается с keeper initial до запуска sampler/ELF/setup и
заканчивается keeper final после фактического exit0/reap ELF и sampler.
Границы длительности из read brackets: baseline 514,214917–516,187625 мс,
current 441,462251–443,246000 мс. Префикс keeper до initial и его поздний
exit-tail не входят в CPU-разность. Пик cumulative включает ранний префикс
группы до финального чтения; поздний keeper-only tail, физическая атрибуция
shared pages, isolated per-operation memory и расход preparer — UNKNOWN.
Лимиты preparer не считаются измеренным расходом.

На каждом коротком плече 10 квалифицированных snapshots: keeper initial,
sampler initial, шесть series cuts, sampler final и keeper final. **Периодических
PIN_SAMPLE нет**: серия закончилась быстро, cuts сбрасывают cadence. Максимальные
промежутки чтений — 160,395167 /133,487875 мс; это не длительная нагрузка.
Во всех snapshots memory events, включая OOM/OOM-kill/max, равны нулю,
swap0; максимум наблюдавшегося pids.current — 6, не непрерывный максимум.

## Подтверждение и границы

Owner call `call_1d3fd5f95a814184802c8b61ebad8168`, decision `84e3a5b4…`,
admission `0c6c9097…`; tool session69174, terminal exit0. Разовый допуск
израсходован, повторов/увеличения лимитов не было. Подтверждены normal0/noOOM
и удаление четырёх собственных контейнеров и четырёх volumes; heavy-lock
отсутствует. Main2278/index4a1a, owner records и все 26 frozen input descriptors
сохранены. Ни коммита, ни выпуска этим запуском не выполнено.

Локальные игнорируемые RAW: `docs/_internal/d1-artifacts/2026-10-28/`:
`s6-pin-write-component-pair-host-source-v1/`, `s6-pin-write-component-owner-package-v1/`
и два каталога плеч в `s6-pin-write-component-pair-work-v1/`. Эти файлы не
публикуются данным Markdown. Манифест — 166 физических файлов /366 072 байта
плюс pair stdout/stderr. Независимые проверки повторяли только reducers над
сохранёнными данными, не сам ресурсный запуск.

| Свидетельство | SHA-256 |
|---|---|
| `ROOT-RUNTIME-AFTER.json` | `710888bd5c13ce42c2d9816d85604a74f5fd0622c21f2e6607ec5de654d595c4` |
| `RUNTIME-RAW-MANIFEST.json` | `6614e088699668129cf0fdec16f4399680f7b23a961a275f5403041157108ac1` |
| `TESTER-ACTUAL-RUNTIME-DATA-ACK.json` | `5c4f55f94e3256b11bb951aef713564848dbe5b17629a707feb04d721672327f` |
| `TESTER-ACTUAL-RUNTIME-CLOSURE-ADDENDUM.json` | `43689acb5d9bd5e78658790c91e3766cf686c042f27019f7a01aeec6a4d7c0d7` |
| `REVIEWER-ACTUAL-DATA-ACK.json` | `325b042abff28b59d823afd1e5b63d64e3498bf6a0ca9b76be9529786249b5be` |

Результат добавляет scoped pin-write evidence к Common32. Не покрыты
заполненные endpoint tiers, классифицированные или migrated/over-capacity
stores, recovery history, полная D1-нагрузка, Desktop/SQLite/UI или стоимость
включённых S6 observers. `S6_accepted=false` сохранён; packaging admission
не переведён в DONE. До S6 остаются оценка достаточности ресурсных свидетельств,
clearnet first-contact/loopback budgets, активный startup/pin конформанс,
независимая криптоэкспертиза, утверждённый A-план наблюдения и финальные
проверки/notes/решение владельца. Windows/DACL и Tor остаются DEFERRED/UNVERIFIED.
