# Full512: предлагаемый диагностический профиль NativeVZ

Статус: **разрешённая первая попытка израсходована: настоящий Go/fixture FAIL**. Прямой ответ `6115ae3f21964964b596c13c5a875c54` разрешил один запуск точного профиля и условный ACK. Попытка выполнена 2026-10-26 и завершилась FAIL до набора 512 участников; одинаковый повтор этим не разрешён. Прежний Member4 grant остаётся отдельным.

## Цель и область

Один офлайн-запуск `TestOwnedP0FullPopulationAndRegistryOverflow` из Linux arm64/CGO0 test ELF: полезная пересылка при 512 F1 и 512 legacy участниках → отказ при 1025 посещениях registry → удаление лишнего legacy, новое полное покрытие 1024 и полезная пересылка. Проверяются сохранение принятых обязательств и отсутствие возвращения отклонённого запроса после recovery.

Это функциональная fixture с Service и 1025 клиентами внутри одного гостя. Бюджет всего гостя **не является расходом standalone corsad** и не подтверждает пригодность D1 в 512 000 000 байт. Физические временные границы, G2, активация и выпуск остаются отдельными.

## Предлагаемые пределы

| Параметр | Значение и область |
| --- | --- |
| Гость | 2 vCPU, 1 610 612 736 байт RAM (1536 MiB) |
| Изоляция | Нет NIC, дисков, host sharing, virtio sockets и balloon; внешняя сеть отсутствует |
| Guest swap | Только пустая таблица `/proc/swaps`; backing disks и swapon не используются |
| Guest PID | Жёсткий предел не обеспечен; UID 0 не даёт считать RLIMIT_NPROC таким пределом |
| Go | GOMAXPROCS=1, test timeout 180 с |
| FD | soft=hard=4096; 2050 endpoint FD + 2046 планового остатка, не измеренная достаточность |
| Guest supervisor | Свой monotonic origin; программные deadline: work300 / KILL310 / reap-refusal-UNKNOWN320 |
| Host worker | Общий origin при входе launch.py; программные work360 / cleanup end380 |
| Outer | Программный fence: KILL398 / reap wait400 от того же origin |
| Application quota | 131 072 frames, 134 217 728 payload bytes; это не TLS wire traffic |
| RAW | Console 4 MiB; native events 16 KiB с received/retained/EOF/hash; stderr prefix 4096 B; outer combined prefixes 65 536 B |
| Диск | Перед каждой попыткой минимум 10 GiB плюс подготовка и фактически выделенные входные копии |

**Суммарные host RAM/CPU/PID hard caps и гарантированный запрет host swap не обеспечены.** Гостевые vCPU/RAM их не заменяют. Плановое время и outer fence не гарантируют физическую остановку VM/XPC или освобождение всех ресурсов после аварии. Плановые timer actions также зависят от scheduling/syscall completion; просроченное завершение не даёт PASS. При неопределённости может понадобиться ручное восстановление.

Отдельные BusyBox setup-timeout 2 с удалены из-за daemonizing watchdog. Установка адресов теперь ограничена общим guest supervisor; прежний отдельный setup2s не обещается. Внутренние Go-ограничения сохранены. При нехватке бюджета результат — отказ/таймаут; автоматического увеличения лимитов нет. Сам `piratecashd` не запускается.

## Зафиксированные артефакты

| Артефакт | SHA-256 |
| --- | --- |
| node.test, 65 193 127 B | `3196e734087c80ee0701f190f573fa6ea7657b1e3276386bb92cb4e83ab2a48b` |
| Kernel Image, 36 306 944 B | `256d5fff4bb892edcb9218c1e6353e388bc3646e65f76c2bc9d958b635d1b00e` |
| initrd, 100 940 160 B | `2f44233a93bccb85bec0f89ee36bf6845213b203f2f4189e3c576ec9b597f293` |
| Подписанный launcher, 220 720 B | `796596c2cb841a0108821f17bd5c271de410ffba7d5b92d98313af81014bf983` |

Go Source domain: `827a3f550d9b5209f6ce2228dcb57fead80abe4e7fc9f5064260c266646e3ad8`. В initrd сохранены исторические вспомогательные артефакты/source domain `1f3a92ee…`; они не выдаются за единую актуальную сборку всего приложения. initrd: 2896 записей, точные 3 замены + 5 добавлений, остальные 2888 записей побайтно сохранены.

Launcher: ad-hoc identifier `org.corsa.d1.p0-full512.native-vz`, CDHash `b9224c7850c6ebb6ecb6e29206da236e5ccb35c6`, ровно `com.apple.security.virtualization=true`. До первой попытки эффективность entitlement и пригодность новой конфигурации VZ не были доказаны; наблюдавшиеся запуск и нормальная остановка ниже относятся только к этой попытке. Длина файла не означает расход RAM.

Подготовка принята тестером и ревьювером: static C compile, восемь fake-syscall controls, семь Python controls oracle/RAW, шесть групп архиватора, отдельный retained scan, полный readback нового initrd и шесть compile/sign/static команд. Это разные области; они не заменяют full512 runtime PASS.

## Остановка, lock и повтор

Ошибка, OOM, таймаут или недостоверные данные останавливают текущую попытку; результаты сохраняются. Canonical heavy-lock сохраняется **при любом исходе**. Никакого автоматического ACK/удаления lock нет. Владелец условно разрешил ACK только после независимой сверки ROOT + TESTER + REVIEWER по одному из двух оснований:

1. Подтверждённые stopped callback/terminal state, native normal exit0/reaped и завершение owned процессов.
2. Узкий доказанный отказ до создания VM на input/unsupported/validation: полный RAW, created=false во всём потоке, start не запрошен, native exit2/reaped и завершение owned процессов.

Silence, один exit2, ошибка wrapper context или неполный RAW не заменяют эти свидетельства. Удаляется только точный собственный неизменившийся lock. ACK не доказывает физическое освобождение всех XPC-ресурсов; при неизвестном состоянии lock остаётся до operator recovery.

Предлагаемые повторы — только после локализованного и независимо проверенного исправления обвязки, в том же профиле и после разрешённого ACK. Настоящий Go/fixture FAIL не разрешает одинаковый повтор, смену backend/oracle или увеличение лимитов. Preflight может отказать в будущей отдельно допущенной попытке; остаётся открытым отдельный Member4 guard3. Новая диагностика сохраняет первую отвергнутую строку маршрута, сохраняя predicate.

## Фактическая первая попытка: 2026-10-26

Один RUN, один FAIL, ноль PASS: `find_node_p0_full_coverage_linux_test.go:390`, «genuine original F1/auth/publication binding absent». Helper объединяет `valid`, `proven` и `caps == 1`; RAW не показывает, какие отдельные conjuncts отказали. Первый клиент прошёл v2 establishment и ping/pong. Маркеров POSITIVE, OVERFLOW и RECOVERY нет: набор 512, полезная пересылка, отказ при 1025 и recovery не доказаны.

Запущен frozen Source domain `827a3f55…`, а не текущий MAIN. Host elapsed около 4,47 с; console RAW — 28 961 B, SHA-256 `03bbb991092bf6c5e14849ce200a8317417af5466b85e54c4c2f4d4accc82553`. Native event RAW полный: 342 B, EOF, без truncation, с точным semantic replay; наблюдались нормальная остановка гостя и native exit0. Outer exit2, console oracle отказал. Остановка/exit0 не превращают Go FAIL в PASS и не доказывают физическое освобождение XPC. Попытка не принимает критерии 512 МБ/RAM, G2, T5, активации или выпуска.

Read-only source finding, независимая проверка ожидается: fixture требует typed NetCore F1 capability count1, тогда как test-only flag добавляет F1 в wire advertisement, а обычный typed capability intersection его исключает. Это кандидат на расхождение из исходников; отдельных значений `valid/proven/caps` в RAW нет. Исправление, обход assertion, изменение oracle или активация defaults этим не подразумеваются.

Статус ACK: **выполнен для точного собственного lock** после независимой сверки ROOT + TESTER + REVIEWER и нового P0-specific script review. ROOT read-only check `a6127e` и ACK `3da7c9` завершились exit0; перед удалением повторно проверены normal VM stopped/native0, завершение owned PID/PGID и неизменность inode/двух own files. Удалены только эти два файла и их собственный lock-каталог; canonical lock отсутствует. SHA-256 квитанции ACK: `756258c33322a67cbd79be69b8f6d58b6e083ee98fb31ce7e349131eb81a78ce`. Physical XPC release остаётся UNKNOWN; это не blanket cleanup. Настоящий fixture FAIL сохранён, одинаковый повтор не разрешён.

## Подготовленное исправление capability; Native-результат прежний

Source finding выше после этого прошёл независимый CODE review. Подготовленное исправление переносит существующий private F1 selection в одну typed local capability function: её используют wire rendering, исходная inbound negotiation и оба outbound metadata caller. Wire rendering теперь только проецирует этот typed set, без отдельного добавления F1. Обычные defaults остаются off; исходные intersections, proof/auth requirements и настоящий assertion fixture сохранены. Каждый вызов по-прежнему пересчитывает datagram capabilities по существующим live-plane правилам: общая функция не является frozen handshake snapshot или атомарным capability cut разных моментов.

Новая native-регрессия из четырёх случаев проверяет реальные capability/metadata functions и NetCore mirrors: default-off, selected peer-present, peer-absent и duplicates. Проверяется, что metadata не создаёт auth и не открывает исходный capability gate. Область — только metadata, без TLS/publication или full512 proof.

Guard correction отдельно учитывает восемь пропущенных исторических pin updates для K10 observation hooks, затем два новых capability-caller body delta. Сохранены все 27 Registry keys, publication checks и 17 исходных negative children (16 table cases плюс OTHERFILE). Неизменённый Registry baseline один раз запущен и отказал на устаревших pins; результат сохранён. Принятие Source/guard CODE не заменяет corrected functional QA.

**Фактический focused QA на Darwin/arm64:** ordinary59 и race59 — всего118 RUN/PASS (Node58+driver1 в каждом режиме), без FAIL/SKIP, race report и stderr; все шесть команд завершились normal0/reaped. Scoped Node/driver lint macOS и Linux arm64 при выключенном CGO — zero issues. Private2142 source, MAIN2141, Git stage entries и original held telemetry FD сохранены; использованы прежние собственные lint caches. Исходный Registry baseline1FAIL сохранён отдельно и не включён в118PASS. Эти проверки не создавали corrected full512 ELF и не выполняли второй Native run; Native PASS не заявляется. Первый настоящий FAIL и exact ACK выше сохранены. Standalone512МБ/RAM, G2, T5, активация и выпуск не приняты; этот QA не разрешает Native retry.

## Подготовка исправленных артефактов; второго Native-результата нет

После переноса capability correction MAIN2142 дополнен двумя private fixture files: всего 2144 файла, Source domain `cb0851c9d3bb493d8996dc0f30dc0e52537edb8f4cef466ffabd0c13bd73d67f`. Настоящий predicate fixture сохранён; сообщение об отказе теперь показывает захваченные скаляры `valid/proven/caps`. Source controls: 21 ordinary + 21 race, всего 42 PASS. Linux arm64/CGO0 compile создал node.test, 66 009 232 B, SHA-256 `a3a284686d2b03da79a43f59195d9c39d782cc5b0e4c7f644700747b3f0e4d23`; ELF не выполнялся.

Семь архивных проверок прошли: шесть controls и retained read-only scan. Исправленный initrd: 101 266 775 B, SHA-256 `c56a3edbea7f1a96bddeabd6d690a5cfcad00220bf887a175003fe8ad6f62137`; полный readback проверил 2896 записей и побайтное сохранение 2888 неизменённых записей. Независимый archive DATA принят (reviewer `5052cfb6…`, tester `6b0e0dcf…`). Исторические auxiliary artifacts остаются смешанными с актуальным Node source. Native Source review принял узкую замену input/path/hash/domain literals с сохранением operational bodies, профиля, lifecycle и oracle; прежние controls остаются историческими свидетельствами.

Шесть compile/sign/static команд завершились normal0/reaped за 11,224 с; source/tool/selected-SDK/MAIN custody сохранён. Compiler stdout и stderr пусты; compiler warnings не зарегистрированы. Новый ad-hoc launcher: 220 720 B, SHA-256 `171dddf835d88122154b938b14d38e6bcc5470b689878e0069d50044e98d2137`, CDHash `96f31da31b9bf6b3246117ce0ef2cbe1a0d494d8`; identifier `org.corsa.d1.p0-full512.native-vz`, ровно `com.apple.security.virtualization=true`. Независимый build DATA принят (reviewer `3f77fd33…`, tester `39f6c044…`). Это квалификация только static artifact. Native/API/VM не выполнялись; effective entitlement и VZ support остаются UNKNOWN. Новый runtime grant, standalone 512 МБ/RAM, G2 или принятие выпуска не доказаны. Первый FAIL, ACK и таблица артефактов выше сохранены.

## Фактическая исправленная попытка: controlled stop, неполный Go verdict

Предыдущий раздел подготовки описывает состояние до этой новой попытки.

Новый прямой grant `14eefab2…` разрешил ровно одну отдельную исправленную попытку. ROOT `af97e9` / session 13451 → `2acc72` CLOSED2: 2026-10-26, около 5,79 с. Результат — **STOP-NOT-PASS**, generic `console-error`. Сохранено 4 194 251 B console при прежнем cap 4 MiB (SHA-256 `5467f7b7fa9ef301a87f7068c319bc765dfd099e79636d67c7fb479279755286`). Точные отвергнутый block и пойманное exception не записаны; overflow следующего блока — source-consistent inference, а не сохранённое свидетельство exception. Есть один Go RUN, нет полного PASS/FAIL verdict, POSITIVE/OVERFLOW/RECOVERY и финальных child/parent witnesses. ConnID и число progress logs не подтверждают retained session population.

Native RAW 342 B полный: EOF/replay, ready → start_ok → stop_complete с final stopped=true; записаны native exit0 и input recheck. Это controlled host stop: guest_normal_stop=false, без успешного завершения гостя. Фактический DATA принят тестером `4a17425d…` и ревьювером `b60baf28…`. Physical XPC release остаётся UNKNOWN. Успех full512, standalone 512 МБ/RAM, G2, T5, активация или выпуск не приняты.

Точная correction record `f46ddac4…` связывает четыре primary capability files (capabilities, datagram integration, peer sessions и Service) с patch SHA-256 `0fb30dbcb9f4e108a43eb30b31d8c658f16d346778690236fb09efb3f07f3c89`; reviewed 18-Go scope patch — `aaf09376360b9868d2dc00bc0d5aa6b96cf0403865c33a92669173481053b526`. Попытка привязана к подготовленным выше cb085/a3a Node, c56a initrd и 171ddd signed launcher; не заявляется, что capability correction — единственное различие whole bundles. Профиль, quota и oracle сохранены. Единственная попытка израсходована; дальнейшие повторы требуют отдельного решения. Первый настоящий FAIL и его ACK сохранены.

Exact outcome-specific ACK выполнен после независимых eligibility и Source CODE acceptance, затем свежих live checks: ROOT `--check` 81f9e2 и `--ack` 092403 оба CLOSED0. Удалены только два recorded own files и их точный lock-каталог; отсутствие canonical lock зафиксировано при завершении. SHA-256 ACK-квитанции: `007908c8c427a00655db8dcaa770f45250a67a09923f43a06ccec888c0e72ef4`; независимый readback принят (тестер `102da4a1…`, ревьювер `a2ddd70c…`). Квитанция сохраняет SOURCE_BOUND_CONTROLLED_KNOWN_STOP / STOP_NOT_PASS, physical XPC release UNKNOWN. ACK не разрешает повтор и не доказывает успех fixture или blanket resource cleanup.

## Подготовка quiet fixture; нового Native-запуска нет

Private full fixture теперь ограничивает настоящий zerolog global severity уровнем Warn после точного opt-in и до construction; сохранённый level восстанавливается после более позднего owned cleanup по LIFO. Writers, Warn/Error events, Go/phase witnesses, профиль, quota, timers и oracle сохранены. Это test-harness изменение; обычные daemon logging defaults и admission не меняются.

Private source из 2147 файлов имеет domain `99f9f1f0ebc928cd5ce433084abd338dfc498d5702b4a4651c789ba23ab739bf`. Все 26 ordinary +26 race events прошли: 52 новых portable/source-control RUN/PASS, включая пять focused logging events на режим вместе с прежними 21. Linux arm64/CGO0 compile создал node.test, 66 015 964 B, SHA-256 `bc077f581c4e56f8345d1d82054337f5c3bf9c01d0e450e8126f923990993dba`. ELF/full fixture не выполнялись. Нового исполнения исторических 29 или Native Python7 controls этим не заявлено.

Quiet initrd: 101 276 791 B, SHA-256 `2e5801794457ed9e6e050b79afb50858ac8c40c35ab5f1a330bb704973541b50`. Полный gzip/newc readback принял 2896 записей: прежние три замены, пять добавлений и 2888 неизменённых raw members, с сохранением kernel/generic C helpers и алгоритма архива (тестер `511017e4…`, ревьювер `89ef2c6d…`). Первый readback с FileNotFound при записи final receipt сохранён. ROOT исправил только output-path metadata binding и повторил неизменённый reader на неизменённом архиве; исходный attempt не переименован в PASS. Auxiliary artifacts остаются историческими/смешанными.

ROOT static build `5cf9a4/session97910 ->39c02e CLOSED0` завершён за 11,937647 с: все шесть команд вернули normal0 и reaped; compiler stdout/stderr пусты, custody сохранена. Новый launcher: 220 720 B, SHA-256 `5e0508c4445b412740b1f2c37c9ac0d8bbac75264d8443f015da8ca36cc7344d`, ad-hoc signature, CDHash `5fba5f7dccb72fe74ee7e5262c07640b35d742ec` и единственный static virtualization entitlement. SHA-256 ROOT result: `26bffe3f60668b2060bff3ad9c96a252cb0fce0a030bb877f90998affc44e465`. Независимые actual static-build DATA приняты (тестер `93344451…`, ревьювер `e27852c4…`); qualification охватывает только эти шесть static phases. Launcher не выполнялся; effective entitlement и host VZ support остаются UNKNOWN. Нового Native/API/VM исполнения и owner grant нет; прежний 14ee grant израсходован. Успех full512, standalone 512 МБ/RAM, G2, активация и выпуск не приняты. Прежние FAIL, STOP, точные ACK и таблица их артефактов сохранены.

[English](d1-full512-native-profile-proposal.md)
