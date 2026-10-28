# Диагностика clock-domain D1

[English version](d1-clock-domain-diagnostic.md)

Статус на2026-10-28: одна исправленная Docker-проба полностью собрала матрицу и завершилась0. Привязка keeper/observer к процессам совпала, но CURRENT time namespace identity различаются — shared-time guard отказывает. Это не resourcePASS и не доказательство старой причины64c0. Оба актора завершились0, ограниченная очистка подтверждена. Новый запуск не разрешён.

## Цель и доступность свидетельств

Инструментированная OFF-попытка `64c0…` от 2026-10-27 отказала с `time-clock-reader-domain`; ON был пропущен. [Исторический отчёт о памяти](d1-corsad-memory-screen.ru.md) сохраняет исход и прежние точные хеши. К 2026-10-28 исходные private-пути `/private/tmp/corsa-D1-*` с Source, runtime-пакетом, ELF и RAW стали недоступны. Исчезновение установлено; его причина этой работой не определена.

Ограниченное статическое восстановление из этой D1-беседы сохранило полный исходный фрагмент `CurrentBootContext`, его сравнение `fstat(fd) → [st_dev, st_ino]` и относящиеся к нему авторские edit-строки. Ни один из шести полных runtime-файлов не восстановлен до заявленного финального SHA; ELF `64c0` также не восстановлен. Исторический отчёт и восстановленные выдержки сохраняют разные свидетельства. Они не заменяют потерянный полный Source-пакет или runtime RAW. Историческая первопричина остаётся **UNKNOWN**.

Новая диагностика отвечает на узкий вопрос: какие отношения процессов/proc и time namespace действительно наблюдают два выбранных Docker-актора? Это новый объект с новыми Source и будущими artifact bindings. Он не является точным воспроизведением прежней попытки или доказанным исправлением её причины.

## Два ограниченных актора

Предлагаемые места Source в репозитории — `internal/testutil/loadstand/clockprobe/probe.py` и `host.py`. Импорт не запускает работу. Host остаётся неактивным без точной будущей записи решения владельца и проверенных Source/wrapper inputs.

Keeper — PID 1 своего контейнера. Он записывает собственные self/thread-self/numeric-PID свидетельства, удерживает именованные namespace FD через интервал и сам снимает CLOCK_BOOTTIME до и после observer. Observer разделяет PID/network namespace keeper и записывает три собственных reader-view, а также свой view keeper PID 1. Observer-view PID 1 не заменяет чтение часов самим keeper. Демон, Go ELF, peer-нагрузка, Capture, credentials, preparer, volume, host mount, опубликованные порты, `setns`, `unshare` и перевод offsets не используются. `piratecashd` вне области диагностики.

Ограниченные факты каждого актора включают process birth/status/NSpid, диагностические namespace readlink, device/inode дескрипторов, proc-mount metadata и raw offsets. Удерживаемые и вновь открытые CURRENT/FOR_CHILDREN FD перепроверяются вокруг чтений часов/offsets. Байты offsets читаются повторно и сравниваются для проверки стабильности. Offsets сохраняют метку **FOR_CHILDREN**, а CLOCK_BOOTTIME актора относится к его **CURRENT** context. Текст readlink не заменяет идентичность FD. Условия «offsets должны быть нулевыми» или выведенного преобразования часов нет.

Матрица отдельно сохраняет raw-сравнение numeric-reader и сравнение настоящих actor-current. Для вывода о совместимости также нужны согласованность self/thread/numeric view, keeper before/after identity и observer target-PID-1 binding по process/birth/namespace. Стабильное несовпадение может дать полную диагностическую матрицу с ложными `supported_source_guard` и `shared_time_domain`. Обе стороны сохраняются. Пропуски/изменения фактов, некорректные значения и неподдерживаемые чтения остаются incomplete/UNKNOWN; bool не принимается за числовую identity. Полная diagnostic capture не является PASS прежнего runtime guard, доказательством общей clock-domain при недостающих предпосылках или resource PASS.

## Предлагаемый профиль — нужно новое решение владельца

| Пункт | Предлагаемая граница |
|---|---|
| Акторы | Два собственных Linux arm64 контейнера; keeper network none, observer разделяет его PID/network namespace |
| Каждый контейнер | 0,25 CPU; 128 MiB (134217728 байт) RAM без swap; 32 PID; UID/GID 501:20 |
| Изоляция | Read-only root; все capabilities сняты; no-new-privileges; без mounts и volumes |
| Образ | Точный `sha256:910cfcaf707e2051587b72b0b3baaa9ca1e05d3a6289de9e8b05f9e292b5b97e`; `--pull=never`; свежие проверки platform и пустого image `Config.Volumes` |
| Передача Source | Ограниченный SHA-bound Python `-c` argv; без shell, нового image или preparer |
| Docker endpoint | Фиксированный локальный `unix:///Users/hades/.docker/run/docker.sock`; без remote API и автоматического запуска Docker/VM |
| Обычная работа / очистка | 30 секунд с подготовкой, затем 30 секунд очистки; программные границы |
| RAW | До 64 KiB на роль и до 256 KiB суммарно для захватываемых CLI-потоков |
| Метаданные / весь вывод | Две квитанции до 256 KiB каждая, ограниченные claim и console; весь логический runtime-вывод до 1 MiB |
| Условия хоста | Исходный canonical heavy-run wrapper/lock; минимум 10 GiB свободного места плюс подготовка; общих host CPU/RAM/PID hard caps и гарантированного host no-swap нет |

Host проверяет локальную доступность, точные inputs, образ/platform, отсутствие неявных volumes, место и владение lock до попытки создать однократный claim владельца или запустить акторов. Чистый отказ до старта не создаёт акторов и не расходует claim. Неопределённый исход claim/create трактуется консервативно. Автоматические повторы и повышение лимитов не предлагаются. Read-only preflight 2026-10-28 обнаружил недоступный Docker socket; это результат доступности окружения, не вывод о поддержке time namespace ядром Linux.

## Завершение и неопределённость

Расписание 30+30 секунд не гарантирует завершения зависших filesystem/Docker syscalls или физической жизни процессов. CLI-дети наследуют исходную heavy process group. Очистка ограничена точными собственными ID и проверенными nonce/name/role labels; затем нужны свежие own-resource queries и сбор CLI-процессов. Подтверждённое принудительное удаление может закрыть ресурсную область, сохраняя incomplete диагностику; оно не доказывает штатный actor exit, drain или полезные наблюдения.

При UNKNOWN физического завершения Host должен оставаться живым в настоящей heavy process group, удерживая существующий wrapper/lock до operator recovery. Текстовый marker или возврат ошибки недостаточен. Такой idle HOLD может продолжаться дольше 60 секунд. Автоматического ACK нет. Для recovery нужны независимо подтверждённое физическое завершение и явное решение оператора/Root. Внезапное принудительное завершение контроллера или host crash находятся вне гарантии живого контроллера; прежние правила исходного wrapper продолжают действовать.

RAW и метаданные ограничены отдельно. Метаданные provisional до соединения с реальным outer terminal, проверенным закрытием вывода и свежим custody/closure readback. Потеря финальных данных, ошибка вывода или неполная очистка не становятся успешным свидетельством. Точные Source, admission, nonce и actor results должен связать последующий проверенный пакет; одни labels не доказывают общие часы, историческую первопричину или готовность выпуска.

## Проверка и хранение

Независимый source/adapter CODE принят (`35ed915e`, test-only supplement `7808743f`). Первый pure-прогон завершился CLOSED2: восемь методов прошли, два упёрлись в неканонический временный путь fixture на macOS; его FAIL и RAW сохранены. Исправлена только fixture, добавлен actual отрицательный контроль symlink-пути; Host guard и оба implementation-файла не менялись. Свежий полный повтор (`49927c`, result `181566d1…`) прошёл все 10 unittest-методов и 33 subTest без failures/errors/skips, с наблюдённым outer exit0. Сохранены тела и записанные метаданные Source/Main, а также Git stage entries. Alarm30 относится к тестовой области; служебные Git metadata reads идут отдельно. Для subjects/tests были закрыты реальные process/network/fork/exec пути; использовались управляемые proc/clock/Docker-границы и собственные обычные временные файлы/FD. Это не проверка настоящих nsfs, Linux time namespaces, Docker, cgroups, приложений или расхода ресурсов. На том подготовительном этапе Docker/kernel диагностика ещё не запускалась; последующая допущенная неполная попытка записана ниже.

Принятый Source должен получить tracked-место в репозитории. Новые private manifests, recovery data и bounded RAW размещаются в `docs/_internal/d1-artifacts/`, а не только в `/private/tmp`. Эта папка остаётся локальной и ignored; такой порядок не гарантирует сохранность при потере диска или наличие свидетельств на другом хосте. Восстановленные исторические фрагменты класса/callers — инертный исходный материал, не исполняемые инструкции и не новое разрешение. При этой подготовке кеши или прежние свидетельства не удалялись.


Команда pure-тестов: `python3 -I -S -B internal/testutil/loadstand/clockprobe/test_clockprobe.py`. Управляемые subprocess-границы тестов не запускают Docker. Выбранные implementation-хеши: Probe `c58fee4558ba4f4e5952be72ad581d1085382453ab9a914bb7fc5a931295f3f6`, Host `c7b023ba102f1b41ba479244caf1cc747cd4ac21ec4e3ec7bda853507eec7231`; тестовый файл `901c0bd9d5fa9fb4950e1bd4ed8c78245f4a427b0f4010cbf29530455d35e9a2`. Локальные QA-материалы: `docs/_internal/d1-artifacts/2026-10-28/clock-probe-thread-offset-qa/`; их наличие на других хостах не предполагается.


## Фактическая Docker-проба — 2026-10-28

Ответ владельца «докер запустил можешь проверять» дан после описания точного профиля одной пробы; Root явно принял его как допуск этой одной попытки без изменения ресурсов. Docker29.8.0 сообщает kernel7.0.12-linuxkit; точный образ — Linuxarm64. Read-only preflight выявил отсутствие ключа `.Config.Volumes`, из-за чего прежний шаблон отказал. В Host исправлен только литерал на `{{json (index .Config "Volumes")}}`; реальный тот же образ затем вернул `Volumes:null`. Строгие image/isolation/resource guards сохранены. Независимый CODE949b06b7 и свежий pure7fd163 подтвердили те же10методов/33subTest без реальных subject subprocess/network. Это повтор существующего набора, не20уникальных методов. Actual использовал private tested Host676a, а staged Main во время пробы сохранял Hostce8d.

Единственная допущенная попытка a9c3a3 завершилась outerexit2/**INCOMPLETE**: keeper получил FileNotFoundError/ENOENT на `/proc/thread-self/timens_offsets`; Host вернул `keeper-capture-incomplete`, observer не создавался. Начальные self/thread/numeric FD identity были одинаковыми `[5,4026532960]`, self FOR_CHILDREN offsets — нулевыми. Полный BOOTTIME bracket и keeper-after/observer matrix отсутствуют. Эти части не доказывают общие часы, отсутствие поддержки time namespaces, совместимость старого collector или историческую причину64c0. Требуется конкретная правка proc-пути, без ослабления guard или перевода часов.

Создан только собственный keeper с заявленными бюджетами и изоляцией. Проверенный Host закрыл/reap свой CLI-путь, подтвердил владельца и принудительно удалил контейнер; все11 сохранённых Docker-команд дали0. Свежие запросы своего label и точного ID611a78 дали пустой ответ/0, wrapper завершился, lock отсутствует. KNOWN closure ограничен этой own-container/API/CLI моделью: это не normalkeeperexit/drain и не доказательство физического освобождения всех backend allocations. Observer/corsad/piratecashd не запускались. Логический runtime-вывод22 241байт. ROOT-ACTUAL.json SHA256 `1fe033b3f48e5d30785a98e39caeba1dfb1d2f1765c416286bfd4d10580770db` связывает локальный RAW; TESTER38a27b05/REVIEWERfe85da3f приняли неполный результат и указанную границу очистки. Независимое чтение файлов не выдаётся за личное наблюдение ими tool terminal Root.

Свидетельства находятся в `docs/_internal/d1-artifacts/2026-10-28/clock-probe-runtime-docker29-v2/`, RAW — в `runs/clockprobe-4f50de4c25fe4264a8574deb35694c58/`. Отказ и исходные свидетельства сохранены. Claim одной попытки израсходован; повторов не было и они не разрешены. Исправленный runtime-прогон требует нового решения после приёмки source/tests/package. Общий clock-domain и прежняя первопричина остаются UNKNOWN; память всего демона, полные K5/S6, G2, активация и выпуск не подтверждены.


## Исправленный reader потока — pure-подготовка

Неподходящий task-directory путь исправлен без fallback offsets. Из `/proc/thread-self` строго выводятся bounded proc-visible TGID/TID; offsets читаются через `/proc/<TID>/timens_offsets`. Native TID не считается автоматически ID этого proc mount. До и после чтений offsets и собственного BOOTTIME подтверждаются birth/status и оба namespace FD выбранного task относительно удерживаемого thread-self. Повторно сверяются link/raw offsets и held/named FD. Host требует точный typed `offsets_reader` с устойчивыми bookends. Offsets остаются FOR_CHILDREN, BOOTTIME — CURRENT; старый numeric-PID comparator и current/children guards сохранены. Нет копирования self offsets, перевода часов или превращения missing/change в успех.

Объяснение структуры подтверждают upstream [proc entries Linuxv7.0](https://github.com/torvalds/linux/blob/v7.0/fs/proc/base.c), [thread-self](https://github.com/torvalds/linux/blob/v7.0/fs/proc/thread_self.c) и [time namespace offsets](https://github.com/torvalds/linux/blob/v7.0/kernel/time/namespace.c). Это reference upstream, а не доказательство точных patches установленного LinuxKit7.0.12. Его реальные наблюдения — сохранённый ENOENT и успешное чтение self offsets.

Sourceb312dfa1/Testdd1d205e прошли независимый CODE835ccaa5; свежий Root8475ed завершился0:14методов/58subTest, безfail/error/skip, с сохранением Source/Main/index entries. Fixture теперь действительно запрещает task-directory offsets; отдельно проверены nonleader/proc-native mismatch, разные raw offsets без self-copy, malformed links, пропуски/переходы process/status/обоихnamespace, FDclose и Host provenance. Это controlled pure QA с обычными FD, не Linux/nsfs/resource proof. Позднее исправлена только поясняющая строка общего размера в CODE-квитанции88855→88555; исходные active plan/CODE и фактический run binding не переписаны, ретроактивного допуска или повторного теста нет.

Выбранные выше hashes относятся к новым проверенным файлам; свежие private-свидетельства — `clock-probe-thread-offset-qa/`. Прежний Docker Source/claim/INCOMPLETE сохранён отдельно. Перед запросом ещё одной попытки новый inert-пакет связывает исправленные bytes с прежним ресурсным профилем. На том этапе pure-подготовки исправленный контейнерный прогон ещё не запускался; последующая единственная допущенная попытка описана ниже. На том этапе подготовки shared-time qualification оставалась UNKNOWN; текущий фактический результат записан ниже. Прежняя первопричина UNKNOWN; полные K5/S6/512МБ/G2/выпуск не подтверждены.


## Исправленная попытка: полная матрица, shared-time guard отказывает

Владелец явно разрешил одну исправленную попытку. Свежие input/Docker проверки пройдены; точный образ работал на Docker29.8.0/LinuxKit7.0.12. adb93c/session54324 завершился733f46/outerexit0. Все три пакета keeper-before/observer/keeper-after полны: собственные clock brackets, proc-visible reader provenance и lifetime bookends действительны. `cross_actor_binding_equal=true`: observer видит именно исходный PID1 keeper с теми же process/status/birth/namespace до и после. Смешение PID-view не объясняет этот текущий результат.

| Свидетельство в этом запуске | CURRENT `(st_dev,st_ino)` | FOR_CHILDREN совпадает | Путь offsets |
| --- | --- | --- | --- |
| Keeper до/после, PID/TID1 | `(5,4026532960)` | да | `/proc/1/timens_offsets` |
| Собственные aliases observer, PID/TID7 | `(5,4026532965)` | да | `/proc/7/timens_offsets` |
| PID1 keeper глазами observer | `(5,4026532960)` | да | `/proc/1/timens_offsets` |

Все self/thread/numeric views observer согласованы, targetkeeper привязан правильно. Но `actor_current_equal=false`, `shared_time_domain=false`: guard observer отказывает, оба keeper guard истинны. Наблюдённые monotonic/boottime FOR_CHILDREN offsets нулевые. Нули и близкие численные timestamps не заменяют namespace-identity guard и не разрешают междоменное вычитание. Это **COMPLETE_DIAGNOSTIC при неподдерживаемой shared-time политике**, а не доказательство различия каждого физического значения часов или старой причины64c0. Identity ограничены этим run/lifetime; их нельзя переносить как вечные ID между запусками.

До удаления оба containerState были exited/ExitCode0/OOMKilledfalse. Все19 Docker-команд дали0; владельцы проверены, удалены только два собственных контейнера. Свежие ownlabel+оба exactID запросыf1f454 пусты/0, wrapper завершился, lock отсутствует. Подтверждена ограниченная own-process/container/API очистка, не физическое освобождение всех backend allocations. Логический Host runtime-вывод (RAW, две квитанции, claim, console)71 647байт. Corsad/Capture/GoELF/piratecashd не запускались; память/K5/S6/G2/выпуск не квалифицированы. Первый INCOMPLETE сохранён. Оба допуска одной попытки израсходованы; автоматического повтора нет.

Свидетельства: `docs/_internal/d1-artifacts/2026-10-28/clock-probe-runtime-thread-v3/`, run `clockprobe-22f567eed40b5a212c2bc14fd38e6b1a`. ROOT-ACTUAL.json SHA256 `230bf04cf1e060b9548976d8c6c5c4697de62b1f9f7f205bf9a92fae2135c786` связывает полный manifest. TESTER8c5eaa27/REVIEWER7b99719d независимо проверили файлы, matrix joins и указанную границу очистки; фактические tool terminals и свежие Docker-запросы наблюдал Root. Наличие private-файлов на других хостах не предполагается.

### Следующее инженерное направление — предложение без допуска запуска

Для следующего полезного screen512000000байт sampler/clock reader следует поместить в service container, предпочтительно в owned-child lifecycle keeper. В неизменный общий memory limit и объявленные суммарные CPU/PID входят **corsad+keeper+sampler**, без добавления прежних observer128MiB и без вычитаний. Внешний драйвер отделён, сопоставимые случаи имеют одинаковую инструментализацию. Sampler делит CPU с daemon и способен снизить выполненную нагрузку; низкий peak не является PASS без точных объявленных workload/state-completion counts и полного покрытия без пропусков. Это консервативная граница наблюдённого daemon charge в объявленном сценарии; отдельный sampler charge, пик без инструментализации или перенос прежних чисел такой замер не доказывает. Перед полезной выборкой нужны реальные process/cgroup membership и CURRENT/FOR_CHILDREN identity, затем peak/OOM/closure; недоказанное остаётся UNKNOWN.

Это меняет прежнюю раздельную observer layout и требует нового конкретного ресурсного профиля до исполнения. Предложение здесь не реализовано и не допущено. Namespace/cgroup preflight включается в полезную ресурсную работу, вместо новой серии пустых clock-проб. В [Docker run options](https://docs.docker.com/reference/cli/docker/container/run/) и [namespace flags CLI29.3](https://github.com/docker/cli/blob/v29.3.0/cli/command/container/opts.go) нет флага join time namespace контейнера; общий PID/network в фактическом запуске его не обеспечил. Не вводить выдуманный `--time`, privileged/customruntime setns или ослабление guard ради зелёного результата.
