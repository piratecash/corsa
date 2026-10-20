# Optional saved degree statistics / Необязательная статистика степени

## English

`peers-<port>.json` may contain this optional field:

```json
"overlay_d_win": {"value": 64, "at": "2026-10-20T06:00:00Z"}
```

It is a saved statistic, separate from identity pins and session protection.
The intended future first-window policy uses a saved value younger than 24 hours,
clipped to 64, or capacity 512 when there is no usable hint. The upper capacity
would be 3840. This codec and persistence slice does not create that consumer,
change the fixed-cap intake constructor, update live degree, or activate FindNode.

The encoded optional JSON **value** is limited to 256 bytes before its own token
decoder is allocated. This does not limit `os.ReadFile`, whole-document JSON
validation, top-level key length or the size of the existing peer-state file.
Only a bounded scalar value, its original timestamp and a validity flag remain
in `Service`. Raw JSON and duplicate-tracking decoder state are not retained.
No memory, RSS, disk-write or latency acceptance is implied by the byte limit.

The value accepts decimal JSON integers from 0 through 18446744073709551615.
Accepted integers of 64 or more are clipped to 64 before conversion or arithmetic.
Representation overflow, negative values, fractions, exponents and numeric strings
make only the hint unusable. Both members, `value` and `at`, are required once.
Unknown members, duplicate members, invalid time or an encoded value above the
bound also make the hint unusable. Decoded escaped member names count as the same
member. The exported DTO field has a private value type; this is not a new API.

Repeated recognized outer `overlay_d_win` members are invalid, including case
aliases, escaped names and null between two values. The decoder uses a value
field with sticky duplicate state, so a later null cannot reset a pointer and
let another occurrence win. Malformed optional values do not discard otherwise
valid peers, either ban direction or first-hop guards. Invalid whole-document
JSON and existing non-hint type errors keep the previous load-error behavior.
The peer-state version remains unchanged.

Freshness uses wall time only for this optional statistic: the timestamp must be
nonzero, not in the future, and less than 24 hours old. Exactly 24 hours is stale.
This is separate from N-in's security-clock accounting and the owned monotonic
clock required for live degree observations. A future hint discarded on load is
not retained to become valid later by waiting.

The ordinary common constructor copies the validated scalar from the loaded
peer state. Every existing periodic, debounced or shutdown flush uses the same
`flushPeerState` assembly and `savePeerState` writer. It copies the scalar with
the peer snapshot under `peerMu`, releases the existing peer/IP locks, checks
freshness with a new wall-time sample, and includes the optional field in the
actual saved DTO. The original `at` is preserved. File `UpdatedAt` describes the
file write and never refreshes the observation timestamp. A node with no active
candidate can preserve a fresh loaded hint through normal saves, but saves do
not keep it fresh indefinitely; a stale copy is omitted.

There is no second writer, new worker, timer, settings option, producer, public
setter or clock callback. Provider snapshots remain outside `peerMu`, the peer
and IP lock order is unchanged, and disk I/O remains outside those locks. The
existing dirty flag is cleared at snapshot time, marked again on a failed write,
and not cleared after the write. The existing `os.WriteFile` behavior is retained;
this statistic has no new atomic-commit or durability guarantee.

Tests are authored for actual JSON and load paths, the ordinary common
constructor followed by the real flush and reload, exact freshness and byte
boundaries, duplicates, integer overflow, old migration and failed-write retry.
Source guards supplement those functional assertions with a finite reference
and writer-order inventory. A concurrent blocked-write experiment is not added:
there is no new blocking I/O seam or runtime callback. The source freeze and its
handoff distinguish authored tests from tests actually executed later by QA.

A future live producer and the real Node/FindNode clock, ownership and membership
bridge are separate work. A complete genuine degree cut would need to become a
bounded scalar outside degree/intake/peer locks and use this existing writer.
A saved hint cannot stand in for that cut, a session proof or admission authority.
Numeric deployment profiles, resource acceptance, G2 and activation remain
separate decisions and checks.

## Русский

Файл `peers-<port>.json` может содержать необязательное поле
`overlay_d_win {value, at}`. Это сохранённая статистика узла, отдельная от identity
pins и защиты сессий. Для будущего первого окна предусмотрено сохранённое значение
моложе 24 часов с ограничением 64; при отсутствии пригодной записи используется
ёмкость 512. Верхняя ёмкость составит 3840. Этот срез добавляет только кодек и
сохранение: потребитель, живая степень и активация FindNode здесь не появляются.

Предел 256 байт относится к закодированному JSON-значению необязательного поля до
создания его токенного декодера. Он не ограничивает чтение и первичную проверку
всего файла, длину ключей верхнего уровня или общий размер peer-state. В `Service`
остаются только ограниченное число, исходное время и признак пригодности.
Сырые JSON-данные и состояние обнаружения дубликатов туда не копируются. Этот
предел не подтверждает расход памяти, RSS, записи на диск или время выполнения.

Допустимы десятичные целые JSON-числа от 0 до 18446744073709551615. Допустимые
значения от 64 ограничиваются числом 64 до преобразования типа и арифметики.
Переполнение представления, отрицательные числа, дроби, экспоненциальная запись
и числовые строки делают только hint непригодным. Нужны ровно по одному `value`
и `at`. Неизвестные или повторяющиеся члены, неверное время и превышение предела
также отбрасывают только hint. Экранированные написания одного ключа считаются
повтором. Экспортируемое поле DTO использует приватный тип и не добавляет API.

Повтор внешнего поля тоже запрещён: учитываются регистр, экранированные имена и
`null` между значениями. Поле хранится значением, а признак уже увиденного поля
сохраняется при каждом повторе. Поэтому `null` не сбрасывает указатель и не даёт
последующему значению стать допустимым. Ошибка необязательного значения не удаляет
правильные peers, оба направления bans и first-hop guards. Повреждение всего JSON
или прежние ошибки типов других полей обрабатываются как раньше. Версия файла
не меняется.

Пригодность статистики проверяется по настенным часам: время ненулевое, не из
будущего и моложе 24 часов. Ровно 24 часа уже не подходит. Эта проверка не связана
с учётом времени N-in или монотонными часами живой степени. Отброшенное при загрузке
будущее значение не сохраняется в ожидании момента, когда оно станет пригодным.

Обычный общий конструктор копирует проверенную статистику из загруженного файла.
Существующий `flushPeerState` снимает её вместе с peer snapshot под `peerMu`, затем
освобождает прежние peer/IP-блокировки, заново проверяет возраст и передаёт поле
в настоящий DTO единственного существующего writer. Исходное `at` не меняется.
`UpdatedAt` относится к записи файла и не продлевает срок статистики. Обычные
сохранения могут удержать свежую загруженную запись, но не освежают её бесконечно:
просроченная запись не выводится.

Нового writer, worker, таймера, настройки, producer, публичного setter или callback
часов нет. Порядок блокировок и снятие provider snapshot сохранены. Запись на диск
остаётся вне этих блокировок. Dirty очищается при снятии snapshot, снова ставится
при ошибке записи и не очищается после неё. Прежний `os.WriteFile` не заменяется
новой транзакцией; гарантия атомарного commit или новой надёжности здесь не заявлена.

Подготовлены тесты настоящих JSON/load-путей, общего конструктора и настоящего
flush с повторной загрузкой, границ времени и размера, дубликатов, переполнения,
старой миграции и повторной записи после ошибки. SOURCE-проверки дополняют их
конечным перечнем ссылок и порядка writer. Искусственный блокирующий I/O-порт или
callback ради конкурентного теста не добавляется. Handoff отдельно указывает,
какие проверки пока только написаны и что позже действительно запускал QA.

Живой producer и настоящий мост Node/FindNode для времени, владельца и membership
остаются отдельной работой. Сохранённый hint не заменяет полный живой degree cut,
доказательство сессии или право допуска. Численные профили выпуска, ресурсная
приёмка, G2 и активация требуют своих решений и проверок.


## Dormant live producer: preserve prior coverage / Неактивный производитель

The separate unused `newDegreeStatisticCandidateService` root supplies statistics
only. It requires the existing finite positive `degreeProfile.MaxActive`, creates
one ordinary Service through the concrete production degree construction graph,
and binds one immutable producer after the all-ProvenSession observer is
installed. Ordinary constructors and the older degree and C5 roots leave the
producer nil. There is no configuration switch, admission consumer, bridge,
capability, new worker, timer, writer or save caller.

The owner approved preservation until a complete trailing 600 seconds has been
observed after tracker installation. Construction time before installation does
not count. A single concrete `time.Now()` sampled under the owner's mutex pairs
original monotonic elapsed time with its own wall instant. The high-water mark
advances before partial or unknown-coverage refusal. A negative, saturated,
regressing, poisoned, foreign or invalid observation supplies no optional value;
it never creates zero or refreshes a prior timestamp. Fixture elapsed clocks
cannot be paired with unrelated runtime wall time.

A complete mature observation projects the current terminal basis and every
bounded active witness using fixed 64 + 65 workspace. It leaves terminal history
unchanged. Genuine complete zero replaces a nonzero prior value; no maximum of
old and new is taken. During partial observation or refusal, the original prior
value and original `at` remain unchanged. Repeated short restarts preserve that
same instant until the ordinary future/24-hour validation makes it unusable.
An absent or stale hint retains the separate first-window fallback of 512; this
producer installs no first-window consumer and changes no frozen capacity or
debt. It does not reconstruct previous-run identities from a scalar.

The existing flush captures the optional observation before provider/peer/IP
locks, copies a valid bounded candidate in the existing peer snapshot, then
revalidates after those locks. Wall validation never restamps observation time.
The sole existing disk writer remains outside these locks. Failed saves preserve
`lastPeerSave` and re-mark dirty; an unavailable retry keeps the accepted original
instant. `UpdatedAt` is still independent. This promises neither freshness at
IO completion nor a new save cadence or durability deadline.

Tests distinguish actual runtime partial/genuine P1 and existing flush/reload
checks from pure arithmetic 599/600, projection and selection fixtures. No runtime
origin is rewound to pretend that 600 seconds passed. Clock-pairing and constructor
closure also have finite SOURCE oracles. Compilation, execution, resources, G2,
activation and release remain separate; this implementation is inactive.

Владелец утвердил сохранение прежнего значения и исходного `at`, пока после
установки наблюдателя не пройдут полные 600 секунд наблюдения. Время построения
узла до установки наблюдателя не засчитывается. Один настоящий `time.Now()` под
мьютексом владельца даёт согласованную пару монотонного времени и wall timestamp.
Неполное, неизвестное или недопустимое наблюдение не превращается в ноль и не
обновляет старый `at`. Короткие перезапуски сохраняют тот же timestamp; после
24 часов прежняя проверка делает его непригодным. Настоящий полный ноль после
созревания заменяет ненулевое значение без выбора максимума.

Новый конструктор не используется обычным запуском. Старые degree/C5 и обычные
конструкторы не включают производителя. Профиль остаётся обязательным и конечным;
значение M для эксплуатации не выбирается. Новых фоновых задач, писателей,
потребителя допуска, capability, публичного setter или настроек нет. Проекция
использует фиксированную память и не меняет историю. Единственный существующий
flush сохраняет исходное время наблюдения, прежний dirty/retry порядок и
независимость `UpdatedAt`. Реальные проверки 600 секунд не заявляются: граничные
случаи времени проверяются чистой арифметикой. G2, активация, ресурсная приёмка и
выпуск остаются отдельными.
