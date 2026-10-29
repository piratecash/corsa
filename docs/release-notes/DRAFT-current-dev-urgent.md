# DRAFT — current DEV, version TBD

**Not approved for publication.** Final commit, version, artifact SHA-256s and
final-candidate evidence are pending. This draft covers the daemon and Desktop
since published v2.4.70, including reviewed regression fixes in the working
candidate; it does not describe an already tagged clean commit.

## English

The owner selected the usual GitHub daemon+Desktop matrix: Linux amd64,
macOS arm64/amd64 and Windows amd64/arm64. Android APK/AAB follow separately.
Only overlaysim is DEFERRED to D1, not PASS; generic make test-all is not green.
All applicable emitted-product, routing, TLS and pin tests, builds, crypto
review and applicable 17-condition admission requirements remain.

### What changes

- **Secure peer sessions.** Protocol 31/minimum 28 adds TLS 1.3 sessions with
  connection-bound identity proofs and persistent v2 pins/endpoint protection.
  Legacy compatibility is limited to supported older peers and a validated
  legacy welcome; a failed TLS connection alone does not authorize downgrade.
  Signing-key validation rejects noncanonical and small-order keys at ingress.
- **Safer reconnects and shutdown.** Dial paths use the same authentication
  gate. Local pin-store errors/refusals and cancellation do not punish a healthy
  remote peer. POSIX pin/marker persistence confirms directory and ancestor
  entries before acknowledging success; Windows durability remains unverified.
  Connection reservations remain owned until Close returns;
  shutdown joins admitted background/session work. Routing and delivery fixes
  preserve session-owned direct claims and check acknowledgements against the
  captured sender identity and current local forwarding/message state. They
  add no guarantee of on-wire attempt freshness.
- **Less history-dependent bookkeeping.** Route-health and desktop identity
  bookkeeping no longer retain unrelated entries simply because the node saw
  them in the past. Ordinary resource polling omits an unpublished orphan
  diagnostic series instead of scanning that history or reporting a fake zero.
  Traffic totals remain cumulative when sessions close or peers are evicted.
  These changes are not a measured RSS reduction or memory-cap guarantee.
- **Read receipts follow the reader.** Selecting a chat no longer marks unseen
  messages read. New messages received while scrolled up remain below the
  current view with an unread marker instead of pulling the reader down.
- **Delete/wipe retries use exact time ordering.** Due selection and reconnect
  revival compare UTC nanoseconds correctly across different fraction lengths.
  Schema 8 adds two indexes without rewriting stored timestamps or task rows;
  unsupported active timestamps cause an explicit migration refusal.
- **Data-directory and startup diagnostics.** `CORSA_DATA_DIR` keeps node-local
  state in the configured existing directory and refuses a missing directory
  instead of recreating it outside an encrypted mount. Unsafe, unreadable or
  unexpectedly missing pin state prevents startup, with an actionable error.

TLS-first is on by default. `CORSA_V2_TLS_FIRST=off` changes the first attempt
only where legacy probing is permitted; it does not disable v2 listening,
erase pins or allow fallback where v2 is already required. SDK configuration
uses its explicit `NodeConfig.V2TLSFirst` field, not environment inheritance.

For the owner-selected PirateCash commit
`071855db490b1f493bd2a985fa390e9620c8db0e`, the accepted compatibility criterion
is protocol only: Corsa 31 meets its required 31. Real interaction with
piratecashd has not been run and is not claimed by that source check.

### Before upgrading or rolling back

Preserve a coherent identity, pin store and marker backup before an approved
upgrade. The store is normally `secure-sessions-<port>.json`; its marker follows
the identity path. On POSIX, files must belong to the node user with mode 0600,
and their directory route must pass the owner/permission audit. Do not delete
trust state or change identity/port merely to bypass a refusal. Follow the
[operator migration draft](DRAFT-pin-store-operator-migration.md) and
[session migration guidance](../protocol/session_v2.md).

A safe, writable POSIX leaf is not sufficient: durable pin/marker confirmation
also needs this process to read-open and synchronize every real ancestor in
the audited route, including symlink-resolution parents. A search-only
ancestor can cause a local permission/durability refusal. Keep identity, pins
and marker; do not widen system-directory permissions, delete protection or
force legacy fallback. Use controlled data-directory migration or support.
The owned OS-only probe demonstrated this access distinction; it did not run
the Go pin writer or Android. No Android or device failure is claimed.

Docker runs as UID 10001 and persists data at `/home/corsa/.corsacore`, not
`/home/corsa/.corsa`. Retain the existing identity and pin state when correcting
a mount; prepare the volume for that user rather than using a writable 0777
directory. See the [Docker instructions](../../README.md#docker).

Rollback is limited to a qualified secure-session-aware build that understands
the saved protection state. A pre-v2 binary is not a safe downgrade. Clients
and SDK databases that commit schema 8 cannot be opened by a catalog ending at 7;
in-place binary rollback is unsupported. Establish a verified,
SQLite-consistent pre-upgrade backup using the ordered shutdown/recovery
procedure in the [migration 8 notes](../operations/delete-intent-time-order-migration.md).
Do not copy a live `.db` alone, discard WAL files or edit the ledger to force
an older binary to open it. Index creation has a one-time scan/disk/CPU cost.

### Boundaries and release status

Session K2/R1b/R2/N-in activation is deferred to the next release. Ordinary
shipped constructors/config/RPC keep it off; explicit internal source opt-in
APIs are separate. D1/FindNode/F1 activation is not included. Identity-pin
removal and destructive endpoint remove/demote operations remain unavailable;
read-only inspection does not enable mutation.

Windows runtime/DACL and real Tor checks remain **DEFERRED / UNVERIFIED**,
not passed. Full applicable suite/platform/client startup, final hashes,
independent crypto review, resource/latency/compatibility evidence, observation
criteria and final admission remain required. This draft claims neither that
all network attacks are closed nor fitness within 512,000,000 bytes. Release
admission remains **CLOSED** until required evidence and the owner decision
are accepted. A draft, unit pass or build is not permission to publish.

---

## Русский

Владелец выбрал обычную GitHub-матрицу демон+Desktop: Linux amd64,
macOS arm64/amd64 и Windows amd64/arm64. Android APK/AAB выпускаются отдельно
позже. Только overlaysim DEFERRED до D1, а не PASS; общий make test-all
не зелёный. Все применимые тесты выпускаемых продуктов, routing, TLS и pins,
сборки, crypto review и применимые требования допуска из 17 пунктов сохраняются.

### Что меняется

- **Защищённые peer-сессии.** Протокол 31/минимум 28 добавляет TLS 1.3 с
  доказательствами identity, привязанными к соединению, и сохраняемой защитой
  v2 identity/endpoint. Legacy-совместимость ограничена поддерживаемыми старыми
  пирами и проверенным legacy welcome; одного сбоя TLS для downgrade
  недостаточно. На входе отвергаются неканонические signing keys и ключи малого
  порядка.
- **Переподключение и shutdown.** Пути дозвона используют единый authentication
  gate. Локальные ошибки/отказы pin store и отмена не наказывают исправного пира.
  POSIX-запись pin/marker подтверждает записи каталогов и предков до успешного
  подтверждения; durability Windows остаётся непроверенной. Reservation
  соединения удерживается до возврата Close; shutdown дожидается
  принятой фоновой и session-работы. Исправления routing/delivery сохраняют
  владение direct claims сессией и проверяют ACK по захваченной identity
  отправителя и текущему локальному forwarding/message state. Новая гарантия
  свежести конкретной попытки на проводе не добавляется.
- **Меньше bookkeeping, зависящего от истории.** Route-health и desktop-кеш
  identity больше не удерживают посторонние записи только потому, что узел
  встречал их раньше. Обычный resource polling исключает неопубликованную
  orphan-серию вместо сканирования этой истории или выдачи фиктивного нуля.
  Traffic totals сохраняются при закрытии сессии или вытеснении пира. Это
  не измеренное уменьшение RSS и не гарантия предела памяти.
- **Read receipts следуют за читателем.** Выбор чата больше не помечает
  невидимые сообщения прочитанными. При прокрутке вверх новые сообщения
  остаются ниже текущего вида с unread marker и не тянут читателя вниз.
- **Delete/wipe retries сравнивают точное время.** Выбор due-заданий и revival
  при переподключении правильно сравнивают UTC-наносекунды с дробями разной
  длины. Schema 8 добавляет два индекса без переписывания timestamp и task rows;
  неподдерживаемые активные timestamps вызывают явный отказ миграции.
- **Каталог данных и диагностика старта.** `CORSA_DATA_DIR` сохраняет локальное
  состояние в заданной существующей папке и отказывает при её отсутствии,
  вместо создания папки за пределами encrypted mount. Небезопасное, нечитаемое
  или неожиданно пропавшее состояние pin не позволяет стартовать; ошибка
  объясняет причину.

TLS-first включён по умолчанию. `CORSA_V2_TLS_FIRST=off` меняет первую попытку
только там, где разрешена legacy-проба; это не отключает v2 listener,
не стирает pins и не разрешает fallback там, где уже требуется v2.
SDK использует явное поле `NodeConfig.V2TLSFirst`, а не наследует окружение.

Для выбранного владельцем commit PirateCash
`071855db490b1f493bd2a985fa390e9620c8db0e` согласован только протокольный
критерий совместимости: Corsa 31 соответствует требуемому 31. Реальное
взаимодействие с piratecashd не запускалось и этим Source-check не заявляется.

### Перед обновлением или откатом

До разрешённого обновления сохраните согласованную резервную копию identity,
pin store и marker. Обычный путь store — `secure-sessions-<port>.json`,
marker следует за identity. На POSIX файлы должны принадлежать пользователю
узла с mode 0600, а путь каталогов — проходить проверку владельца и прав.
Не удаляйте trust state и не меняйте identity/порт ради обхода отказа.
Используйте [операторский migration draft](DRAFT-pin-store-operator-migration.md)
и [руководство по session migration](../protocol/session_v2.md).

Безопасного writable-каталога на POSIX недостаточно: для durable pin/marker
подтверждения процесс должен открыть для чтения и синхронизировать каждого
реального предка проверенного пути, включая родителей symlink-resolution.
Search-only предок может вызвать локальный permission/durability refusal.
Сохраните identity, pins и marker; не расширяйте права системных каталогов,
не удаляйте защиту и не форсируйте legacy fallback. Используйте контролируемый
перенос каталога данных или поддержку. Собственный OS-only probe показал лишь
это различие доступа; он не запускал Go pin writer или Android. Сбой Android
или устройства не заявляется.

Docker работает с UID 10001 и хранит данные в `/home/corsa/.corsacore`,
а не `/home/corsa/.corsa`. При исправлении mount сохраните прежние identity
и pins; подготовьте том под этого пользователя вместо writable-папки 0777.
См. [инструкцию Docker](../../README.md#docker).

Откат допустим только на проверенную secure-session-aware сборку, понимающую
сохранённую защиту. Pre-v2 binary не является безопасным downgrade. База
клиента/SDK после commit schema 8 не открывается каталогом, заканчивающимся на 7;
in-place rollback бинарника не поддерживается. Заранее обеспечьте проверенный
SQLite-consistent backup до обновления по ordered shutdown/recovery процедуре
из [заметок о миграции 8](../operations/delete-intent-time-order-migration.ru.md).
Не копируйте отдельно live `.db`, не отбрасывайте WAL и не редактируйте ledger
ради открытия старым binary. Создание индексов имеет разовую стоимость
сканирования, диска и CPU.

### Границы и статус выпуска

Активация session K2/R1b/R2/N-in отложена до следующего релиза. Обычные
поставляемые конструкторы/config/RPC сохраняют её выключенной; явные внутренние
opt-in API исходников — отдельный путь. Активация D1/FindNode/F1 не включается.
Удаление identity pins и разрушительные endpoint remove/demote остаются
недоступными; read-only просмотр не включает мутации.

Runtime/DACL Windows и реальные проверки Tor остаются **DEFERRED / UNVERIFIED**,
а не PASS. Ещё требуются полный применимый набор тестов, платформы и старт
клиентов, итоговые хеши, независимая crypto review, свидетельства ресурсов,
задержек и совместимости, критерии наблюдения и итоговый допуск. Черновик
не заявляет закрытие всех сетевых атак или пригодность в 512000000 байт.
Допуск выпуска остаётся **ЗАКРЫТЫМ** до принятия обязательных свидетельств
и решения владельца. Черновик, unit PASS или сборка не разрешают публикацию.
