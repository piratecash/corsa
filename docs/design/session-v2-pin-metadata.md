# Identity-pin metadata and recovery format

## English

### Implemented boundary

This first C5 increment adds storage and recovery infrastructure for the [accepted pin-class contract](session-v2-pin-classes.md). Production proofs still create unclassified pins. It does not enable K/N-sel/N-in/R admission, ten-minute eligibility, lookup provenance producers, D1 service priority, new dial consumers, public pin removal, or release admission.

`secureSessionStore.identities` remains the sole live owner. Each `identityPinEntry` retains its original protection timestamp and runtime proof revision, plus an entry-owned immutable pointer to value-only `domain.IdentityPinMetadata`. A nil internal pointer is the canonical representation of unclassified; domain and persistence APIs always expose the explicit value. Metadata updates allocate a new owned copy, and snapshots and backups copy values; caller, snapshot and earlier-entry mutations cannot alter the published entry. Reproof advances the proof revision while preserving timestamp and metadata. Up to four bounded class allocations may coexist; they share one global pin place. An allocation records its explicit absent/pending/eligible state, typed local origin, timestamp, canonical source group where applicable and a reference of at most 128 bytes. K is exempt from class quotas; the stored contact fact is historical provenance, not proof of current contact trust. Future live-session priority must check current state separately.

All unique pins, including unclassified pins, remain subject to the accepted 20,000 total. Existing C3 restoration reservations continue to occupy the missing identity's one place; reproof of that same identity does not take a second place. No pin is evicted, and no class metadata is inferred from endpoint bindings, imported keys, address mappings, missing fields or current trust. Class admission and quota accounting remain a later increment.

### Main store format 2

The main file now writes format 2. `identities` retains every identity/timestamp; the derived `identity_metadata` snapshot contains exactly the same identity keys. This companion map is a serialization DTO, not a second live owner. Format 2 requires explicit versioned metadata for every pin, including `{"version":1,"classification":"unclassified"}`. Missing, foreign, unsupported or inconsistent metadata refuses loading. Unknown file fields also refuse loading rather than disappearing on the next save.

A genuine format-1 main file without metadata migrates every existing pin to explicit unclassified state, retaining all timestamps and endpoint protections. The in-memory snapshot is format 2; the next successful store save writes it atomically through the existing persistence path. Migration invents no class or source history. This schema adds storage overhead; previous pin-file size estimates do not establish a D1 resource budget for it.

Old readers reject format 2. After it has been saved, rolling back only the node binary is unsupported. Preserve backups of the complete main store and its matching recovery artifacts before upgrading; do not edit the format number or strip metadata to make an older binary start.

### Independent full-entry recovery backup

C3's external fence binding and permit now carry `domain.IdentityPinBackup`: backup version 1, a canonical public pin-record payload, and a domain-separated SHA-256 digest. The payload binds the identity, original timestamp and complete metadata. It is limited to 2,048 bytes; invalid versions, unknown fields, mismatched identities/timestamps, noncanonical encoding, changed digests and oversized payloads refuse admission. Runtime proof counters, private keys and preview HMAC keys are not persisted in the backup.

The external broker retains that full backup in its own SQLite ledger across restart. Consequently, recovery can restore the complete entry even when both the node-local guard and the deleted main-file entry are absent. It does not reconstruct provenance from local trust or endpoints. If a later normal proof has already restored the identity, recovery preserves that complete valid existing entry, including any newer metadata and timestamp; the old backup restores only a missing entry. Online rollback, offline recovery, main snapshots and guards retain the same metadata.

The guard is now format 2 and includes the full permit. Before `BeginRemoval`, the node checks that even a maximum-length operation ID fits the existing 8,192-byte guard bound. The backup is also bounded before a broker admission or local write. Existing private-channel frames remain bounded to 16 KiB; HTTP response limits remain unchanged.

**Upgrade boundary:** a guard of format 1 or a persisted active broker permit without a valid version-1 full backup is refused. Finish that older candidate's fenced recovery with the matching older candidate before upgrading the node/broker pair. Missing backup context is never treated as unclassified. Historical main format 1 is migratable because its verified schema explicitly predates metadata; an unknown active permit is a different case. Do not roll back the broker while an upgraded permit or receipt is outstanding: an older broker does not understand the new backup semantics. Preserve the independent ledger; refusal is not permission to clear its fenced state.

### Preview and concurrency

Removal confirmation binds the runtime proof revision, a separate monotonic runtime protection revision, and a versioned canonical metadata commitment. The [C5-2A owner CAS preparation](session-v2-pin-metadata-cas.md) prevents metadata A → B → A from reviving a preview; its staging result is explicitly memory-only and enables no live class producer. A metadata change invalidates the previous preview even without reproof. Restart still invalidates previews through the fresh process key. The backup in the external binding must match the current complete entry during confirmation recheck.

The lock order remains `pinMutationGate → writeMu → mu`. Proof publication uses the read gate; destructive removal and recovery use the exclusive gate. Snapshot copying under `mu` copies values. Full-file encoding, filesystem I/O and external-fence I/O occur outside `mu`; bounded single-record metadata encoding for preview and binding validation occurs under the store lock. There is no new lock, token map or metadata owner. All production class producers remain inactive in this increment.

## Русский

### Реализованная граница

Первый инкремент C5 добавляет хранение и восстановление для [принятого контракта классов pin](session-v2-pin-classes.md). Production proof по-прежнему создаёт неклассифицированный pin. Инкремент не включает admission K/N-sel/N-in/R, десятиминутную eligibility, producers происхождения lookup, приоритет обслуживания D1, новые dial consumers, публичное снятие pin или допуск выпуска.

Единственный живой владелец — `secureSessionStore.identities`. В `identityPinEntry` сохраняются исходный timestamp защиты и runtime proof revision, а также принадлежащий записи immutable pointer на value-only `domain.IdentityPinMetadata`. Внутренний nil — каноническое представление unclassified; domain и persistence API всегда предоставляют явное значение. Обновление metadata создаёт новую принадлежащую записи копию; снимки и backup копируют значения. Изменения caller, снимка и прежней записи не могут изменить опубликованную запись. Reproof повышает proof revision, сохраняя timestamp и metadata. До четырёх ограниченных классовых allocations могут сосуществовать, занимая одно глобальное место pin. Allocation содержит явное состояние absent/pending/eligible, типизированное локальное происхождение, timestamp, каноническую source group там, где она применима, и reference не длиннее 128 байт. K освобождён от классовых квот; сохранённый факт контакта — историческое происхождение, а не доказательство текущего доверия. Будущий приоритет живой сессии отдельно проверяет текущее состояние.

Все уникальные pin, включая неклассифицированные, остаются под принятым общим пределом 20 000. Существующие C3 restoration reservations сохраняют одно место отсутствующей identity; повторный proof этой identity не занимает второе. Вытеснения нет; классы не выводятся из endpoint-привязок, импортированных ключей, адресных карт, отсутствующих полей или текущего trust. Admission и учёт классовых квот остаются следующим инкрементом.

### Формат основного хранилища 2

Основной файл теперь записывается в формате 2. `identities` сохраняет каждую identity и timestamp; производный снимок `identity_metadata` содержит ровно те же ключи identity. Эта companion map — DTO сериализации, а не второй живой владелец. Формат 2 требует явные версионированные metadata каждого pin, включая `{"version":1,"classification":"unclassified"}`. Отсутствующие, чужие, неизвестные или несогласованные metadata запрещают загрузку. Неизвестные поля файла тоже запрещают загрузку, а не исчезают при следующем сохранении.

Настоящий основной файл формата 1 без metadata мигрирует все прежние pin в явное состояние unclassified, сохраняя timestamps и endpoint-защиту. Снимок в памяти имеет формат 2; следующее успешное сохранение атомарно записывает его через существующий persistence path. Миграция не придумывает классы и историю источников. Схема увеличивает объём хранения; прежние оценки размера pin-файла не утверждают для неё ресурсный бюджет D1.

Старый reader отказывает формату 2. После его сохранения откат только бинарного файла узла не поддерживается. До обновления сохраните копию полного основного файла с согласованными recovery-артефактами; не меняйте номер формата и не удаляйте metadata ради запуска старой сборки.

### Независимая резервная запись полного pin

В binding и permit внешнего fence C3 добавлен `domain.IdentityPinBackup`: версия backup 1, канонический публичный pin-record payload и SHA-256 с отдельным доменом. Payload связывает identity, исходный timestamp и все metadata. Предел — 2048 байт; неизвестная версия или поля, несовпадающие identity/timestamp, неканоническая кодировка, изменённый digest и превышение размера запрещают admission. Runtime proof counters, приватные ключи и HMAC-ключ preview в backup не сохраняются.

Внешний broker сохраняет полный backup в собственной SQLite ledger после restart. Поэтому recovery восстанавливает всю запись даже при одновременном отсутствии локального guard и удалённой записи в основном файле. Происхождение не восстанавливается из trust или endpoints. Если последующий нормальный proof уже восстановил identity, recovery сохраняет всю существующую корректную запись вместе с более новыми metadata и timestamp; прежний backup восстанавливает только отсутствующую запись. Online rollback, offline recovery, снимки и guard сохраняют одинаковые metadata.

Guard теперь имеет формат 2 и содержит полный permit. До `BeginRemoval` узел проверяет, что даже максимальный operation ID помещается в существующий предел guard 8192 байта. Размер backup ограничен до broker admission или локальной записи. Private-channel frames по-прежнему ограничены 16 КиБ; лимиты HTTP-ответов не меняются.

**Граница обновления:** guard формата 1 и активный permit, сохранённый broker без корректного полного backup версии 1, отвергаются. До обновления пары node/broker завершите прежний fenced recovery соответствующим старым candidate. Отсутствующий контекст backup никогда не считается unclassified. Основной формат 1 мигрируется, поскольку его проверенная схема явно предшествует metadata; неизвестный активный permit — другой случай. Не откатывайте broker при незавершённом новом permit или receipt: старый broker не понимает новую семантику backup. Сохраните независимую ledger; отказ не разрешает снимать её fenced state.

### Preview и конкурентность

Подтверждение снятия связывает runtime proof revision, отдельную монотонную runtime protection revision и версионированный commitment канонических metadata. [Подготовка owner CAS в C5-2A](session-v2-pin-metadata-cas.md) не позволяет metadata A → B → A вернуть preview валидность; результат staging явно относится только к памяти и не включает живые producers классов. Изменение metadata отменяет прежний preview даже без reproof. Restart по-прежнему отменяет preview новым ключом процесса. При confirmation recheck backup внешнего binding обязан совпасть с полной текущей записью.

Порядок блокировок прежний: `pinMutationGate → writeMu → mu`. Публикация proof использует read gate; снятие и recovery — exclusive gate. Снимки под `mu` копируют значения. Кодирование полного файла, файловый и внешний fence I/O выполняются вне `mu`; ограниченное кодирование metadata одной записи для preview и проверки binding выполняется под блокировкой хранилища. Новых мьютексов, token map и владельцев metadata нет. Все production producers классов в этом инкременте остаются неактивными.
