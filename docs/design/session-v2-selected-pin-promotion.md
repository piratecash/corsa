# Selected-neighbour pin promotion

## English

### Scope and evidence

C5-2B1 adds only the N-sel producer. Every accepted v2 proof still immediately installs the ordinary unclassified identity pin within the existing 20,000 total. There is no class reservation before ten minutes, no automatic eviction, and no change to K/R/N-in, DialIntent consumers, D1 priority, capabilities, public pin-removal RPC or release admission.

A candidate comes only from an actual proven outbound ConnectionManager session whose slot generation was accepted and whose session was published. The opaque v2 proof, ConnID and CM generation identify that connection. A monotonic, injected elapsed-time source marks proof readiness; pin timestamps, peer-health times and historical address/endpoint mappings do not establish session age. One-shot sync/notice, inbound, legacy, failed setup and stale CM generation are excluded. Close and terminal reader exit end continuity. Distinct connections and restart never accumulate pending age; already stored Eligible metadata is retained without another ten-minute wait.

The actual mandatory proof publication supplies an entry binding. Its runtime incarnation is assigned from the protection sequence on creation and never changes during reproof or metadata additions. Removal/recreation changes it, and restart changes the owner store instance. Eligibility cannot rebind an old live session to a later entry for the same identity. Source group is captured from the actual outbound dial: canonical IPv4 /16, IPv6 /32, separate loopback, direct DNS reached address, or the overlay's own Tor/I2P group. It is never inferred from a previous binding.

### Admission and persistence

One joined lifecycle worker scans the bounded existing CM sessions; wakeups coalesce instead of queuing identity jobs. A session that is observed continuously proven for at least ten minutes may earn a historical Selected fact. Later disconnect does not erase an already earned fact. Under the pin owner gate/mutex, admission atomically checks the current incarnation and the 4,096 Selected-allocation bound. Unconfirmed allocations count. An absent identity's full C3 recovery backup reserves its allocation. When a new allocation is refused because the class is full, its existing pin, session and service remain intact and unclassified; previously allocated Eligible metadata is preserved. Refusal never causes a legacy fallback.

Strict-backend preflight precedes new staging. Unsupported storage preserves normal node operation and creates no new class allocation. Memory CAS returns only staged/unconfirmed state. The strict writer takes `pinMutationGate.RLock → writeMu → mu`, captures a full snapshot, its dirty generation and the protection revisions included, releases `mu`, and forces `secretfile.DurableWrite`: file sync, replacement, parent-directory sync. Ordinary persistence success and `changes == written` never substitute for this operation.

Only `DurableCommitted` acknowledges the captured metadata whose current protection revision still matches. Confirmation is runtime-only COW state beside the optional metadata value; no second live identity map, new file/backup/guard format or acknowledgment field for unclassified entries is added. An older snapshot cannot acknowledge newer metadata or clear a later endpoint dirty generation. Reproof does not revoke an existing metadata acknowledgment.

NotCommitted, Indeterminate and errors leave counted staged metadata unconfirmed; they promise neither rollback nor unchanged disk bytes. Normal service continues. Retry can confirm the current snapshot. Stored Eligible survives restart; a strict snapshot confirmation restores runtime acknowledgment without earning eligibility anew. Cancellation before work, including while waiting for the removal gate, prevents new staging; a completed strict namespace commit remains acknowledged despite a cancellation arriving afterwards. Filesystem calls are not promised to be interruptible. No destructive C3 fence or network stop is introduced for promotion.

### Earlier preparation files and resource boundary

Format 1 still migrates to explicit unclassified. Structurally valid earlier format-2 pins and declared metadata are preserved without invented provenance or demotion. If earlier/synthetic Selected occupancy exceeds 4,096, it remains visible as OverCapacity and new allocations are refused; retaining that state does not certify compliance with the new bound. D1/PR6 readiness must not ignore OverCapacity. Unsupported metadata retains the existing fail-closed decoding rule.

The 20,000-pin and 4,096-allocation limits bound ownership, not D1 resource acceptance. The original unclassified memory bound must continue to pass; classified storage costs are measured explicitly and are not release/resource admission. N-in's independent durable daily journal remains separate.

## Русский

### Область и доказательства

C5-2B1 добавляет только producer N-sel. Каждый принятый v2 proof по-прежнему немедленно устанавливает обычный unclassified identity pin в существующем общем пределе 20 000. До десяти минут классовое место не резервируется; автоматического вытеснения и изменений K/R/N-in, DialIntent consumers, D1 priority, capabilities, публичного pin-removal RPC или допуска выпуска нет.

Кандидат возникает только из настоящей доказанной исходящей ConnectionManager-сессии, чья slot generation принята и чья сессия опубликована. Opaque v2 proof, ConnID и CM generation определяют это соединение. Внедрённый монотонный источник elapsed time фиксирует готовность proof; pin timestamps, peer-health times и прежние адресные/endpoint mappings не определяют возраст сессии. One-shot sync/notice, inbound, legacy, неудавшийся setup и устаревшая CM generation исключены. Close и terminal reader exit прекращают непрерывность. Разные соединения и restart не складывают pending age; сохранённые Eligible metadata не требуют ещё десяти минут.

Настоящая публикация обязательного proof возвращает binding записи. Его runtime incarnation назначается из protection sequence при создании и сохраняется при reproof и добавлении metadata. Удаление/повторное создание меняет incarnation, а restart — экземпляр owner store. Старую живую сессию нельзя привязать к более поздней entry той же identity. Source group фиксируется из фактического outbound dial: canonical IPv4 /16, IPv6 /32, отдельный loopback, достигнутый адрес direct DNS либо собственная Tor/I2P-группа overlay. Она не выводится из прежней привязки.

### Admission и persistence

Один joined lifecycle worker просматривает ограниченный набор существующих CM sessions; wakeups объединяются без очереди identity jobs. Сессия, наблюдаемая непрерывно доказанной не менее десяти минут, может заработать исторический Selected fact. Последующий disconnect не стирает уже заработанный факт. Под gate/mutex владельца pin admission атомарно проверяет текущую incarnation и предел 4096 Selected allocations. Неподтверждённые allocations учитываются. Полный C3 recovery backup отсутствующей identity резервирует её allocation. При отказе новой allocation из-за заполненного класса её существующие pin, сессия и обслуживание сохраняются в unclassified; ранее выделенные Eligible metadata сохраняются. Автоматического legacy fallback нет.

Preflight строгого backend предшествует новому staging. Неподдерживаемое хранилище сохраняет обычную работу узла без новой классовой allocation. Memory CAS возвращает только staged/unconfirmed. Strict writer берёт `pinMutationGate.RLock → writeMu → mu`, снимает полный snapshot, его dirty generation и включённые protection revisions, освобождает `mu` и принудительно вызывает `secretfile.DurableWrite`: file sync, replacement, parent-directory sync. Успех обычного persistence и `changes == written` эту операцию не заменяют.

Только `DurableCommitted` подтверждает снятые metadata, чья текущая protection revision по-прежнему совпадает. Confirmation — runtime-only COW state рядом с optional metadata value; второй живой identity map, нового file/backup/guard format и поля acknowledgment у unclassified entries нет. Старый snapshot не подтверждает новые metadata и не очищает более позднюю endpoint dirty generation. Reproof не отменяет существующий metadata acknowledgment.

NotCommitted, Indeterminate и ошибки сохраняют учтённые staged metadata неподтверждёнными, не обещая rollback или прежних disk bytes. Обычное обслуживание продолжается. Retry может подтвердить текущий snapshot. Сохранённый Eligible переживает restart; strict snapshot confirmation восстанавливает runtime acknowledgment без повторного заработка eligibility. Отмена до работы запрещает новую работу; законченный strict namespace commit остаётся подтверждённым при поздней отмене. Прерываемость filesystem calls не обещается. Destructive C3 fence и остановка сети ради promotion не добавляются.

### Прежние preparation-файлы и ресурсная граница

Формат 1 по-прежнему мигрирует в явный unclassified. Структурно корректные прежние pins и объявленные metadata формата 2 сохраняются без выдуманного происхождения и demotion. Если прежняя/синтетическая Selected occupancy превышает 4096, она остаётся видимой как OverCapacity, новые allocations отклоняются; сохранение этого состояния не подтверждает соблюдение нового предела. D1/PR6 readiness не вправе игнорировать OverCapacity. Неподдерживаемые metadata сохраняют прежний fail-closed decode.

Пределы 20 000 pin и 4096 allocations ограничивают ownership, а не утверждают ресурсный допуск D1. Прежний предел памяти unclassified должен оставаться зелёным; стоимость classified-хранения измеряется явно и не является допуском ресурса/выпуска. Отдельный durable daily journal N-in остаётся другой задачей.
