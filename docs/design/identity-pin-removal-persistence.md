# Identity pin removal: persistence and recovery proposal

## English

### Status

Transaction proposal for the [owner's S6 decision](identity-pin-removal-decision.md). The owner has
approved the distinct `RecoveryRequired` outcome, subject to a concrete external restart fence and
confirmed recovery before removal RPC is enabled. A local transaction and [external supervised candidate](../operations/identity-pin-recovery-candidate.md) are implemented. The owner approved the Linux stop/finalize workflow on 2026-10-18; the dedicated desktop dialog is implemented; deployment provisioning, admission of every startup path and public activation remain pending. The private local `Committed` outcome does not complete that external workflow.
The existing pin store synchronises the temporary file and atomically renames
it; it does not synchronise the containing directory. A destructive operation needs an explicit
commit result. An error after replacement is not proof that the old file is intact.

### Proposed outcomes

| Outcome | Operator meaning | Live protection | Restart |
|---|---|---|---|
| `NotCommitted` | Removal did not commit | Pin retained | Retain pin; recover any guard before listening |
| `IndeterminateRecoveryRequired` | Saving removal was not confirmed; do not report `removed=true` | Pin retained; further removals blocked | Restore the guarded pin before listening, or refuse startup |
| `Committed` (local) | Removal and recovery-record cleanup were durably confirmed; external finalization is pending | Only the selected identity pin is removed | Normal restart with the removal saved requires verified clean exit, sealed receipt and external operator finalization; a crash or nonzero exit before that requires recovery |
| `RecoveryRequired` | Cleanup and subsequent restoration could not be confirmed; do not retry automatically | Pin retained; networking stopped; further removals blocked | Disk outcome is unresolved; trusted recovery is required before restarting networking |

For the last outcome the UI must say: **"The pin remains active in this process. Its on-disk state
could not be confirmed. Recovery is required before restarting networking."** It must not describe
this as either a completed removal or a durably rolled-back failure. A newly proved v2 session can
pin the identity again in all cases under the normal rules.

### Transaction and concurrency

Keep one bounded recovery guard per store, created durably before changing the main store. It names
the operation, exact identity, previous pin timestamp and store identity. It never authorises
downgrade. At most one destructive operation can be pending.

The existing identity owner-map gains a runtime proof revision; every accepted proof, even for an
already pinned identity, changes it. A preview confirmation binds the store's runtime epoch, exact
identity, proof revision and consequence text version. A different identity's proof does not
invalidate it. Remove/re-pin cannot revive an old confirmation.

A mutation gate serialises removal with pin publication throughout the durable transaction.
Acquire it before `writeMu`, then acquire `mu` only for checks and snapshots. Never hold `mu` during
I/O or a network handshake. Recheck confirmation under that gate, write a copy omitting only this
identity pin, and apply the deletion in memory only after both the main commit and recovery-guard
cleanup are durably confirmed. New proofs
published after removal re-pin normally. Endpoint changes made during I/O must remain dirty; do not
advance the persisted change counter past changes the snapshot did not contain.

The main snapshot includes a bounded audit record for this operation. Normal local structured audit
logging is additional. Tokens appear only in the intended confirmation field and are never logged;
filesystem paths are excluded from RPC/UI payloads.

### Conservative recovery

Before opening listeners, a surviving valid guard restores its identity pin into the main snapshot,
preserving all current pins and endpoint bindings. Save that more protective snapshot durably and
remove the guard durably. Only then start networking. A damaged, foreign, unsupported or unreadable
guard, or failed recovery, refuses startup. Do not instruct operators simply to delete a guard: the
main file may already lack the pin. A fresh preview/confirmation can repeat removal after recovery.

If cleanup fails or is indeterminate, retain the runtime pin and attempt a durable rollback of the
main file to a snapshot including that pin. A confirmed rollback is an ordinary failed removal.
If rollback cannot be confirmed either, report `RecoveryRequired` rather than `NotCommitted`.

There is an unavoidable crash image: guard unlink became visible, directory sync failed, and the
rollback also failed. The next process may see the new main file without a guard. It cannot infer
the failed operation from those files alone. Automatic guard recovery therefore does not prove
that every reported failure retains the pin after restart. The owner explicitly accepts this
indeterminate boundary with `RecoveryRequired`, which stops networking and blocks further removals.
Before enabling removal RPC, implement an external fence preventing automatic networking restart
until confirmed recovery; another file on the same failing storage cannot guarantee it. Where no
such mechanism is ensured, removal is unavailable while ordinary node operation remains available.
Functional tests must cover both emergency outcomes and automatic restart. A different storage
format alone is insufficient: the guarantee must be checked under an explicit failure model.

### Platform and validation boundary

The persistence primitive must distinguish `NotCommitted`, `Committed` and `Indeterminate`; a
post-rename directory-sync error is `Indeterminate`. Unsupported durability must be rejected before
guard or main-file mutation. On Windows a verified backend is required: replacing the protected,
handle-relative write with an unchecked pathname call is not acceptable.

Functional tests must inject each failure point, then load every possible crash image in a new
store: old/new main with guard, new main without guard, cleanup unknown with guard present/absent,
damaged guard, failed recovery and a concurrent proof or periodic flush. Check that no write failure
silently enables legacy, endpoint bindings survive, stale confirmations fail and audit reports the
actual outcome. These are functional tests, not heavy measurements.

## Русский

### Статус

Проект транзакции для [решения владельца S6](identity-pin-removal-decision.md). Владелец утвердил
отдельный исход `RecoveryRequired` при условии конкретного внешнего запрета перезапуска и
подтверждённого восстановления до включения RPC снятия. Локальная транзакция и
[внешний supervised candidate](../operations/identity-pin-recovery-candidate.md) реализованы.
Linux stop/finalize workflow утверждён владельцем 2026-10-18; отдельный desktop-диалог реализован.
Deployment provisioning, проверка всех способов запуска и публичное включение ещё не выполнены. Приватный локальный `Committed` не завершает внешний workflow.
Нынешнее хранилище синхронизирует временный файл и
атомарно переименовывает его, но не синхронизирует каталог. Разрушающая операция требует явного
результата commit. Ошибка после замены не доказывает сохранность старого файла.

### Предлагаемые исходы

| Исход | Смысл для оператора | Защита работающего узла | Перезапуск |
|---|---|---|---|
| `NotCommitted` | Снятие не зафиксировано | Pin сохранён | Сохранить pin; восстановить guard до слушателей |
| `IndeterminateRecoveryRequired` | Сохранение снятия не подтверждено; `removed=true` не возвращать | Pin сохранён, новые снятия запрещены | Восстановить pin по guard до слушателей либо отказать в старте |
| `Committed` (локальный) | Снятие и очистка recovery-записи надёжно подтверждены; внешняя финализация ожидается | Удалён только указанный identity pin | Обычный перезапуск с сохранённым снятием требует проверенного штатного завершения, запечатанной квитанции и внешней финализации оператором; сбой или ненулевой код завершения до этого требуют восстановления |
| `RecoveryRequired` | Очистка и последующее восстановление не подтверждены; автоматически не повторять | Pin сохранён; сеть остановлена; новые снятия запрещены | Состояние диска неизвестно; до перезапуска сети нужно доверенное восстановление |

Для последнего исхода UI сообщает: **«Pin действует в этом процессе. Его состояние на диске не
подтверждено. До перезапуска сети требуется восстановление»**. Это нельзя называть ни завершённым
снятием, ни надёжно отменённой операцией. Новое доказательство v2 во всех случаях может заново
закрепить identity по обычным правилам.

### Транзакция и конкурентные операции

На хранилище — одна ограниченная recovery-запись, надёжно созданная до изменения основного файла.
В ней операция, точная identity, прежнее время pin и принадлежность хранилищу. Это не разрешение
downgrade. Одновременно незавершённой бывает не больше одной разрушающей операции.

Существующая map identity получает runtime-ревизию доказательства; каждое принятое доказательство,
в том числе уже закреплённой identity, меняет её. Подтверждение связывает runtime epoch хранилища,
точную identity, ревизию proof и версию предупреждения. Proof другой identity его не отменяет;
снятие и повторное закрепление старое подтверждение не оживляют.

Отдельный mutation gate сериализует снятие и публикацию pin на всём durable transaction. Он берётся
до `writeMu`, а `mu` — лишь для проверок и снимка; во время I/O и сетевого handshake `mu` не держат.
Под gate повторно проверяется подтверждение, записывается копия без одного identity pin, и лишь
после подтверждения durable commit основного файла и надёжной очистки guard pin удаляется из памяти.
Позднее proof закрепляет обычным
порядком. Изменения endpoint во время I/O остаются dirty; счётчик сохранённых изменений не перескакивает
через изменения, которых не было в снимке.

Основной снимок включает ограниченную запись аудита операции; локальный структурированный лог —
дополнительный аудит. Токены не логируются; токены и пути файлов не попадают в обычные RPC/UI данные
вне явно предназначенного для подтверждения поля токена.

### Консервативное восстановление

До открытия слушателей сохранившийся корректный guard восстанавливает pin своей identity в основном
снимке, сохраняя все текущие pins и endpoint-привязки. Более защищённый снимок и удаление guard
надёжно сохраняются; лишь затем разрешается сеть. Повреждённый, чужой, неизвестный или нечитаемый
guard либо ошибка recovery означают отказ старта. Инструкция просто удалить guard недопустима:
основной файл уже мог не содержать pin. После восстановления снятие повторяется новым preview/confirm.

При ошибке или неопределённом результате cleanup pin остаётся в памяти, а основной файл пытаются
надёжно восстановить с возвращённым pin. Подтверждённый rollback — обычное неудачное снятие.
Если и rollback не подтверждён, возвращается `RecoveryRequired`, а не `NotCommitted`.

Остаётся принципиальный crash-image: unlink guard уже виден, sync каталога завершился ошибкой,
rollback тоже не удался. Следующий процесс может увидеть новый основной файл без guard и не может
вывести из этих файлов факт неудавшейся операции. Поэтому автоматический recovery по guard не
доказывает сохранение pin после перезапуска при любой ошибке. Владелец явно принял эту границу
неопределённости: `RecoveryRequired` останавливает сеть и запрещает новые снятия. До включения RPC
нужно реализовать внешний запрет автоматического сетевого перезапуска до подтверждённого
восстановления; ещё один файл на том же отказавшем носителе этого не гарантирует. Без обеспеченного
механизма снятие недоступно, обычная работа узла доступна. Функциональные тесты проверяют аварийные
исходы и автоматический перезапуск. Замены формата хранения недостаточно: гарантия проверяется
в явно заданной модели отказов.

### Платформы и проверка

Primitive сохранения различает `NotCommitted`, `Committed` и `Indeterminate`; ошибка синхронизации
каталога после rename — `Indeterminate`. Неподдерживаемая надёжность отклоняется до изменения guard
или основного файла. Для Windows нужен проверенный backend; замена защищённых операций относительно
handle на непроверенные вызовы по пути недопустима.

Функциональные тесты вводят ошибку на каждом этапе и загружают все возможные crash-images новым
store: старый/новый main с guard, новый без guard, неизвестный cleanup с guard и без, повреждённый
guard, неуспешное recovery и конкурентное proof/periodic flush. Проверяются отсутствие молчаливого
допуска legacy после ошибки, сохранность endpoint-привязок, отказ старых подтверждений и точный
исход в аудите. Это функциональные проверки, а не тяжёлые измерения.
