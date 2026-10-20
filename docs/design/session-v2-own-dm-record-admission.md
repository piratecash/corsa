# Inactive own-DM record admission / Неактивный допуск записей own-DM

## English

One unused INTERNAL constructor `NewInactiveOwnDMAdmissionCandidate` creates a new
Service, its owner-bound port and the existing charged R admission owner together.
It requires an explicit finite `OwnDMResidenceProfile`; there is no deployment
value/default, config/RPC/wire flag, existing-Service getter or late attachment.
Normal node/Desktop/DMCrypto/SDK/raw/import/reseed constructors remain inactive.
The separate provenance-only constructor still stages no R records.

The lifetime starts at entry to node candidate Submit, before that call's
prepare/verification/storage. Earlier service key lookup/encryption and the
start of the user's action are outside this boundary. The private existing
observed-monotonic clock supplies the original anchor and exact actual-Run
started token. An original pre-Run Submit remains NoAdmission even when storage
returns after Run starts. A required copied finite profile, checked overflow,
original context Done, message TTL, exact delivery cancellation and shutdown
bound PRESTAGE authority. The interval is half-open; ACK, retry, queue residence,
new records and redraw do not refresh it. Tests supply proposal durations only;
numeric deployment/G2 approval and measurement remain separate.

The genuine typed encrypted message follows ordinary storage, actual CM Ready,
Route and the common first directed item. Only its actual successful socket
Write closes the retained ACK. The Submit tail reserves X then distinct Y in
the SAME fixed256 table; pending ACK, verification, Processing and earned work
use the same table as R1b. The table type also hosts the separate immediate-K
variant; this construction enables charged R only and does not activate combined
K work. One free slot permits X only; a full table refuses
without eviction/debit. X==Y uses one slot/record; a new allocation uses
OutgoingSend. Distinct roles admit/confirm independently, without an atomic pair
promise. They share the existing charged R FIFO, aggregate30/60 rolling rate
and startup60 observed seconds. The one existing paced joined worker polls ACK
nonblockingly; there is no new queue/map/worker/waiter/timer/dial.

X's original pin observation is captured before storage; Y's exact CM proof,
ConnID/session and original pin binding precede its readiness waits. Settlement
consumes the genuine original claim once, transferring only reserved refs. Raw
frames, imported keys, incoming DM and diagnostics cannot mint this authority.
The already available exact-network signed record is copied into fixed bounded
storage and genuinely verified with min_session>=2 outside owner locks. Missing,
foreign, min1 or changed records refuse only optional admission; no lookup/dial
or fake Window TargetProof is added. Existing unsolicited receiver policy is
not bypassed or declared complete by this local-use path.

Final stage locks gateR -> evidenceR -> deliveryMu -> resolver.mu -> trust.muR
-> store.mu and rechecks original Run/deadline/cancel/claim/owners, exact Body AND
Sig/sequence/min2 and shared rate/quotas. Both typed validators enter common
allocation/revision/C3 mechanics. X and Y get OutgoingSend and ProvenNextHop;
already eligible R origin is preserved. The original absent-X observation is
not refreshed after ACK: its own stage publishes only the exact same-claim
earned binding for sibling validation. C3 delete/readd cannot reuse it. That
binding certifies in-memory protection, never strict durability/confirmation.

Stage, owed binding and debit commit without a fallible gap. Existing strict
snapshot ACK and legacy drain/purge complete the obligation. Unknown/failed
writes do not refund or confirm it. Deadline/cancel after stage cannot erase
protection; actual backend return AND Close precede physical slot release.
Shutdown joins the existing worker and keeps any stopped earned history in that
closed Service only; restart does not replay runtime claim/queue authority.
Ordinary successful storage/DirectMessage echo stays true if optional provenance
or admission fails. No activation, resource/latency acceptance or D1 release is
implied. Fixed per-owner pointers/arrays exclude ciphertext/channel backing and
allocator costs; existing delivery/outbox populations are not globally bounded.

Node functional sources cover real Run/CM/socket ACK/strict positive using a
private pre-Run clock fixture. Service sources independently cover real typed
DMCrypto encryption, SQLite, CM/writer provenance and optional-refusal echo.
This slice does not claim a combined successful service R stage: ordinary
import_contacts establishes K and there is no public business/setup API here
for injecting a non-K signed min2 record or private clock. No public test bridge
is added. An actual60-second startup wait alone would not supply that missing
record input. Full integration and finite execution remain separately checked.

## Русский

Неиспользуемый INTERNAL-конструктор одновременно создаёт новый Service, opaque
port и прежний общий owner R. Конечный профиль времени обязателен; значения для
выпуска и default нет. Обычные конструкторы, config/RPC/wire, импорт и outbox
не получают origin. Прежний provenance-only кандидат R не ставит.

Anchor фиксируется на входе в node Submit до его prepare/verify/store. Более
ранние lookup/encryption сервиса и начало действия пользователя не входят в эту
границу. Используются исходные monotonic sample и token фактического Run. Вызов
Submit до Run не получает admission после ожидания хранения. Конечный скопированный
профиль, overflow-проверки, исходные Done/TTL, точная отмена delivery и shutdown
ограничивают право ДО stage. Ровно на deadline право истекло; ACK, retry, очередь
и новый record срок не продлевают. Числа тестов — предложения, не политика G2.

Настоящие CM Ready, Route и первый item остаются в пути; ACK закрывает только
успешный socket Write. X и отдельный Y занимают прежние общие 256 слотов вместе
с pending ACK, Processing и earned work. При одном свободном месте допускается
X, при полном owner — отказ без вытеснения/дебета. X==Y учитывается один раз.
Общий FIFO класса R, лимит30/60 и startup60 сохранены. Используется один прежний
paced/joined worker без новой очереди, map, waiter, таймера или dial.

Исходные X observation и Y CM proof/binding не снимаются заново после ожиданий.
Только уже имеющийся signed record точной сети/identity с min_session>=2 проходит
настоящую проверку вне locks. TargetProof Window не подделывается. Перед stage
проверяются исходный claim/Run/deadline/отмена, owners, текущие Body И Sig/seq,
лимиты20k/R8192 и общий rate. Для разных X/Y допускается независимый результат;
атомарная пара не обещается. Собственный stage отсутствовавшего X оставляет
точный earned binding того же claim для Y; C3 delete/readd его не возрождает.
Это факт защиты в памяти, не подтверждение записи.

Прежний strict ACK и drain/purge завершают earned обязательство. Unknown/ошибка
не возвращают дебет и не дают подтверждения. Отмена/срок после stage не снимают
защиту; слот освобождается только после реальных return и Close. Очередь не
восстанавливается после рестарта. Успешные обычные storage/echo остаются правдивыми.
Активация, численный профиль G2, ресурсная приёмка и выпуск D1 отдельно.

Node-тесты содержат положительный путь настоящих Run/CM/ACK/strict с private
clock до Run. Service-тесты отдельно проверяют настоящие DMCrypto/SQLite/CM и
правдивый echo при отказе. Полный положительный service R stage здесь не заявлен:
import_contacts создаёт K, а публичного business/setup API для non-K signed min2
record или private clock нет. Новый test bridge не добавляется.
