# Ordinary SDK global publication / Обычная публикация global через SDK

## English

`Runtime.PublishGlobalMessage(ctx, body)` and `publishGlobal` use
the same operation-guarded runtime → typed DesktopClient → own node publisher.
The RPC keeps its normal configured access/authentication policy; the loadstand
profile requires authenticated loopback access.
They canonicalize a nonempty UTF-8 body, mint a UUID, and use the node's actual
identity and clock. The envelope is global/broadcast, immutable, TTL0. This named
minimal policy reuses existing envelope behavior; propagation keeps its existing
broadcast age/hop/fanout and queue bounds. The API accepts no sender, ID, key,
signature, timestamp, flag or hop overrides. There is no raw-frame fallback.
Without the composed provider, the business RPC is unavailable.

Success means the sender's registered store committed the message locally.
The actual SDK has a DesktopClient and SQLite. `stored_locally` does not mean
that any peer received it. Missing gateways, a saturated dispatch queue or an
offline peer do not turn local persistence into a delivery receipt. Cancellation
before admission refuses the call; cancellation after a synchronous commit does
not discard its known ID/result. An SDK Close waits for admitted operations
before closing its database. A kernel/SQLite syscall cannot be forcibly cancelled.

Only the typed own publisher creates a private node origin value. Generic raw,
network and import paths cannot set it, including an unsigned frame claiming our
own sender. The origin allows the client's initial own global emission through
the ordinary admission/store/router/gossip pipeline. Client forwarding of foreign
globals, client ingress selection, subscriptions and full-only mesh targets are
unchanged. Existing bounded queues continue an already admitted attempt with
its same ID. No sender-owned global retry, automatic republish after restart,
offline catch-up or new delivery guarantee is added. Inferring trusted origin
from an old unsigned row's Sender would be unsafe and is not implemented.

Global sender claims are unsigned. The API's actual local identity is a fact
about that local call; receiving nodes cannot verify global authorship like DM
signatures/decryption. Global remains ordinary broadcast data and bypasses the
DM crypto path. No wire frame, signing scheme, capability or G2 activation is added.

The T5 functional profile sends from real SDK children and observes matching
replicas on FULL members using ordinary read-only `fetchMessage`. A headless
FULL's retained runtime copy is not labelled SQLite persistence. Correlation uses
actual minted ID, exact global topic, claimed sender, body digest, fixture nonce,
and observer identity/incarnation; duplicate polls never count twice. An exact
unsigned forgery is not cryptographically distinguishable. Missing replicas
remain missing. Observation state is bounded by existing `MaxActions`, including
the number of expected FULL observations, and existing trace/sample limits.
The plan v3 generator and SDK-only origin eligibility are unchanged. Unsupported
ports/old versions refuse before Start. The named light gossip fixture retains
the existing topology, seed, schedule and budgets; this is not resource acceptance
or full T5. Namespace orchestration, full process samples, T6/T7 and G2 remain open.

The existing `HoldDMUntilReachable` admission hold is now explicitly DM-class
(`dm` and `dm-control`) only. A global broadcast has no directed recipient and
must not be held for a missing DM route. This corrects the second normal-pipeline
gate exposed by the SDK's actual default configuration; unreachable signed DM
and control-message hold controls retain their previous behavior.

## Русский

`Runtime.PublishGlobalMessage(ctx, body)` и `publishGlobal`
идут через один operation-guarded runtime → typed DesktopClient → own node
publisher. Политика RPC access/authentication остаётся обычной конфигурируемой;
в профиле loadstand требуется аутентифицированный loopback. Тело UTF-8 канонизируется, UUID создаётся узлом; identity и время —
свои. Используется существующая политика global/broadcast, immutable, TTL0.
Broadcast age/hops/fanout и очереди сохраняют прежние пределы. API не принимает
sender, ID, ключи, подпись, timestamp, flag или hops. Без составленного provider
business RPC недоступен; raw-frame fallback отсутствует.

Успех означает локальный commit зарегистрированного хранилища отправителя;
настоящий SDK использует DesktopClient и SQLite. `stored_locally` не доказывает
приём пиром. Отсутствующий gateway, полная очередь и offline-пир не превращают
локальную запись в квитанцию доставки. Отмена до admission отказывает; после
синхронного commit известный ID/result сохраняется. Close SDK ждёт принятые
операции до закрытия БД. Зависший kernel/SQLite syscall принудительно не прерывается.

Закрытый origin создаёт только typed own publisher. Raw/network/import не могут
передать его, даже заявляя нашего Sender. Он разрешает начальную отправку своего
global через обычные admission/store/router/gossip. Пересылка чужого global
клиентом, выбор client ingress, подписки и full-only mesh targets не меняются.
Ограниченная очередь продолжает уже принятое действие с прежним ID. Не добавлены
global retry engine, автопубликация после restart, offline catch-up или гарантия
доставки. Происхождение не восстанавливается по неподписанному Sender старой строки.

Global содержит неподписанный claimed sender. Своя identity достоверна для
локального API-вызова, но получатель не получает DM-level доказательства
авторства или дешифровки. Новый wire-формат, подпись, capability и G2 не вводятся.

T5-функциональный профиль публикует из настоящих SDK-child, а FULL наблюдает
реплику обычным read-only `fetchMessage`. Копия headless FULL в runtime не
называется SQLite commit. Сопоставляются actual UUID, topic, claimed sender,
body digest, nonce прогона и identity/incarnation наблюдателя; повторный poll
не увеличивает счётчик. Идеальная подделка unsigned sender криптографически
неотличима. Ненаблюдаемая реплика не объявляется доставленной. Число ожидаемых
FULL-наблюдений ограничено существующим `MaxActions`, trace/sample bounds прежние.
Генератор v3 и SDK-only origins неизменны, старые версии/нет нужных ports — отказ
до Start. Именованная лёгкая фикстура сохраняет topology, seed, schedule и бюджеты.
Это не ресурсная приёмка и не завершение T5; namespace, полные process samples,
T6/T7 и G2 остаются открытыми.

`HoldDMUntilReachable` явно ограничен DM-классом (`dm` и `dm-control`). У global
broadcast нет directed recipient, поэтому отсутствующий DM-маршрут не должен
блокировать его отправку. Это второй gate обычного pipeline, обнаруженный на
настоящей конфигурации SDK; прежний hold для недоступных подписанных DM/control
сохраняется и проверяется отдельными controls.
