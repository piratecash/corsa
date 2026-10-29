# What's New in v2.5.71

This is an interim release before completion of S6, under the version-specific
publication policy described in [Releases](../release.md#interim-2571-publication--2026-10-30).
Independent cryptographic review and final candidate latency, load, resource and
observation acceptance remain incomplete. TLSv2/protocol 31 and storage schema 8
are active; this release does not claim completed S6, D1/G2 or a full-load 512 MB
memory guarantee. Native Windows/DACL and Tor measurements remain UNVERIFIED.

Это промежуточный выпуск до завершения S6. Независимая криптоэкспертиза,
итоговая приёмка задержек, нагрузки, ресурсов и наблюдения ещё не завершены.
TLSv2/protocol 31 и storage schema 8 активны; завершение S6, D1/G2 и пригодность
для полной нагрузки в 512 МБ не заявляются. Windows/DACL и Tor — UNVERIFIED.

## Peer Connections Use Secure Session v2

Nodes running protocol version 31 can now establish secure session v2 connections over TLS 1.3. The peer's identity proof is tied to the exact encrypted connection, and Corsa remembers verified identities and endpoints across restarts so that a proven v2 peer cannot silently return to the legacy session later.

TLS-first connections are enabled by default. Supported older peers can still use the legacy session after a strictly validated legacy handshake, but a failed TLS attempt alone is never treated as permission to downgrade. The minimum supported protocol version is now 28, and signing keys receive stricter validation when they enter the node.

To switch TLS-first off temporarily for peers that have never proved v2, set `CORSA_V2_TLS_FIRST=0` for the node. The SDK uses its `NodeConfig.V2TLSFirst` field rather than reading that environment variable. This does not disable the v2 listener, remove saved pins or permit legacy for a known pinned identity or bound endpoint. Where legacy is still allowed, an on-path attacker can hold peers that have never proved v2 in legacy at first contact; this release does not claim that attack closed for the whole network.

The saved v2 protection is part of the node's identity state. If its pin store or marker is missing, unreadable or kept in an unsafe location, Corsa refuses to start and explains the problem instead of discarding that protection or silently falling back.

## Messages Are Read When They Are Seen

Opening or selecting a conversation no longer marks every unseen message as read. If a new message arrives while the reader is scrolled further up, Corsa leaves the view in place, keeps the message unread and does not send a read receipt until the message is actually visible.

An unread divider marks where unseen messages begin, and an open conversation keeps its unread badge until the reader reaches them. This prevents incoming messages from pulling the conversation to the bottom while someone is reading earlier history.

## Local Data Can Stay Inside an Encrypted Container

The new `CORSA_DATA_DIR` setting moves all node-local files, including the identity, message database, contacts, pins and received files, into one existing directory. This makes it practical to keep the complete Corsa state inside an encrypted container instead of moving individual files separately.

When a configured directory is missing or a link into it is broken, Corsa refuses to start rather than creating a new identity on the unencrypted disk. Pointing `CORSA_DATA_DIR` at a subdirectory inside the encrypted mount also makes an unmounted container detectable.

## Delivery and Connection State Are More Reliable

Delivery receipts, cancellations and failed sends are now matched by the message ID together with its sender and recipient. Unrelated traffic that happens to carry the same outer ID can no longer settle, replace or remove another message's delivery state. Relay retry records are committed and removed together with the message or receipt they belong to, avoiding detached retry state under concurrent delivery and cleanup.

Delete and conversation-wipe retries now compare their full UTC nanosecond timestamps correctly. Storage schema 8 adds indexes for due work and reconnect revival without rewriting the existing task rows or timestamps.

Connection management now separates local policy decisions from remote failures. A local pin refusal no longer lowers a healthy peer's score, unauthenticated sockets cannot reserve a host, inbound connections do not consume the node's outbound subnet diversity, and a handshake response can wait for bounded send-queue space instead of failing immediately.

## Long-Running Nodes Keep Less Stale State

Route-health records, desktop identity bookkeeping and rate-limiter entries now follow the active working set instead of growing with everything the node has seen since startup. Connection resources are closed during shutdown, admitted background work is joined, and no new session work starts after the node has stopped.

Traffic totals now remain cumulative when sessions close or peers are evicted, and diagnostics distinguish secure v2 sessions from legacy v1 sessions. The toolchain has also moved to Go 1.27.3 with refreshed runtime and build dependencies.

## DHT Preparation

This release contains a substantial amount of groundwork for the future DHT, but it does not enable DHT routing or anonymous transport. Current message delivery continues to use the existing routing path.

The overlay simulator now measures lookup success, graph connectivity, bucket behaviour and competing connection policies across reproducible network shapes. These experiments make it possible to reject unsuitable policies before they reach the running node and have already identified large-topology cases that still need further design work.

The codebase also gains inactive D1 foundations: bounded `FIND_NODE` handling, relation-record admission, pin lifecycles, readiness composition, observation sources, resource diagnostics and Linux load-test infrastructure. These paths are present for testing and measurement but remain disabled in normal constructors, configuration and RPC. They do not change the behaviour of the released node.

## Before Upgrading

Keep a coherent backup of the identity file, `secure-sessions-<port>.json`, its marker and the state database. When moving a node or changing its data directory, move the identity and pin state together; deleting trust state to bypass a startup refusal removes the protection learned from earlier secure sessions.

After storage schema 8 is committed, an older binary whose migration catalog ends at schema 7 cannot open that database. An in-place binary rollback is therefore unsupported. Create a verified, SQLite-consistent backup after an orderly shutdown; copying only a live `.db` file without its WAL is not a safe backup.

Any rollback build must support secure session v2 and the database schema it is asked to open, including schema 8 after that migration. A build ending at schema 7 requires an explicitly verified, SQLite-consistent pre-upgrade backup and recovery procedure; never delete pins or lower the session protection to make an older binary start. Native Windows runtime/DACL checks and real Tor measurements remain DEFERRED / UNVERIFIED, not passed; this does not remove the applicable Windows build obligations or proxy and authentication protections.

The Docker image runs as UID 10001 and keeps its persistent data in `/home/corsa/.corsacore`. Mount the volume at that location and make it private to the container user rather than using a world-writable directory.

---

# Что нового в v2.5.71

## Соединения между нодами используют secure session v2

Ноды с protocol version 31 теперь могут устанавливать secure session v2 поверх TLS 1.3. Доказательство identity привязано к конкретному зашифрованному соединению, а проверенные identities и endpoints сохраняются между перезапусками. После успешной v2-проверки peer не может незаметно вернуться к legacy-сессии.

TLS-first включён по умолчанию. Поддерживаемые старые peers всё ещё могут использовать legacy-сессию после строго проверенного legacy handshake, но одного сбоя TLS недостаточно для downgrade. Минимальная поддерживаемая версия протокола повышена до 28, а signing keys проходят более строгую проверку при поступлении в ноду.

Чтобы временно отключить TLS-first для peers, которые ещё не доказали v2, задайте ноде `CORSA_V2_TLS_FIRST=0`. SDK использует поле `NodeConfig.V2TLSFirst`, а не эту переменную окружения. Это не отключает v2 listener, не удаляет сохранённые pins и не разрешает legacy для известной pinned identity или привязанного endpoint. Там, где legacy ещё разрешён, on-path атакующий может удерживать peers, ещё не доказавших v2, в legacy при первом контакте; релиз не объявляет эту атаку закрытой для всей сети.

Сохранённая v2-защита является частью состояния identity ноды. Если pin store или его marker отсутствует, не читается или хранится в небезопасном месте, Corsa объясняет проблему и отказывается стартовать вместо удаления защиты или незаметного fallback.

## Сообщения считаются прочитанными, когда их увидели

Открытие или выбор переписки больше не помечает прочитанными все невидимые сообщения. Если новое сообщение приходит, пока пользователь читает историю выше, Corsa сохраняет текущую позицию, оставляет сообщение непрочитанным и не отправляет read receipt, пока сообщение действительно не появится на экране.

Разделитель непрочитанных показывает начало новых сообщений, а badge открытой переписки сохраняется, пока пользователь до них не дойдёт. Входящие сообщения больше не перетягивают диалог вниз во время чтения старой истории.

## Локальные данные можно хранить внутри зашифрованного контейнера

Новая настройка `CORSA_DATA_DIR` переносит все локальные файлы ноды — identity, базу сообщений, контакты, pins и полученные файлы — в один существующий каталог. Благодаря этому полное состояние Corsa можно хранить в зашифрованном контейнере, не настраивая отдельные пути для каждого файла.

Если указанный каталог отсутствует или ведущая в него ссылка недоступна, Corsa отказывается стартовать вместо создания новой identity на незашифрованном диске. Если указать в `CORSA_DATA_DIR` подкаталог внутри зашифрованного тома, приложение также сможет определить, что контейнер не подключён.

## Доставка и соединения стали надёжнее

Подтверждения доставки, отмена и неудачная отправка теперь сопоставляются не только по message ID, но и по отправителю и получателю. Посторонний трафик с совпавшим внешним ID больше не может завершить, заменить или удалить состояние доставки другого сообщения. Записи relay retry создаются и удаляются вместе со своим сообщением или подтверждением, поэтому параллельная доставка и очистка не оставляют отделённое retry-состояние.

Повторные попытки удаления сообщений и очистки переписки теперь корректно сравнивают полные UTC timestamps с наносекундами. Storage schema 8 добавляет индексы для выбора готовых задач и их пробуждения после возвращения peer, не переписывая существующие задачи или timestamps.

Connection manager теперь отличает локальные решения от ошибок удалённой ноды. Локальный отказ из-за pin не снижает score исправного peer, неаутентифицированное соединение не резервирует host, входящие соединения не уменьшают разнообразие исходящих подсетей, а ответ handshake может ограниченное время дождаться места в очереди отправки вместо мгновенного отказа.

## Долгоживущая нода удерживает меньше устаревшего состояния

Route-health, desktop-кеш identities и записи rate limiters теперь следуют за активным рабочим набором, а не растут вместе со всем, что нода видела с момента запуска. Ресурсы соединений закрываются при shutdown, принятые фоновые задачи завершаются, а новая session-работа после остановки ноды больше не запускается.

Traffic totals сохраняют накопленные значения после закрытия сессий и вытеснения peers, а диагностика отдельно показывает secure v2 и legacy v1 sessions. Toolchain также обновлён до Go 1.27.3 вместе с runtime- и build-зависимостями.

## Подготовка к DHT

В этот релиз вошёл большой объём подготовительных работ для будущего DHT, но DHT-маршрутизация и анонимный транспорт пока не включены. Доставка сообщений продолжает использовать существующий маршрут.

Симулятор overlay теперь измеряет успешность поиска, связность графа, поведение buckets и разные правила распределения соединений на воспроизводимых моделях сети. Это позволяет отбрасывать неподходящие варианты до их появления в работающей ноде. Проверки уже выявили крупные топологии, для которых правила ещё требуют доработки.

В код также добавлена неактивная основа D1: ограниченная обработка `FIND_NODE`, приём relation records, жизненный цикл pins, сборка состояния готовности, источники наблюдений, диагностика ресурсов и инфраструктура нагрузочных тестов Linux. Эти пути доступны для тестирования и измерений, но выключены в обычных constructors, config и RPC. Они не меняют поведение выпускаемой ноды.

## Перед обновлением

Сохраните согласованную резервную копию identity, файла `secure-sessions-<port>.json`, его marker и state database. При переносе ноды или смене каталога данных перемещайте identity и pin state вместе. Удаление trust state ради обхода отказа запуска удаляет защиту, накопленную предыдущими secure sessions.

После применения storage schema 8 старый binary, чей каталог миграций заканчивается на schema 7, не сможет открыть эту базу. Поэтому in-place rollback binary не поддерживается. Сделайте проверенную SQLite-consistent копию после корректного shutdown; копирование только работающего `.db` без WAL не является безопасным backup.

Binary для отката должен поддерживать secure session v2 и схему открываемой базы, включая schema 8 после этой миграции. Для binary с каталогом до schema 7 требуется явно проверенная SQLite-consistent резервная копия до обновления и процедура восстановления; нельзя удалять pins или ослаблять session-защиту ради запуска старого binary. Настоящие Windows runtime/DACL проверки и Tor-замеры остаются DEFERRED / UNVERIFIED, не пройденными; это не отменяет применимые Windows builds, защиту proxy и аутентификацию.

Docker image работает от UID 10001 и хранит постоянные данные в `/home/corsa/.corsacore`. Монтируйте volume в этот каталог и ограничьте доступ пользователем контейнера вместо использования world-writable директории.
