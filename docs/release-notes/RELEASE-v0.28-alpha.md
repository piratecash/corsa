# What's New in v0.28-alpha

## Message Reply

The desktop client now supports message replies. Right-clicking a message opens a context menu with Reply and Copy options. Selecting Reply shows a preview banner in the composer with the quoted sender and text; pressing Escape or the close button cancels. Sent replies render as quoted bubbles above the message body — clicking the quote scrolls to the original message in the conversation. The `reply_to` field lives inside `PlainMessage` and is fully encrypted within the AES-GCM envelope, so neither the relay server nor the local SQLite can observe the reply graph without the decryption key. The RPC layer validates `reply_to` as a valid UUID before accepting a `send_message` command. This is the largest change in the release, touching the UI, RPC parser, domain types, and test coverage.

## Online Status and Contact Sorting

Each contact in the sidebar now displays a colored dot: green when at least one route exists in the routing table, gray when unreachable, and gray outline when reachability data is unavailable. The reachable set is built from `RoutingSnapshot()` on every probe cycle and covers all identities in the table — not just trusted contacts — so peers discovered via chatlog or DM headers also get correct status.

The sidebar applies a new 4-tier sort in the UI layer (`sidebar_sort.go`): online contacts with unread messages appear first, then online without unread, then offline with unread, then offline without unread. Within each tier, entries are sorted by unread count (descending) or last message timestamp (descending). The router provides the data (peers, unread counts, reachability), and the presentation layer decides display order. When reachability data is missing, sorting degrades gracefully to two tiers (unread first, then by timestamp). The sort runs on every frame render from the current `RouterSnapshot`, so any state change is reflected immediately.

## Self-Identity Route

Nodes can now always resolve their own address in the routing table. `Lookup()` and `Snapshot()` synthesize a local route entry (`Hops=0`, `Source=RouteSourceLocal`) when `localOrigin` is configured. The route is injected on read, never stored or announced to peers, and never expires. `UpdateRoute` rejects `RouteSourceLocal` with `ErrLocalSourceReserved` to prevent accidental persistence. Snapshot counters (`TotalEntries`, `ActiveEntries`) exclude the synthetic entry. Reachability helpers (`reachableFromSnapshot`, `reachableIDsFrame`, `fetch_route_summary`) also exclude it since reachability is about remote peers.

## Event-Driven Direct Routes

Direct route refresh no longer races with TTL expiry. Previously, a direct peer could briefly appear unreachable if the TTL-based eviction ran between keepalive updates. The fix extends `UpdateRoute()`: when a peer sends a new announce with the same `SeqNo` as the current entry, the route is accepted and the TTL is reset — even if the old entry has technically expired. This eliminates the gap where a perfectly healthy direct peer could momentarily lose its route. The routing documentation has been updated with the full trust hierarchy and direct-route refresh rules.

## Durable Deduplication

Message deduplication now closes two previously open paths. `AppendReportNew()` replaces the old `Append()` in the store pipeline and returns a boolean indicating whether the row was genuinely new (`RowsAffected > 0`). The node service checks this result on both the event path (incoming DM from network) and the header-extraction path (relay learning identity from message headers). Duplicate messages no longer emit `LocalChangeEvent`, trigger beep notifications, or increment unread counters. The chatlog `Store` API has been migrated to typed domain parameters (`domain.PeerIdentity`, `domain.MessageID`, `domain.ListenAddress`) for compile-time safety.

## Forward-Compatible Relay

Route announcements now preserve unknown JSON fields across re-announce. `AnnounceRouteFrame` gained a custom `UnmarshalJSON` / `MarshalJSON` pair that captures any field beyond the known set (`identity`, `origin`, `hops`, `seq`) into an `Extra` blob of `json.RawMessage`. When a node relays a route learned from a neighbor, the `Extra` blob is forwarded unchanged. This means future protocol extensions — such as onion box keys or capability flags — will survive transit through older nodes that do not yet understand them. `RouteEntry.Extra` travels through the routing table alongside the entry and is included in `Announceable()` and `AnnounceTo()` output.

## Compile-Time Identity Safety

The desktop UI layer and `DMRouter` have been migrated from raw `string` to `domain.PeerIdentity` throughout. Method signatures, map keys, event payloads, and RPC handlers now use the typed wrapper, catching identity/address mix-ups at compile time rather than at runtime.

---

# Что нового в v0.28-alpha

## Ответ на сообщение

Десктопный клиент теперь поддерживает ответы на сообщения. Правый клик по сообщению открывает контекстное меню с пунктами Reply и Copy. При выборе Reply в композере появляется баннер предпросмотра с цитируемым отправителем и текстом; Escape или кнопка закрытия отменяют ответ. Отправленные ответы отображаются как цитаты над телом сообщения — клик по цитате прокручивает к оригинальному сообщению в переписке. Поле `reply_to` находится внутри `PlainMessage` и полностью зашифровано в AES-GCM конверте, поэтому ни relay-сервер, ни локальный SQLite не могут наблюдать граф ответов без ключа дешифрования. RPC-слой валидирует `reply_to` как корректный UUID перед принятием команды `send_message`. Это самое крупное изменение в релизе, затрагивающее UI, RPC-парсер, доменные типы и тестовое покрытие.

## Онлайн-статус и сортировка контактов

Каждый контакт в sidebar теперь отображает цветную точку: зелёную при наличии хотя бы одного маршрута в таблице маршрутизации, серую при недоступности и серый контур при отсутствии данных о достижимости. Набор достижимых identity строится из `RoutingSnapshot()` при каждом цикле пробы и охватывает все identity в таблице — не только доверенные контакты — поэтому peers, обнаруженные через chatlog или DM headers, тоже получают корректный статус.

Sidebar применяет новую 4-уровневую сортировку в UI-слое (`sidebar_sort.go`): online-контакты с непрочитанными сообщениями идут первыми, затем online без непрочитанных, затем offline с непрочитанными, затем offline без непрочитанных. Внутри каждого уровня записи сортируются по количеству непрочитанных (по убыванию) или по времени последнего сообщения (по убыванию). Роутер предоставляет данные (peers, счётчики непрочитанных, достижимость), а слой представления определяет порядок отображения. При отсутствии данных о достижимости сортировка корректно деградирует до двух уровней (непрочитанные первыми, затем по timestamp). Сортировка выполняется на каждом кадре рендеринга из текущего `RouterSnapshot`, поэтому любое изменение состояния отражается немедленно.

## Self-identity маршрут

Ноды теперь всегда могут разрешить собственный адрес в таблице маршрутизации. `Lookup()` и `Snapshot()` синтезируют локальную запись маршрута (`Hops=0`, `Source=RouteSourceLocal`) при настроенном `localOrigin`. Маршрут инжектируется при чтении, никогда не сохраняется и не анонсируется пирам, и никогда не истекает. `UpdateRoute` отклоняет `RouteSourceLocal` с `ErrLocalSourceReserved` для предотвращения случайного сохранения. Счётчики snapshot (`TotalEntries`, `ActiveEntries`) исключают синтетическую запись. Хелперы достижимости (`reachableFromSnapshot`, `reachableIDsFrame`, `fetch_route_summary`) также исключают её, поскольку достижимость касается удалённых пиров.

## Event-driven прямые маршруты

Обновление прямого маршрута больше не гонится с истечением TTL. Ранее прямой пир мог кратковременно казаться недоступным, если TTL-based eviction срабатывал между keepalive-обновлениями. Исправление расширяет `UpdateRoute()`: когда пир отправляет новый announce с тем же `SeqNo`, что и текущая запись, маршрут принимается и TTL сбрасывается — даже если старая запись технически истекла. Это устраняет окно, в котором полностью здоровый прямой пир мог моментально потерять свой маршрут. Документация маршрутизации обновлена с полной иерархией доверия и правилами обновления прямых маршрутов.

## Устойчивая дедупликация

Дедупликация сообщений теперь закрывает два ранее открытых пути. `AppendReportNew()` заменяет старый `Append()` в pipeline хранения и возвращает boolean, указывающий, была ли строка действительно новой (`RowsAffected > 0`). Node service проверяет этот результат как на пути событий (входящее DM из сети), так и на пути извлечения заголовков (relay изучает identity из заголовков сообщений). Дублирующиеся сообщения больше не генерируют `LocalChangeEvent`, не воспроизводят звук уведомления и не увеличивают счётчик непрочитанных. API chatlog `Store` мигрирован на типизированные доменные параметры (`domain.PeerIdentity`, `domain.MessageID`, `domain.ListenAddress`) для compile-time безопасности.

## Forward-compatible relay

Анонсы маршрутов теперь сохраняют неизвестные JSON-поля при ре-анонсе. `AnnounceRouteFrame` получил кастомную пару `UnmarshalJSON` / `MarshalJSON`, которая захватывает любое поле за пределами известного набора (`identity`, `origin`, `hops`, `seq`) в `Extra` blob из `json.RawMessage`. Когда нода ретранслирует маршрут от соседа, `Extra` blob пересылается без изменений. Это означает, что будущие расширения протокола — такие как onion box keys или capability flags — переживут транзит через старые ноды, которые их ещё не понимают. `RouteEntry.Extra` проходит через таблицу маршрутизации вместе с записью и включается в вывод `Announceable()` и `AnnounceTo()`.

## Compile-time безопасность identity

Слой UI десктопа и `DMRouter` мигрированы с сырых `string` на `domain.PeerIdentity` повсеместно. Сигнатуры методов, ключи map, payload событий и RPC-обработчики теперь используют типизированную обёртку, ловя ошибки смешивания identity/address на этапе компиляции, а не в runtime.
