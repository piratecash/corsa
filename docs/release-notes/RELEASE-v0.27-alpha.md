# What's New in v0.27-alpha

## Identity Deletion Done Right

Deleting a contact from the desktop sidebar now actually cleans up everything. The trust store removes the identity from disk, not just from memory. The sidebar is built exclusively from the router's in-memory peer list instead of polling, so stale entries no longer linger after deletion. If the node is temporarily unreachable, deletion proceeds anyway — it's best-effort rather than blocking. A new `delete_trusted_contact` RPC command exposes the same operation from the console. Deletion errors are propagated to the UI with localized status messages instead of silently failing.

Internally, a new `domain.ListenAddress` type prevents accidental mixing with `PeerAddress`, and `domain.PeerIdentity` is now enforced in chatlog constructors and node RPC handlers.

## Event-Driven Pending Queue Drain

The pending message queue no longer relies solely on periodic retry loops. Three new triggers flush queued messages immediately when conditions change: TTL expiry with a surviving backup route, peer disconnect that promotes a backup path, and reconnection of an unchanged route. The queue state is now persisted atomically, so an unexpected shutdown doesn't lose queued messages. This is the largest change in the release — roughly 2,600 new lines of code and tests across the routing integration layer.

## Origin Filtering in Route Announcements

A subtle routing bug caused peers to send routes back to the node that originated them. When node B's routes traveled through the network (B → C → A) and A announced them back to B, every such route was rejected as a "forged own origin" — flooding the logs with warnings. The fix adds origin filtering in `AnnounceTo` and `Announceable`: routes where `Origin == peer` are no longer sent to that peer. The receiver-side rejection log has been lowered from Warn to Debug since it's now a rare edge case rather than a constant occurrence.

## Peer Identity in get_peers

The `get_peers` console response now includes the `identity` field for each connected peer. Previously, only the transport address was shown, making it impossible to correlate connected peers with entries in the routing table. The root cause was that `addPeerID` was only called through `learnIdentityFromWelcome`, which skipped peers without a `listen` field — typically desktop clients. The identity is now stored by the health-tracking key for all outbound sessions, regardless of whether the peer advertises a listener.

## Quieter Logs

Blacklist rejection messages (`reject connection: blacklisted`) have been lowered from Warn to Debug. These are routine events during normal operation and were adding noise to production logs.

---

# Что нового в v0.27-alpha

## Корректное удаление identity

Удаление контакта из боковой панели десктопа теперь полностью очищает все данные. Trust store удаляет identity с диска, а не только из памяти. Боковая панель строится исключительно из текущего списка пиров роутера вместо поллинга, поэтому устаревшие записи больше не остаются после удаления. Если нода временно недоступна, удаление всё равно выполняется — оно работает по принципу best-effort, а не блокирует. Новая RPC-команда `delete_trusted_contact` предоставляет ту же операцию из консоли. Ошибки удаления передаются в UI с локализованными сообщениями вместо молчаливого игнорирования.

Внутренне добавлен новый тип `domain.ListenAddress`, предотвращающий случайное смешивание с `PeerAddress`, а `domain.PeerIdentity` теперь обязателен в конструкторах chatlog и обработчиках RPC.

## Event-driven доставка отложенных сообщений

Очередь отложенных сообщений больше не полагается только на периодические циклы повтора. Три новых триггера немедленно сбрасывают очередь при изменении условий: истечение TTL при наличии резервного маршрута, отключение пира с переключением на backup-путь и переподключение с неизменённым маршрутом. Состояние очереди теперь сохраняется атомарно, поэтому неожиданное завершение процесса не приводит к потере сообщений. Это самое крупное изменение в релизе — порядка 2600 новых строк кода и тестов в слое интеграции маршрутизации.

## Фильтрация маршрутов по Origin в анонсах

Баг маршрутизации приводил к тому, что пиры отправляли маршруты обратно узлу, который их создал. Когда маршруты узла B проходили через сеть (B → C → A) и A анонсировал их обратно B, каждый такой маршрут отклонялся как «поддельный own origin» — засоряя логи предупреждениями. Исправление добавляет фильтрацию по origin в `AnnounceTo` и `Announceable`: маршруты с `Origin == peer` больше не отправляются этому пиру. Уровень логирования отклонения на стороне получателя понижен с Warn до Debug, поскольку теперь это редкий edge case, а не постоянное явление.

## Identity пиров в get_peers

Ответ консольной команды `get_peers` теперь включает поле `identity` для каждого подключённого пира. Ранее показывался только транспортный адрес, что делало невозможным сопоставление подключённых пиров с записями в таблице маршрутизации. Причина была в том, что `addPeerID` вызывался только через `learnIdentityFromWelcome`, которая пропускала пиров без поля `listen` — как правило, десктопных клиентов. Теперь identity сохраняется по ключу health-tracking для всех исходящих сессий, независимо от того, объявляет ли пир listener.

## Тише логи

Сообщения об отклонении заблокированных адресов (`reject connection: blacklisted`) понижены с Warn до Debug. Это штатные события при нормальной работе, которые добавляли шум в продакшен-логи.
