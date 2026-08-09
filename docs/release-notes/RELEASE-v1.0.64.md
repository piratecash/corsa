# What's New in v1.0.64

## Corsa Comes to Android

This release introduces the first Android client, bringing the existing Corsa chat experience to Android 8.0 and newer as a light node. The UI switches to a single-pane layout on phone-sized screens, supports both the system Back action and an in-app back button, and keeps the aggregate network status visible in the compact contacts view.

Android file handling is integrated with the system picker: attachments can be streamed into a conversation and received files can be exported through the Storage Access Framework. Identity keys, chat history and other local state live in Android's no-backup storage, while Gradle packaging provides adaptive icons and signed APK/AAB build paths.

This first mobile version is foreground-only: the node receives messages while the app is open. Reliable delivery while the app is backgrounded or closed remains a separate roadmap item.

## First-Contact Messages Can Authenticate Themselves

Direct-message transport frames now carry the origin sender's public key material. Because a Corsa address is derived from its signing key, the recipient can verify an unknown sender directly from the signed envelope and import the keys only after that proof succeeds.

This fixes the loop where a first message could remain stuck with `unknown-sender-key`. The fallback contact-sync path was also made reply-aware, bounded and asynchronous, so mixed-version routes can recover without mistaking inbox replay or routing traffic for the requested response.

## A Stateless Datagram Plane Lays the Next Transport Foundation

Corsa now includes `mesh_datagram_v1`: one capability-negotiated transport for small typed protocol exchanges that should not need a new wire command and a separate relay implementation every time. It supports signed routed frames plus request/response flows, with route selection, hop limits and explicit endpoint type negotiation.

The plane is deliberately stateless at relays. Admission budgets, anti-replay protection, weighted control/bulk queues and socket write deadlines bound the work a neighbour can impose, while retry and durability remain the responsibility of the two endpoints. Operators can inspect the layer through `fetchDatagramSummary`, `datagramReachable` and `explainDatagramRoute`, or disable it with `CORSA_ENABLE_DATAGRAM_V1=0`.

No existing DM or file-transfer path is moved onto datagrams in this release. The substrate ships first so routing, limits and observability can settle before application protocols begin using it.

## Mobile Polish and Runtime Foundations Improve

The compact contacts screen now keeps the connection-count badge visible even when the desktop two-pane composer is absent, making network state easier to understand on phones and narrow windows.

The build foundation has also been refreshed to Go 1.26.5 alongside current Gio, Fiber, SQLite and supporting dependencies, keeping the new mobile and transport work on an up-to-date runtime stack.

---

# Что нового в v1.0.64

## Corsa появилась на Android

В этом релизе появился первый Android-клиент, который переносит существующий chat experience Corsa на Android 8.0 и новее в роли light node. На экранах телефонного размера UI переключается в single-pane layout, поддерживает системное действие Back и кнопку возврата внутри приложения, а aggregate network status остаётся видимым в компактном списке контактов.

Работа с файлами интегрирована с системным picker: вложения можно потоково добавить в диалог, а полученные файлы — экспортировать через Storage Access Framework. Identity keys, история чатов и остальное локальное состояние хранятся в Android no-backup storage, а Gradle packaging поддерживает adaptive icons и сборку подписанных APK/AAB.

Первая мобильная версия пока работает только на переднем плане: node получает сообщения, когда приложение открыто. Надёжная доставка в background и после закрытия приложения остаётся отдельной задачей roadmap.

## First-contact сообщения теперь аутентифицируют себя сами

Direct-message transport frames теперь несут публичные ключи исходного отправителя. Поскольку адрес Corsa выводится из signing key, получатель может проверить неизвестного отправителя прямо по подписи envelope и импортировать ключи только после успешного доказательства.

Это исправляет цикл, из-за которого первое сообщение могло навсегда застрять с `unknown-sender-key`. Fallback contact-sync path также стал reply-aware, ограниченным и асинхронным, поэтому mixed-version маршрут может восстановиться, не принимая inbox replay или routing traffic за запрошенный ответ.

## Stateless datagram plane закладывает новый транспортный фундамент

В Corsa появился `mesh_datagram_v1` — единый capability-negotiated транспорт для небольших типизированных protocol exchanges, которым больше не нужно каждый раз заводить отдельную wire command и заново реализовывать relay path. Он поддерживает signed routed frames и request/response flows с выбором маршрута, ограничением hops и явным согласованием типов на endpoint.

На relay этот plane намеренно не хранит состояние. Admission budgets, anti-replay защита, weighted control/bulk queues и socket write deadlines ограничивают работу, которую сосед может навязать ноде, а retry и durability остаются ответственностью двух endpoints. Состояние слоя можно наблюдать через `fetchDatagramSummary`, `datagramReachable` и `explainDatagramRoute` или полностью отключить через `CORSA_ENABLE_DATAGRAM_V1=0`.

Существующие DM и file-transfer paths в этом релизе на datagrams ещё не переведены. Сначала выкатывается сам substrate, чтобы routing, limits и observability успели стабилизироваться до появления прикладных протоколов поверх него.

## Mobile polish и runtime foundation стали лучше

В компактном списке контактов connection-count badge теперь остаётся видимым даже без desktop two-pane composer, поэтому состояние сети проще понять на телефоне или в узком окне.

Build foundation также обновлён до Go 1.26.5 вместе с актуальными Gio, Fiber, SQLite и supporting dependencies, чтобы новая мобильная и transport функциональность работала на свежем runtime stack.
