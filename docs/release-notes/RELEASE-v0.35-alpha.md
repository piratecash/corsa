# What's New in v0.35-alpha

## NetCore Migration Consolidated Into a Single Network Boundary

A large part of this release is one architectural change delivered across many commits: the ongoing NetCore migration. NetCore has now been extracted into its own package, connection handling has moved further toward `ConnID`-first APIs, and more inbound and outbound paths now go through a shared `Network` boundary instead of ad hoc socket access.

In practice, this makes connection lifecycle management more coherent, reduces leakage of low-level connection state across the node layer, and gives routing, dispatch, auth, ping, and session teardown a safer and more uniform execution model.

## Built-In Peer Traffic Capture

Corsa can now record live peer wire traffic without relying on external sniffers. The new built-in capture flow supports recording by connection ID, by remote IP, or globally, and it exposes start/stop controls through RPC and the desktop console.

Capture state is also surfaced back into diagnostics and peer views, which makes it much easier to inspect malformed traffic, unexpected protocol behavior, or difficult-to-reproduce peer issues directly from the node itself.

## Peer Upgrade Detection and Incompatible-Peer Lockout

This release adds a node-owned version policy that watches peer version signals, detects when the network is effectively telling you to upgrade, and applies durable lockout behavior for confirmed incompatible peers.

That improves operator visibility into version-related connectivity problems and helps the node avoid repeatedly cycling through peers that are already known to be incompatible. As part of the same work, relay delivery receipts were kept compatible so legitimate transit receipts from newer peers are forwarded instead of being misclassified.

## Leaner Route Announcement Traffic

Route announcement handling has been tightened with canonical per-peer announce snapshots, delta computation, and explicit per-peer announce state tracking. The goal is to reduce redundant announce traffic while keeping resync behavior predictable after reconnects and topology changes.

This makes route propagation less noisy and gives the routing layer a clearer foundation for future incremental announce modes without changing the core correctness rules around withdrawals and origin ownership.

## Safer RPC Startup Defaults

The RPC server no longer starts when authentication credentials are not configured. Instead of exposing or competing for an RPC port in partially configured environments, the node now keeps RPC disabled until both username and password are explicitly set.

This is a safer default for local and multi-instance setups and reduces accidental exposure of an unauthenticated control plane.

---

# Что нового в v0.35-alpha

## NetCore migration сведена к единой сетевой границе

Значительная часть этого релиза на самом деле является одним архитектурным изменением, просто доставленным серией коммитов: продолжающейся `NetCore migration`. NetCore теперь вынесен в отдельный пакет, работа с соединениями заметно сильнее смещена к `ConnID`-first API, а большее число inbound и outbound путей проходит через общую границу `Network`, а не через разрозненный прямой доступ к сокетам.

На практике это делает lifecycle соединений более целостным, уменьшает утечки low-level connection state в node layer и даёт routing, dispatch, auth, ping и session teardown более безопасную и единообразную модель выполнения.

## Встроенный peer traffic capture

Corsa теперь умеет записывать live wire traffic peers без внешних sniffers. Новый встроенный capture flow поддерживает запись по connection ID, по remote IP и глобально, а управление запуском и остановкой доступно через RPC и desktop console.

Состояние capture также возвращается обратно в diagnostics и peer views, поэтому malformed traffic, неожиданное протокольное поведение и трудно воспроизводимые проблемы с peers стало заметно проще исследовать прямо из самой ноды.

## Детектирование необходимости апгрейда и lockout несовместимых peers

В релизе появилась node-owned version policy, которая отслеживает версионные сигналы от peers, определяет ситуации, когда сеть фактически подсказывает, что узел пора обновить, и применяет устойчивый lockout для подтверждённо несовместимых peers.

Это улучшает наблюдаемость проблем связности, связанных с версиями, и помогает ноде не крутиться повторно вокруг peers, про которых уже известно, что они несовместимы. В рамках той же работы сохранена совместимость relay delivery receipts, поэтому легитимные transit receipts от более новых peers теперь форвардятся, а не ошибочно классифицируются.

## Более экономный route announcement traffic

Обработка route announcements стала строже за счёт канонических per-peer announce snapshots, вычисления delta и явного отслеживания per-peer announce state. Цель этих изменений — уменьшить избыточный announce traffic и при этом сохранить предсказуемый resync после reconnect и изменений топологии.

В результате распространение маршрутов становится менее шумным и получает более чистый фундамент для будущих incremental announce modes без изменения базовых правил корректности вокруг withdrawals и origin ownership.

## Более безопасные RPC-defaults при старте

RPC server теперь не запускается, если не настроены auth credentials. Вместо того чтобы открывать или занимать RPC port в частично настроенной среде, нода держит RPC выключенным до тех пор, пока явно не заданы и username, и password.

Это более безопасный default для локальных и multi-instance сценариев и снижает риск случайной экспозиции неаутентифицированной control plane.
