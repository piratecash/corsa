# What's New in v0.37-alpha

## Live Node State Now Flows Through an Internal Event Bus

This release introduces an internal asynchronous topic bus (`ebus`) and moves node-state propagation onto that live event stream. Peer health, slot state, routing reachability, aggregate network status, version policy, traffic, capture lifecycle, contacts, and identities can now flow through one shared in-process path instead of depending on ad hoc ownership and heavier polling updates.

For Desktop, console, and SDK consumers, this creates a cleaner real-time state model. A new `NodeStatusMonitor` seeds from `ProbeNode` and then keeps status current from live deltas, which reduces drift between initial snapshots and ongoing runtime changes and gives UI/runtime components a more consistent view of the node.

## Stricter Advertise-Address Truth and Convergence

Handshake processing is now much stricter about whether a peer's advertised listening address matches the address actually observed on the connection. The node distinguishes valid matches, non-listener and legacy-direct cases, local/private exceptions, and real world mismatches, and it can reject incorrect public advertises with a machine-readable `connection_notice` carrying the observed address hint.

This improves trust in advertised peer endpoints, helps peers converge toward a truthful self-advertise address over time, and reduces the chance that stale or incorrect public addresses remain treated as safely announceable across inbound and outbound paths.

## Stronger Foundations for Diagnostics and Stability

Alongside the two major runtime changes, the codebase also received a broad testing refactor based on generated mocks and expanded integration coverage around routing, capture, RPC, and node-state flows.

That work is mostly internal, but it matters: the new event-driven status path and advertise-convergence rules now land with much stronger regression coverage than before, which should make follow-up iterations safer.

---

# Что нового в v0.37-alpha

## Live-состояние ноды теперь идёт через внутреннюю event bus

В этом релизе появилась внутренняя асинхронная topic bus (`ebus`), и распространение node state переведено на этот live event stream. Peer health, slot state, routing reachability, aggregate network status, version policy, traffic, capture lifecycle, contacts и identities теперь проходят через один общий in-process путь вместо разрозненного владения состоянием и более тяжёлых polling-обновлений.

Для Desktop, console и SDK это создаёт более чистую модель real-time состояния. Новый `NodeStatusMonitor` сначала получает seed через `ProbeNode`, а затем поддерживает статус актуальным через live deltas, что уменьшает расхождение между стартовым snapshot и дальнейшими runtime-изменениями и даёт UI/runtime-компонентам более согласованное представление о ноде.

## Более строгая truth/convergence логика для advertise-address

Обработка handshake теперь заметно строже относится к тому, совпадает ли advertised listening address пира с адресом, реально наблюдаемым на соединении. Нода различает корректные совпадения, случаи non-listener и legacy-direct, local/private exceptions и настоящие world mismatch-сценарии, а при некорректном публичном advertise может отклонять соединение с machine-readable `connection_notice`, содержащим observed-address hint.

Это повышает доверие к advertised peer endpoints, помогает peers со временем сходиться к правдивому self-advertise address и уменьшает вероятность того, что stale или неверные публичные адреса будут и дальше считаться безопасно announceable на inbound и outbound путях.

## Более прочная база для diagnostics и стабильности

Параллельно с двумя основными runtime-изменениями кодовая база получила широкую тестовую миграцию на generated mocks и расширенное integration coverage вокруг routing, capture, RPC и потоков node state.

Это в основном внутренняя работа, но она важна: новый event-driven путь статуса и advertise-convergence правила теперь приходят с заметно более сильной regression coverage, что должно сделать следующие итерации безопаснее.
