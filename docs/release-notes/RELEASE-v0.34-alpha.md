# What's New in v0.34-alpha

## Unified Network Health Across Node and Desktop

This release introduces a single aggregate network-health model inside the node and makes Desktop consume that shared snapshot instead of recomputing its own status locally. The result is more consistent reporting of healthy, limited, warning, reconnecting, and offline states, along with clearer counts for usable peers, connected peers, and pending messages.

In practice, the network status shown in Desktop now matches the node's own policy decisions much more closely, which reduces drift between what the UI says and how the node actually behaves.

## Smarter Peer Exchange in Steady State

Peer exchange is now driven by aggregate network status. When the node is still recovering or has too few usable peers, it actively requests more addresses during sync. Once the node is already healthy, it stops reissuing `get_peers` on every reconnect and relies on normal peer-announcement gossip instead.

This makes steady-state behavior quieter and more intentional while still preserving fast recovery when connectivity is weak. Compatibility coverage was also expanded to keep mixed-version peer exchange stable on the wire.

## Safer Connection Lifecycle and Outbound Writes

Outbound peer-session writes are now routed through the same NetCore single-writer discipline used to protect managed connection I/O, and transitional connection bookkeeping has been collapsed into a unified connection registry.

These changes reduce the chance of inconsistent socket state, keep connection teardown more coherent, and make routing, relay, ping, and file-transfer paths more predictable under reconnects and slow-peer conditions.

## Cleaner RPC Semantics Across Transports

The RPC command table now stays transport-neutral, and desktop-specific semantic overrides have been removed. Desktop UI, desktop console, and external RPC callers now rely more cleanly on the same command definitions and availability rules.

That means fewer semantic forks between local and remote command paths, simpler maintenance, and a more dependable contract for tooling built on top of the RPC layer.

---

# Что нового в v0.34-alpha

## Единая модель состояния сети для node и Desktop

В этом релизе внутри ноды появилась единая aggregate-модель состояния сети, а Desktop теперь использует этот общий snapshot вместо локального пересчёта статуса. В результате состояния healthy, limited, warning, reconnecting и offline отображаются более последовательно, а счётчики usable peers, connected peers и pending messages стали понятнее и точнее.

На практике это уменьшает расхождение между тем, что показывает UI, и тем, какие решения реально принимает нода.

## Более умный peer exchange в steady state

Peer exchange теперь управляется aggregate network status. Когда нода ещё восстанавливается или у неё слишком мало usable peers, во время sync она активно запрашивает новые адреса. Когда сеть уже здорова, нода перестаёт повторно отправлять `get_peers` на каждом reconnect и опирается на обычный gossip через peer announcements.

Это делает steady-state поведение тише и осмысленнее, но при этом сохраняет быстрый recovery при слабой связности. Дополнительно был расширен compatibility coverage, чтобы mixed-version peer exchange оставался wire-compatible.

## Более безопасный lifecycle соединений и outbound writes

Запись в outbound peer sessions теперь проходит через ту же дисциплину NetCore single-writer, которая уже защищает managed connection I/O, а переходное состояние соединений сведено в единый connection registry.

Эти изменения уменьшают вероятность несогласованного socket state, делают teardown соединений более целостным и повышают предсказуемость routing, relay, ping и file transfer в условиях reconnect и slow-peer сценариев.

## Более чистая RPC-семантика между transport paths

RPC CommandTable теперь остаётся transport-neutral, а desktop-specific semantic overrides удалены. Desktop UI, desktop console и внешние RPC-клиенты теперь заметно чище опираются на одни и те же определения команд и правила их доступности.

Это уменьшает количество semantic forks между локальным и удалённым путями вызова, упрощает сопровождение и делает RPC-контракт надёжнее для инструментов, которые строятся поверх него.
