# What's New in v1.0.56

## Inbox Subscription Is Folded Into Auth for v20 Peers

Protocol version 20 removes the extra node-to-node `subscribe_inbox` round-trip from the realtime path. For peers that advertise v20 or newer, authentication now proves the initiator identity and installs the responder-side inbox subscription as part of `auth_session`.

Backlog replay also moves into the authenticated handshake path. The responder queues `auth_ok` first and only then replays backlog items, so `push_message` and delivery-receipt frames cannot race ahead of successful authentication on the writer.

Initiators skip `subscribe_inbox` when the peer advertises v20 or newer, but still send it to older peers. The `subscribe_inbox` and `request_inbox` handlers remain wired for legacy peers and client-role subscribers; they are now deprecated and can be removed once `MinimumProtocolVersion` reaches 20.

The old symmetric reverse-subscribe exchange is no longer used between two v20 nodes. Each node receives its own inbox over its own authenticated outbound session, while gossip still provides mesh-wide propagation. NAT'd nodes remain unaffected because they already act as initiators.

The realtime and mesh docs were updated in English and Russian. They now describe the v20 auth-folded path as primary, mark symmetric `subscribe_inbox` as legacy/client-role, and clarify that relay/pending backlog is an in-memory runtime backlog, not restart-durable disk state.

## More Allocation Churn Removed From Routing and Gossip

The node now memoizes its loopback-reachable listen address instead of rebuilding the `"127.0.0.1" + ListenAddress` form on every self-address check. That self-address predicate runs inside gossip target-selection loops, so caching removes a large number of one-off string allocations without changing the computed address.

Gossip target selection also skips its pending-peer scan when already-connected session targets fill the fan-out cap. In that steady-state case the skipped phase would only allocate and scan a peer list whose results would be discarded later.

Pending-drain work now allocates its metadata map lazily. The common route/announce churn event matches nothing in the pending queue, so that dominant no-match path is now allocation-free.

Regression coverage was updated around the v20 auth-folded backlog flow, legacy subscription behavior, self-address caching, gossip target selection, and pending-drain no-match behavior.

---

# Что нового в v1.0.56

## Inbox subscription встроен в auth для v20 peers

Protocol version 20 убирает лишний node-to-node round-trip `subscribe_inbox` из realtime path. Для peers, которые рекламируют v20 или новее, authentication уже доказывает identity инициатора и устанавливает responder-side inbox subscription прямо внутри `auth_session`.

Backlog replay тоже переезжает в authenticated handshake path. Responder сначала ставит в очередь `auth_ok` и только потом replay'ит backlog items, поэтому `push_message` и delivery-receipt frames не могут обогнать успешную authentication на writer.

Initiators пропускают `subscribe_inbox`, когда peer рекламирует v20 или новее, но всё ещё отправляют его старым peers. Handlers `subscribe_inbox` и `request_inbox` остаются подключены для legacy peers и client-role subscribers; теперь они deprecated и могут быть удалены, когда `MinimumProtocolVersion` дойдёт до 20.

Старый symmetric reverse-subscribe exchange больше не используется между двумя v20 nodes. Каждая node получает свой inbox через собственную authenticated outbound session, а gossip по-прежнему даёт mesh-wide propagation. NAT'd nodes не затронуты, потому что они и так выступают initiators.

Realtime и mesh docs обновлены на английском и русском. Они описывают v20 auth-folded path как основной, помечают symmetric `subscribe_inbox` как legacy/client-role и уточняют, что relay/pending backlog — это in-memory runtime backlog, а не restart-durable disk state.

## Ещё меньше allocation churn в routing и gossip

Node теперь memoizes loopback-reachable listen address вместо того, чтобы заново собирать `"127.0.0.1" + ListenAddress` на каждый self-address check. Этот predicate работает внутри gossip target-selection loops, поэтому cache убирает много одноразовых string allocations без изменения результата.

Gossip target selection также пропускает pending-peer scan, когда already-connected session targets уже заполняют fan-out cap. В таком steady-state случае пропущенная phase только alloc/scan'ила peer list, результаты которого потом всё равно отбрасывались.

Pending-drain work теперь allocates metadata map lazily. Обычный route/announce churn event ничего не находит в pending queue, поэтому доминирующий no-match path стал allocation-free.

Regression coverage обновлена вокруг v20 auth-folded backlog flow, legacy subscription behavior, self-address caching, gossip target selection и pending-drain no-match behavior.
