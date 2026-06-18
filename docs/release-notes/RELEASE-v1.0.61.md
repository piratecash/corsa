# What's New in v1.0.61

## Routing-Control Churn Is More Tightly Bounded

This release puts a lot of work into reducing routing-control storms on dense or unstable meshes. Poison-reverse emission is now batched, probing backs off more deliberately, and several announce-path allocations are pooled so repeated routing cycles generate less memory churn.

The routing snapshot path is also leaner: rebuild cadence is lowered, `Snapshot.Health` is narrowed to the routing-relevant `Dead` and cooled subset, and announce projection buffers are recycled instead of reallocated every cycle. Together, these changes make the routing plane calmer under churn and cheaper to maintain in steady state.

## Long-Lived Nodes Keep Less Stale State Around

Several fixes in this release target live-heap growth on long-running relays and high-churn nodes. Route-health state is now evicted eagerly when a peer is invalidated instead of lingering for tombstone TTL, receipt dedup is bounded, known identities move to a capped recency structure, and receipt backlogs are limited instead of accumulating indefinitely.

There are also smaller memory wins across the node: the nonce cache no longer preallocates a large map when file transfer is idle, and connection capability views are aliased instead of cloned per entry. The practical effect is less silent state buildup over time and a lower baseline allocation profile.

## Operators Get a New `connectOnly` Mode

Nodes can now be pinned to a single outbound peer with `CORSA_CONNECT_ONLY`. In this mode, automatic egress is retained only for the selected peer and other outbound connections are dropped, giving operators a clearer way to run pinned topologies or tightly controlled experiments.

The new mode is wired through config, node behavior, and RPC surfaces, so it can be inspected and managed without ad hoc local patches.

---

# Что нового в v1.0.61

## Routing-control churn теперь жёстче ограничен

В этом релизе много работы ушло на снижение routing-control storms в плотных или нестабильных mesh. Emission для poison-reverse теперь батчится, probing уходит в более аккуратный back-off, а несколько allocation-heavy участков announce path переведены на pooling, чтобы повторяющиеся routing cycles создавали меньше memory churn.

Путь построения routing snapshot тоже стал легче: cadence rebuild'ов снижен, `Snapshot.Health` сужен до действительно routing-relevant подмножества `Dead` и cooled routes, а announce projection buffers теперь переиспользуются вместо нового выделения на каждом цикле. В сумме это делает routing plane спокойнее под churn и дешевле в steady state.

## Long-lived nodes держат меньше stale state

Несколько правок в этом релизе целятся прямо в рост live heap на long-running relays и high-churn nodes. Route-health state теперь eagerly eviction'ится при peer invalidation вместо жизни до tombstone TTL, receipt dedup ограничен, known identities переведены на capped recency structure, а receipt backlog больше не накапливается бесконечно.

Есть и более мелкие memory wins по node: nonce cache больше не делает крупную preallocation, когда file transfer idle, а capability views в connection snapshots теперь alias'ятся вместо per-entry clone. Практический эффект — меньше тихого накопления состояния со временем и более низкий baseline allocation profile.

## У операторов появился режим `connectOnly`

Теперь node можно закрепить на одном outbound peer через `CORSA_CONNECT_ONLY`. В этом режиме automatic egress сохраняется только для выбранного peer, а остальные outbound connections сбрасываются, что даёт более прозрачный способ запускать pinned topology или строго контролируемые эксперименты.

Новый режим проведён через config, node behavior и RPC surfaces, так что его можно наблюдать и использовать без локальных ad hoc патчей.
