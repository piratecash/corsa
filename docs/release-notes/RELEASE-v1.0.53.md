# What's New in v1.0.53

## Pending Relay State Is Now RAM-Only and Bounded

This release removes the `queue-<port>.json` disk persistence hot path that dominated allocation churn at scale. Pending relay/queue state is now kept in memory only, backed by a per-peer ring with a hard capacity (`CORSA_PENDING_RING_SIZE`, default `200`). When the ring is full, the oldest pending item is evicted instead of growing without bound or spilling another full snapshot to disk.

On startup, an existing pre-upgrade queue file is loaded once for migration and then removed. After that, pending relay state no longer survives node restarts. Recovery is intentionally sender-side: end-to-end retry is the contract for messages that were pending when a node stopped.

The practical result is a much tighter memory ceiling during routing and relay churn. The old persisted queue snapshot avoided losing pending state across restarts, but on large meshes it became a major source of marshal churn, GC work, and retained runtime pages. The new ring trades restart persistence for predictable RAM use.

## Mesh Routing v3 Is Enabled by Default

The compact mesh routing v3 announce path is now the default. Nodes advertise `CapMeshRoutingV3` unless operators explicitly opt out with `CORSA_ENABLE_MESH_ROUTING_V3=0`, `false`, `no`, or `off`.

Negotiation remains backward compatible: v3 frames are sent only to peers that also advertise the capability. Older peers, or peers that opt out, continue to use the legacy `announce_routes` / `routes_update` path.

The wire/runtime contract was bumped for this release: client build `53`, protocol version `19`, and minimum protocol version `15`.

## Less Peer-Health Snapshot Churn

Peer-health rebuilds now index inbound connections once per rebuild instead of scanning all connections per peer. Capability lookup is folded into the same index, removing repeated per-entry capability clones.

This keeps behavior unchanged — session capabilities still win over inbound fallback, and all inbound connection IDs for an address are preserved — but collapses the rebuild work from repeated peer-by-connection scans into one connection pass plus O(1) lookups.

## Smoother Desktop Status Updates Under Storms

Status updates now notify only when pending/slot values actually change, and the `OnChanged` to `NotifyStatusChanged` bridge is coalesced with a short trailing debounce. That rate-bounds expensive full node-status deep copies during status-event storms and reduces UI freeze risk while preserving the latest state.

Regression coverage was added for pending-ring eviction and restart behavior, stale queue-file migration/removal, mesh v3 default capability handling, inbound peer-health indexing, and status notification coalescing.

---

# Что нового в v1.0.53

## Pending relay state теперь RAM-only и с жёстким лимитом

Этот релиз убирает hot path disk persistence через `queue-<port>.json`, который на масштабе стал главным источником allocation churn. Pending relay/queue state теперь хранится только в памяти, через per-peer ring с жёсткой ёмкостью (`CORSA_PENDING_RING_SIZE`, по умолчанию `200`). Когда ring заполнен, самый старый pending item вытесняется вместо бесконечного роста или записи очередного полного snapshot на диск.

При старте существующий pre-upgrade queue file загружается один раз для миграции и затем удаляется. После этого pending relay state больше не переживает restart ноды. Recovery намеренно переносится на sender side: end-to-end retry является контрактом для сообщений, которые были pending в момент остановки node.

Практический эффект — намного более предсказуемый memory ceiling во время routing и relay churn. Старый persisted queue snapshot помогал не терять pending state между рестартами, но на больших mesh превращался в крупный источник marshal churn, GC work и retained runtime pages. Новый ring меняет restart persistence на контролируемое RAM use.

## Mesh routing v3 включён по умолчанию

Compact mesh routing v3 announce path теперь default. Ноды рекламируют `CapMeshRoutingV3`, если оператор явно не отключил его через `CORSA_ENABLE_MESH_ROUTING_V3=0`, `false`, `no` или `off`.

Negotiation остаётся backward compatible: v3 frames отправляются только пирам, которые тоже рекламируют capability. Старые peers или peers с opt-out продолжают использовать legacy `announce_routes` / `routes_update` path.

Wire/runtime contract поднят для этого релиза: client build `53`, protocol version `19`, minimum protocol version `15`.

## Меньше churn в peer-health snapshots

Peer-health rebuilds теперь индексируют inbound connections один раз за rebuild вместо scan всех connections для каждого peer. Capability lookup встроен в тот же index, убирая повторные per-entry capability clones.

Поведение не меняется: session capabilities всё ещё сильнее inbound fallback, а все inbound connection IDs для address сохраняются. Но rebuild work схлопывается из повторных peer-by-connection scans в один connection pass плюс O(1) lookups.

## Более плавные desktop status updates во время storms

Status updates теперь notify'ят только при реальном изменении pending/slot values, а мост `OnChanged` to `NotifyStatusChanged` coalesced через короткий trailing debounce. Это rate-bounds дорогие full node-status deep copies во время status-event storms и снижает риск UI freeze, сохраняя последнее состояние.

Добавлена regression coverage для pending-ring eviction и restart behavior, stale queue-file migration/removal, mesh v3 default capability handling, inbound peer-health indexing и status notification coalescing.
