# What's New in v1.0.55

## Message and Relay Hot Paths Allocate Less

Profiling showed that the latest memory spikes were not a live-object leak: heap after GC stayed around the expected working set and goroutine counts were flat. The large peaks came from short-lived allocation churn between GC cycles. This release trims several of the top `alloc_space` sources on gossip, relay, and snapshot paths.

Gossip fan-out now builds the `push_message` frame once per message and shares it read-only across targets, instead of rebuilding the same frame and re-copying ciphertext for every peer. The per-message relay path also keeps the clone-free inbound connection lookup introduced in the previous release.

Pending-frame dedup keys now use a comparable struct instead of concatenated strings, removing per-call string allocations on queue, flush, and drain paths. Relay retry also avoids building debug-only `gossip_targets` slices unless debug logging is actually enabled.

## Less Background Snapshot Churn

The peers-exchange snapshot rebuild is now reader-gated. Nodes that are not receiving `get_peers` calls stop paying for the full 2x-per-second rebuild of persisted metadata, health maps, and peer-provider candidates.

To keep discovery safe after idle periods, the cache still has a staleness floor: while no readers are active it rebuilds occasionally, so the first post-idle `get_peers` call cannot observe an unbounded-old peer list.

Aggregate pending counts now avoid a per-mutation address map allocation and compute the count directly from pending plus orphaned queues. Retryable relay message scans reuse a per-service scratch buffer for the `topics["dm"]` snapshot and shed oversized buffers so old payloads are not pinned after a spike.

## Operational Memory Guidance

The README now documents using `GOMEMLIMIT` during rollout, for example `GOMEMLIMIT=640MiB`, to cap RSS peaks while the reduced-churn paths settle under production load.

Regression coverage was added around non-routable CIDR precomputation, bounded-stale peers-exchange rebuilds, pending-key behavior, relay retry scratch handling, and the unchanged inbound connection lookup semantics.

---

# Что нового в v1.0.55

## Message и relay hot paths теперь меньше аллоцируют

Профилирование показало, что свежие memory spikes не были live-object leak: heap после GC оставался около ожидаемого working set, а число goroutines было стабильным. Большие пики давал short-lived allocation churn между GC cycles. Этот релиз убирает несколько верхних `alloc_space` источников в gossip, relay и snapshot paths.

Gossip fan-out теперь строит `push_message` frame один раз на сообщение и read-only шарит его между targets, вместо того чтобы пересобирать тот же frame и заново копировать ciphertext для каждого peer. Per-message relay path также сохраняет clone-free inbound connection lookup из предыдущего релиза.

Pending-frame dedup keys теперь используют comparable struct вместо concatenated strings, убирая per-call string allocations на queue, flush и drain paths. Relay retry больше не строит debug-only `gossip_targets` slices, если debug logging реально не включён.

## Меньше background snapshot churn

Peers-exchange snapshot rebuild теперь reader-gated. Ноды, на которые не приходят `get_peers` calls, перестают платить за полный 2x-per-second rebuild persisted metadata, health maps и peer-provider candidates.

Чтобы discovery после idle-периодов оставался безопасным, cache всё равно имеет staleness floor: пока активных readers нет, он периодически rebuild'ится, поэтому первый post-idle `get_peers` call не увидит бесконечно старый peer list.

Aggregate pending counts теперь обходятся без per-mutation address map allocation и считают значение напрямую из pending plus orphaned queues. Retryable relay message scans переиспользуют per-service scratch buffer для `topics["dm"]` snapshot и сбрасывают oversized buffers, чтобы старые payloads не удерживались после spike.

## Operational memory guidance

README теперь документирует использование `GOMEMLIMIT` на время rollout, например `GOMEMLIMIT=640MiB`, чтобы ограничивать RSS peaks, пока reduced-churn paths обкатываются под production load.

Добавлена regression coverage вокруг non-routable CIDR precomputation, bounded-stale peers-exchange rebuilds, pending-key behavior, relay retry scratch handling и неизменной семантики inbound connection lookup.
