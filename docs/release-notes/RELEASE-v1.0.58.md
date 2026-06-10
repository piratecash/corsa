# What's New in v1.0.58

## Focused Node Memory-Stability Hotfix

This release targets the slow memory growth seen on long-running nodes after heavy routing and relay storms. The main fix replaces goroutine-per-target gossip fan-out with bounded worker lanes: DM and receipt fan-out now run through a capped dispatch pool, while push notices use a separate lane so normal relay storms cannot starve notice delivery.

Previously, a relay backlog could spawn one goroutine per retryable message and per gossip target on every retry tick. Those goroutines exited, but the Go runtime retained goroutine descriptors and stack high-water marks, causing `mem_sys` to ratchet upward after each storm. Peak gossip fan-out concurrency is now bounded by the worker pool instead of peer/message volume.

## Routing SeqNo State Is Bounded to the Live Working Set

Outbound route-announcement SeqNo state no longer grows for every wire-content shape ever emitted. The reusable outbound-content cache now prunes dead entries, idle entries, and entries for identities that have fully disappeared, with a soft cap as a final backstop.

The per-receiver high-watermark map is bounded more carefully: it is not TTL-evicted, because dropping a live receiver watermark can cause stale-reject loops. Instead, receiver watermarks are removed when the receiver disconnects or when the destination identity is gone, preserving correctness while stopping lifetime-only growth.

## Ban and Setup-Failure Maps Are Periodically Purged

The node now periodically cleans expired or stale state from the ban/blacklist domain: local ban scores, local IP-wide bans, remote IP-wide bans, remote offender buckets, and setup-failure counters. Before this, several maps shrank only lazily when the same IP returned or a peer recovered, so transient bad addresses could leave permanent residue on long-lived nodes.

Together with the routing SeqNo eviction and bounded gossip dispatch, this keeps auxiliary node state closer to the actual live mesh instead of the historical set of every peer, route shape, and offender ever seen during uptime.

## Protocol Version

The client build is bumped to `1.0.58`. The wire `ProtocolVersion` advances to `22`, and `MinimumProtocolVersion` advances to `18`.

Regression coverage was added for bounded gossip dispatch, gossip overflow behavior, ban-state purging, setup-failure cleanup, outbound SeqNo cache eviction, receiver-watermark lifecycle cleanup, and stale-reject safety around route-announcement SeqNo reuse.

---

# Что нового в v1.0.58

## Точечный hotfix стабильности памяти node

Этот релиз закрывает медленный рост памяти на long-running nodes после тяжёлых routing и relay storms. Главная правка заменяет goroutine-per-target gossip fan-out на ограниченные worker lanes: DM и receipt fan-out теперь проходят через capped dispatch pool, а push notices используют отдельную lane, чтобы обычные relay storms не могли вытеснить доставку notices.

Раньше relay backlog мог на каждом retry tick создавать по горутине на retryable message и на каждый gossip target. Эти горутины завершались, но Go runtime удерживал goroutine descriptors и stack high-water marks, из-за чего `mem_sys` поднимался ступеньками после каждого storm. Теперь peak concurrency для gossip fan-out ограничен worker pool, а не количеством peers/messages.

## Routing SeqNo state ограничен live working set

Outbound route-announcement SeqNo state больше не растёт на каждый wire-content shape, который когда-либо был отправлен. Reusable outbound-content cache теперь удаляет dead entries, idle entries и entries для identities, которые полностью исчезли, а soft cap остаётся последним backstop.

Per-receiver high-watermark map ограничен аккуратнее: он не evict'ится по TTL, потому что удаление live receiver watermark может вызвать stale-reject loops. Вместо этого receiver watermarks удаляются, когда receiver отключился или destination identity исчезла, сохраняя correctness и одновременно останавливая lifetime-only growth.

## Ban и setup-failure maps теперь периодически чистятся

Node теперь периодически чистит expired/stale state из ban/blacklist domain: local ban scores, local IP-wide bans, remote IP-wide bans, remote offender buckets и setup-failure counters. Раньше несколько map'ов уменьшались только lazily — когда тот же IP возвращался или peer восстанавливался, — поэтому transient bad addresses могли оставлять постоянный residue на long-lived nodes.

Вместе с routing SeqNo eviction и bounded gossip dispatch это держит auxiliary node state ближе к реальной live mesh, а не к историческому набору всех peers, route shapes и offenders, увиденных за время uptime.

## Версия протокола

Client build поднят до `1.0.58`. Wire `ProtocolVersion` поднят до `22`, а `MinimumProtocolVersion` — до `18`.

Добавлена regression coverage для bounded gossip dispatch, gossip overflow behavior, ban-state purging, setup-failure cleanup, outbound SeqNo cache eviction, receiver-watermark lifecycle cleanup и stale-reject safety вокруг повторного использования route-announcement SeqNo.
