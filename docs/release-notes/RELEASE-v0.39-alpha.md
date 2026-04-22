# What's New in v0.39-alpha

## Hot RPC Reads Are Now Decoupled From Core Locks

This release moves the hottest local RPC read paths onto primed atomic snapshots. Commands such as `fetch_network_stats`, `fetch_peer_health`, and `get_peers` now read prebuilt snapshots instead of competing directly for `s.mu` and `cm.mu` on the request path.

In practice, this makes Desktop and other local consumers more resilient during reconnect storms and heavy node churn. Even when writers are busy, the UI can keep reading the last good snapshot instead of stalling behind lock contention.

## Faster Route Announcement and Withdrawal Fanout

Routing fanout has been parallelized in several places: route announcements fan out more efficiently, inbound full sync work is moved off the critical path, and disconnect-triggered withdrawals now propagate in parallel as well.

The result is a routing layer that reacts faster to topology changes and does less serial waiting when multiple peers need to be updated at once.

## Queue-State Persistence Moves to an Async Debounced Writer

Queue-state persistence is now handled by an asynchronous debounced persister instead of writing synchronously on hot mutation paths. Multiple quick updates can collapse into a smaller number of disk writes while shutdown still performs a final flush.

This reduces write amplification and removes more disk I/O from peer lifecycle, relay, and routing-sensitive paths, which helps keep runtime behavior smoother under bursty activity.

## Inbound Peer Announcement Uses Observed IP Plus Hello/Default Port

Inbound peers are now announced using the observed remote IP together with the peer's declared hello port or the default port when appropriate, instead of relying on less trustworthy combinations.

This improves the quality of propagated peer addresses and makes peer exchange more likely to distribute endpoints that other nodes can actually dial successfully.

---

# Что нового в v0.39-alpha

## Hot RPC reads теперь отвязаны от core locks

В этом релизе самые горячие local RPC read-paths переведены на primed atomic snapshots. Команды вроде `fetch_network_stats`, `fetch_peer_health` и `get_peers` теперь читают заранее собранные snapshots, а не конкурируют напрямую за `s.mu` и `cm.mu` на самом request path.

На практике это делает Desktop и другие локальные consumers заметно устойчивее во время reconnect storms и сильного churn внутри ноды. Даже когда writers заняты, UI может продолжать читать последний корректный snapshot вместо того, чтобы зависать на lock contention.

## Более быстрый fanout route announcements и withdrawals

Routing fanout был распараллелен в нескольких местах: route announcements расходятся эффективнее, inbound full sync вынесен с критического пути, а withdrawals при disconnect теперь тоже распространяются параллельно.

В результате routing layer быстрее реагирует на изменения топологии и меньше страдает от последовательного ожидания, когда сразу несколько peers нужно обновить одновременно.

## Queue-state persistence перенесена на async debounced writer

Сохранение queue state теперь выполняется асинхронным debounced persister'ом вместо синхронной записи на горячих mutation paths. Несколько быстрых обновлений могут схлопываться в меньшее число disk writes, при этом на shutdown всё равно выполняется финальный flush.

Это уменьшает write amplification и убирает дополнительный disk I/O из peer lifecycle, relay и routing-чувствительных путей, помогая системе вести себя плавнее при bursty-активности.

## Inbound peer announce теперь использует observed IP плюс hello/default port

Inbound peers теперь анонсируются с использованием реально наблюдаемого remote IP и порта, объявленного peer'ом в hello, либо default port там, где это уместно, вместо менее надёжных комбинаций.

Это повышает качество распространяемых peer addresses и делает peer exchange более склонным раздавать endpoints, к которым другие ноды действительно смогут успешно подключиться.
