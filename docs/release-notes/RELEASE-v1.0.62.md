# What's New in v1.0.62

## Routing Phase 3 Becomes the New Announce Core

This release is centered around the next big routing step: Phase 3 moves the announce path to a cursor-authoritative model. Instead of rebuilding and diffing the whole table for every peer on every delta cycle, the node now tracks route and health mutations in a change journal and projects only the identities that actually changed since a peer's cursor.

That makes the announce plane much more incremental and gives it better internal correctness signals. The journal and cursor path were first deployed with shadow validation, and temporary divergence counters were surfaced through `getResourceUsage` and cycle logs so operators could confirm that real-world churn stayed at zero divergence before the cursor model became authoritative.

## Steady-State Routing Gets Much Quieter

Route sync also becomes more efficient in this release. The old periodic full-table refresh path is replaced first by a digest heartbeat and then by a version-vector reconcile that can confirm convergence per identity instead of treating the whole table as one opaque hash.

The practical result is that converged peers can stay quiet for much longer instead of repeatedly re-flooding full route state just to prove liveness. TTL and forced-full cadence were stretched accordingly, health aging was tightened, and route-sync now degrades cleanly back to chunked full sync when the lighter reconcile path is not appropriate.

## Send-Path and Routing Churn Are Cheaper to Carry

Several performance fixes support the new model: exact `O(N)` v3 announce chunk sizing replaces the old `O(N^2)` re-marshal probe, `MarshalFrameLineBytes` removes an extra send-path copy, identity hex is memoized on the announce projection, and multiple announce/sync hot paths were tightened to reduce alloc churn.

Together with smaller fixes like capping passive aging at `Bad`, refreshing `Bad` health only on real wire changes, and making route-health churn attribution more precise, the routing stack should be both quieter and easier to reason about under dense mesh load.

---

# Что нового в v1.0.62

## Routing Phase 3 становится новым ядром announce path

Этот релиз почти целиком посвящён следующему большому шагу в routing: в Phase 3 announce path переходит на cursor-authoritative модель. Вместо rebuild-and-diff всей таблицы для каждого peer на каждом delta cycle node теперь ведёт change journal по route и health mutations и проецирует только те identities, которые действительно изменились с момента курсора конкретного peer.

Это делает announce plane гораздо более incremental и одновременно даёт ей более сильные сигналы внутренней корректности. Сначала journal/cursor path был выкатан с shadow validation, а временные divergence counters были выведены в `getResourceUsage` и cycle logs, чтобы можно было подтвердить на реальном churn, что расхождение держится на нуле до переключения authoritative режима.

## Steady-state routing стала заметно тише

Route sync в этом релизе тоже становится эффективнее. Старый periodic full-table refresh сначала заменяется на digest heartbeat, а затем на version-vector reconcile, который умеет подтверждать convergence по identities, а не рассматривает всю таблицу как один непрозрачный hash.

Практический эффект в том, что сошедшиеся peers могут гораздо дольше оставаться тихими вместо постоянного full re-flood route state только ради проверки liveness. Под это же были растянуты TTL и forced-full cadence, tightened health aging и добавлен clean fallback обратно к chunked full sync там, где лёгкий reconcile path не подходит.

## Send path и routing churn стали дешевле

Несколько perf-правок поддерживают новую модель: точный `O(N)` sizing для v3 announce chunks заменяет старый `O(N^2)` re-marshal probe, `MarshalFrameLineBytes` убирает лишнюю копию на send path, hex-представление identity memoize'ится на announce projection, а несколько hot paths в announce/sync дополнительно ужаты по alloc churn.

Вместе с более мелкими фиксам вроде cap для passive aging на уровне `Bad`, refresh `Bad` health только при реальном wire change и более точной attribution для route-health churn это делает routing stack тише и понятнее под нагрузкой плотной mesh.
