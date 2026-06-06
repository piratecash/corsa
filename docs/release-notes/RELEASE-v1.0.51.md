# What's New in v1.0.51

## Bad-Hop Recidivism Now Enters Hold-Down Instead of Rewriting Tombstones Forever

This release adds bad-hops hysteresis on top of the fast-invalidation path introduced in the routing plane. When a single identity repeatedly sends routes with impossible hop counts, the first accepted bad-hop events still invalidate the affected transit claim, preserving the sequence-number guard against resurrection. If the same identity keeps recidivating inside the observation window, it enters hold-down.

During hold-down, further bad-hop claims for that identity are dropped before storage mutation: no tombstone rewrite, no snapshot rebuild, and no per-event log amplification. Sane-hop claims continue to pass, so a loop-free recovery path remains available. Repeated hold-down engagements inside the recidivism window back off the penalty duration, capped to avoid permanent exile.

The practical effect is a calmer route table during count-to-infinity loops or hostile/broken announce storms. Operators can see engagements through the `bad_hops_holdowns` counter in the route summary, while the existing `fast_invalidations` counter remains focused on accepted invalidations.

## Memory and Uptime Are Visible Through RPC and Desktop UI

Nodes now expose `getResourceUsage`, returning process memory totals, live heap allocation, uptime, and the sample timestamp in both machine-readable and human-readable form. The desktop console Info tab shows the same Memory and Uptime values and refreshes them through the node status monitor without sampling on the render path.

This gives operators a cheap first-pass diagnostic before reaching for heavier profiling: watch whether live heap plateaus or climbs over time, then escalate only when the trend points to a real leak.

The release also fixes monotonic growth in per-connection metadata maps keyed by ephemeral inbound ports. Orphaned peer IDs, versions, builds, and observed-IP history are swept after reconnect churn, reducing long-running memory pressure on busy nodes.

## Opt-In Loopback-Only pprof Server

For deeper investigations, `CORSA_PPROF_ADDR` can now start Go's standard pprof HTTP server. It is off by default and refuses non-loopback binds, so heap, CPU, and goroutine dumps are available only for deliberate local diagnostics or SSH-tunneled remote debugging.

The new debugging docs describe when to use the lightweight `getResourceUsage` RPC and when to collect heap or CPU profiles with `go tool pprof`.

## Less Allocation Churn in Node and UI Hot Paths

The pending routing-drain path now skips unmatched addresses earlier and serializes queue-state JSON more compactly, reducing allocation churn during drain processing. The desktop service also caches independently changing halves of the UI snapshot, so frequent notifications no longer force a full snapshot rebuild every time.

Regression coverage was added around bad-hop hysteresis, resource usage reporting, pprof binding safety, orphaned metadata cleanup, routing drain behavior, and split UI snapshot caching.

---

# Что нового в v1.0.51

## Bad-hop recidivism теперь уходит в hold-down, а не бесконечно переписывает tombstones

Этот релиз добавляет bad-hops hysteresis поверх fast-invalidation пути в routing plane. Когда одна identity многократно присылает маршруты с невозможным числом hops, первые принятые bad-hop события по-прежнему инвалидируют затронутый transit claim и сохраняют sequence-number guard против resurrection. Если та же identity продолжает рецидивировать внутри observation window, она попадает в hold-down.

Во время hold-down дальнейшие bad-hop claims для этой identity отбрасываются до storage mutation: без tombstone rewrite, без snapshot rebuild и без per-event log amplification. Sane-hop claims продолжают проходить, поэтому loop-free recovery path остается доступным. Повторные hold-down engagements внутри recidivism window увеличивают penalty duration с ограничением сверху, чтобы не превращать это в permanent exile.

Практический эффект — более спокойная route table во время count-to-infinity loops или hostile/broken announce storms. Операторы видят срабатывания через счетчик `bad_hops_holdowns` в route summary, а существующий `fast_invalidations` остается сигналом именно по принятым invalidations.

## Memory и Uptime видны через RPC и Desktop UI

Ноды теперь отдают `getResourceUsage`: process memory totals, live heap allocation, uptime и sample timestamp в machine-readable и human-readable виде. Desktop console на вкладке Info показывает те же Memory и Uptime значения и обновляет их через node status monitor, не делая sampling на render path.

Это дает операторам дешевую первичную диагностику перед тяжелым profiling: можно смотреть, выходит ли live heap на plateau или продолжает расти со временем, и переходить к профилям только когда trend действительно похож на leak.

В релизе также исправлен monotonic growth в per-connection metadata maps, которые ключевались ephemeral inbound ports. Orphaned peer IDs, versions, builds и observed-IP history теперь вычищаются после reconnect churn, снижая memory pressure на долго работающих busy nodes.

## Opt-in loopback-only pprof server

Для глубокой диагностики `CORSA_PPROF_ADDR` теперь может запускать стандартный Go pprof HTTP server. Он выключен по умолчанию и отказывается bind'иться не на loopback address, поэтому heap, CPU и goroutine dumps доступны только для осознанной local diagnostics или remote debugging через SSH tunnel.

Новая debug-документация описывает, когда достаточно легкого `getResourceUsage` RPC, а когда нужно собирать heap или CPU profiles через `go tool pprof`.

## Меньше allocation churn в hot paths ноды и UI

Pending routing-drain path теперь раньше пропускает unmatched addresses и компактнее сериализует queue-state JSON, снижая allocation churn во время drain processing. Desktop service также кэширует независимо меняющиеся половины UI snapshot, поэтому частые notifications больше не заставляют каждый раз пересобирать полный snapshot.

Добавлена regression coverage вокруг bad-hop hysteresis, resource usage reporting, pprof binding safety, orphaned metadata cleanup, routing drain behavior и split UI snapshot caching.
