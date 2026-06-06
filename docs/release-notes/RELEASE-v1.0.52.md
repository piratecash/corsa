# What's New in v1.0.52

## Resource Usage Now Explains Where Memory Went

`getResourceUsage` now reports more than the old process totals. In addition to `mem_sys`, live heap allocation, uptime, and the sample timestamp, the payload includes Go heap in-use / idle / released bytes, GC metadata memory, cgroup memory limit + usage, and the live peer connection count.

These fields make first-pass memory triage much clearer. Operators can separate live heap growth from reclaimed heap that the Go runtime is still holding, see how much memory GC metadata accounts for, compare process-visible memory with the container's cgroup headroom, and check whether footprint growth tracks the number of active peer connections.

The cgroup numbers are intentionally documented as container-oriented best-effort: they are accurate for Docker/k8s private cgroup namespaces, where the mounted cgroup root is the container's own cgroup. On bare hosts, systemd services, or non-private cgroup namespaces they describe the mounted hierarchy root instead of the Corsa process itself.

The expanded field set is carried through both the public RPC path and the embedded desktop local-frame path, so future dashboards and UI consumers can rely on the same resource snapshot. The system and debug docs now describe the new fields in English and Russian.

## Less Allocation Churn in Node Hot Paths

Profiling on a large mesh showed that high RSS was dominated by allocation churn rather than retained live objects. This release reduces churn in three hot paths:

- queue-state persistence now marshals through a reused buffer and coalesces sustained route churn with a 2-second write ceiling, while still bypassing the ceiling for final and synchronous flushes;
- routing announce chunking now uses a conservative running size estimate instead of re-marshalling the growing candidate on every entry, keeping chunk boundaries safe without the O(n²) allocation pattern;
- peer-health snapshots now skip the periodic 500 ms rebuild when there has been no recent reader activity, while startup priming and peer-state-change rebuilds still happen eagerly.

Together these changes lower marshal pressure, GC work, and runtime page retention on busy or headless nodes, especially during route churn and large announce fan-out.

Regression coverage was added for the expanded resource usage contract, cgroup parsing and unlimited handling, active connection counting, embedded resource frame mapping, queue-state persistence coalescing, announce chunk sizing, and peer-health snapshot gating.

---

# Что нового в v1.0.52

## Resource usage теперь объясняет, куда ушла память

`getResourceUsage` теперь отдаёт не только старые process totals. Кроме `mem_sys`, live heap allocation, uptime и sample timestamp, payload включает Go heap in-use / idle / released bytes, память metadata GC, cgroup memory limit + usage и число живых peer-соединений.

Это делает первичную диагностику памяти намного понятнее. Оператор может отделить рост live heap от reclaimed heap, которую Go runtime ещё держит у себя, увидеть вклад GC metadata, сравнить process-visible memory с запасом контейнерной cgroup и проверить, растёт ли footprint вместе с числом активных соединений.

Семантика cgroup-полей описана как container-oriented best-effort: цифры точны для Docker/k8s private cgroup namespaces, где mounted cgroup root является cgroup самого контейнера. На bare host, systemd service или non-private cgroup namespace они описывают root смонтированной иерархии, а не сам Corsa process.

Расширенный набор полей проходит и через public RPC path, и через embedded desktop local-frame path, поэтому будущие dashboards и UI consumers могут опираться на один и тот же resource snapshot. System/debug docs обновлены на английском и русском.

## Меньше allocation churn в hot paths ноды

Профилирование на большой mesh показало, что высокий RSS в основном создавался allocation churn, а не удержанными live objects. В этом релизе снижена нагрузка в трёх hot paths:

- queue-state persistence теперь marshal'ит через переиспользуемый buffer и coalesces sustained route churn через 2-second write ceiling, но final и synchronous flushes всё равно обходят ceiling;
- routing announce chunking теперь использует conservative running size estimate вместо повторного marshal growing candidate на каждой entry, сохраняя безопасные chunk boundaries без O(n²) allocation pattern;
- peer-health snapshots теперь пропускают periodic 500 ms rebuild, если давно не было reader activity, при этом startup priming и peer-state-change rebuilds остаются eager.

Вместе эти изменения снижают marshal pressure, GC work и runtime page retention на busy/headless nodes, особенно во время route churn и большого announce fan-out.

Добавлена regression coverage для expanded resource usage contract, cgroup parsing и unlimited handling, active connection counting, embedded resource frame mapping, queue-state persistence coalescing, announce chunk sizing и peer-health snapshot gating.
