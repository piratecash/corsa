# What's New in v1.0.47

## Cluster-Mesh Routing Advances Through Phases 1-4

This release pushes the routing stack through the next major cluster-mesh milestone. Instead of one isolated tweak, the work lands as a connected upgrade across Phases 1-4: per-uplink route storage, SeqNo flap capping, fast invalidation, route health tracking, probes, targeted queries, multipath failover, reputation-aware decisions, route sync, a more compact wire format, attested links, and poison reverse behavior.

The practical result is a routing plane that behaves better under churn, converges faster when paths change, and has more tools to keep route quality high as the mesh grows denser. It also lays down the protocol and internal-state groundwork for more scalable cluster-aware routing in future releases.

## Desktop UI Starts Cleaner and Fits Better by Default

The desktop app now has improved font fallback behavior, which makes the interface render more consistently across systems. Default window sizes were also reduced, so first launch feels less oversized and more comfortable on everyday displays.

## Release Builds Now Include Windows ARM64

Release packaging now includes Windows ARM64 builds, improving support for devices such as Microsoft Surface systems that run on ARM-based hardware.

---

# Что нового в v1.0.47

## Cluster-mesh routing проходит через Phases 1-4

Этот релиз заметно продвигает routing stack по линии cluster-mesh. Здесь это не набор разрозненных мелких правок, а связанное развитие сразу через Phases 1-4: per-uplink storage для маршрутов, SeqNo flap cap, fast invalidation, route health, probes, targeted queries, multipath failover, reputation-aware decisions, route sync, более compact wire format, attested links и poison reverse behavior.

Практический эффект в том, что routing plane лучше ведёт себя при churn, быстрее сходится при смене путей и получает больше механизмов для контроля качества маршрутов по мере роста и усложнения mesh. Одновременно релиз закладывает protocol и internal-state основу для следующих шагов в сторону более масштабируемого cluster-aware routing.

## Desktop UI стал аккуратнее и удобнее по умолчанию

В desktop app улучшен font fallback, поэтому интерфейс должен рендериться стабильнее на разных системах. Плюс уменьшены default window sizes, так что первое открытие приложения теперь выглядит более собранно и лучше подходит для обычных экранов.

## Release builds теперь включают Windows ARM64

В release packaging добавлены Windows ARM64 builds, что улучшает поддержку устройств вроде Microsoft Surface и других Windows-систем на ARM-железе.
