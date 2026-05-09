# What's New in v1.0.45

## Routing Tightens Around RIB Caps and Phase 0 Cluster-Mesh Controls

This release continues the routing push from `v1.0.44` and turns it into a more opinionated default. The RIB cap is now active by default, the admission floor no longer depends on `ExpiresAt` in a way that can trigger re-announce thrashing, and the cluster-mesh Phase 0 work introduces operator-tunable cadence, an overload gate, and a cleaner split for v2 wire-frame behavior.

Taken together, these changes make routing behavior more controlled under pressure, reduce noisy route churn, and give operators better knobs for shaping announce behavior before the later cluster-mesh phases arrive.

## Routing Read Paths and Introspection Keep Improving

The routing stack keeps building on the recent atomic-snapshot direction. Read-side access and routing-facing inspection surfaces continue to get clearer and more explicit, while the surrounding tests and docs expand accordingly.

In practice, that means it becomes easier to reason about the live routing state without coupling every observer to the full mutation path.

---

# Что нового в v1.0.45

## Routing становится строже вокруг RIB cap и Phase 0 для cluster-mesh

Этот релиз продолжает routing-вектор из `v1.0.44` и делает его более выраженным поведением по умолчанию. RIB cap теперь активен по умолчанию, admission floor больше не зависит от `ExpiresAt` так, чтобы это вызывало re-announce thrashing, а cluster-mesh Phase 0 добавляет operator-tunable cadence, overload gate и более чистое разделение для v2 wire-frame поведения.

Вместе эти изменения делают routing-поведение более управляемым под нагрузкой, уменьшают шумный churn маршрутов и дают оператору лучшие ручки для настройки announce-поведения до прихода следующих фаз cluster-mesh.

## Read-path и introspection в routing продолжают улучшаться

Routing stack продолжает развивать направление с atomic snapshots. Read-side доступ и routing-ориентированные поверхности наблюдения становятся более явными и понятными, а тесты и документация вокруг них расширяются.

На практике это означает, что live routing state становится проще понимать без того, чтобы каждый observer был привязан к полному mutation path.
