# What's New in v0.41-alpha

## Routing v2 Rollout Becomes a Single Coherent Step

The routing stack now moves much further toward capability-aware announce v2 propagation.

The work includes the `routes_update` delta protocol, persisted and snapshotted peer capabilities for announce decisions, stricter legacy connect-time sync behavior, TTL refresh for unchanged learned routes, and a broader cleanup of routing integration into more focused pieces. Taken together, this makes route propagation more explicit, more incremental, and better aligned with what each peer can actually understand.

## File Transfer Routing and Authenticity Improve Together

File transfer got two substantial upgrades. First, file routes are now ranked more deliberately and the system exposes `explainFileRoute` diagnostics so operators can understand why a particular path was chosen. Second, `file_command` frames are now self-authenticating, which hardens the file-transfer control path against ambiguity and spoofing-style mistakes.

In practice, file delivery should be easier to reason about when debugging and safer when commands are relayed across the network.

## Better Operator Ergonomics for CLI and Desktop Console

The desktop console now supports command-history navigation, which makes repeated diagnostics and iterative command use much more comfortable.

On the CLI side, `corsa-cli` can now read RPC credentials from the environment, and the Docker runtime image bundles `corsa-cli` directly for easier in-container diagnostics.

## Safer Default Dial Behavior

The node now skips persisted private peers during auto-dial instead of treating them as normal dial candidates.

This reduces wasted connection attempts and helps the node avoid spending effort on stale or non-routable private endpoints that should not participate in normal automatic dialing.

---

# Что нового в v0.41-alpha

## Routing v2 оформляется в один связный шаг

Routing stack заметно продвинулся в сторону capability-aware announce v2 propagation.

Сюда входят `routes_update` delta protocol, персистентность и snapshot'ы peer capabilities для announce-решений, более строгий legacy connect-time sync, TTL refresh для неизменившихся learned routes и более чистое разделение routing integration на сфокусированные части. Вместе это делает распространение маршрутов более явным, более incremental и лучше согласованным с тем, что каждый peer реально умеет понимать.

## File transfer одновременно стал умнее по маршрутам и строже по аутентичности

File transfer получил два заметных улучшения. Во-первых, file routes теперь ранжируются более осмысленно, а система отдаёт diagnostics через `explainFileRoute`, чтобы оператор мог понять, почему был выбран тот или иной путь. Во-вторых, `file_command` frames теперь являются self-authenticating, что делает control path file transfer более защищённым от неоднозначности и ошибок в духе spoofing.

На практике это делает доставку файлов проще для отладки и безопаснее при relay-передаче команд через сеть.

## Более удобная работа оператора через CLI и desktop console

Desktop console теперь поддерживает навигацию по истории команд, поэтому повторяющиеся diagnostics и итеративная работа с командами стали заметно удобнее.

Со стороны CLI, `corsa-cli` теперь умеет читать RPC credentials из environment, а Docker runtime image сразу включает `corsa-cli`, что упрощает диагностику прямо внутри контейнера.

## Более безопасное поведение auto-dial по умолчанию

Нода теперь пропускает persisted private peers во время auto-dial, а не рассматривает их как обычных dial candidates.

Это уменьшает число бесполезных попыток подключения и помогает ноде не тратить усилия на stale или non-routable private endpoints, которые не должны участвовать в обычном автоматическом дозвоне.
