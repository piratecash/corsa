# What's New in v1.0.48

## Connection-Manager Setup-Failure Storm Is Closed at the Source

This release focuses on one node-side stability fix: closing the `cm_session_setup_failed` storm and hardening the connection manager around repeated setup failures.

On the peer side, handshake reply traffic such as `subscribed`, `peers`, `contacts`, `auth_ok`, and reverse `subscribe_inbox` no longer trips slow-peer eviction under outbound-buffer pressure. On our side, the node now adds a per-peer setup-failure cooldown after repeated failures and introduces a global dial pacer so reconnect bursts are spread over time instead of spawning as a thundering herd.

The practical effect is a calmer reconnect path, less risk of cascading retry storms, and better resilience when bootstrap or unstable peers start failing during session setup.

---

# Что нового в v1.0.48

## Storm вокруг connection-manager setup failures закрыт у источника

Этот релиз сфокусирован на одной node-side stability правке: закрытии `cm_session_setup_failed` storm и усилении connection manager вокруг повторяющихся setup failures.

На peer side handshake reply traffic вроде `subscribed`, `peers`, `contacts`, `auth_ok` и reverse `subscribe_inbox` больше не провоцирует slow-peer eviction при давлении на outbound buffer. На нашей стороне node теперь вводит per-peer setup-failure cooldown после серии неудач и global dial pacer, чтобы reconnect bursts размазывались по времени, а не запускались как thundering herd.

Практический эффект — более спокойный reconnect path, меньше риска каскадных retry storms и лучшая устойчивость в ситуациях, когда bootstrap peers или нестабильные узлы начинают сыпаться на этапе session setup.
