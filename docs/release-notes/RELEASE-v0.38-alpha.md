# What's New in v0.38-alpha

## Self-Identity Handshakes Are Now Rejected Everywhere

This release closes an important self-loopback class of failures. The node now rejects self-identity handshakes across all dial paths, not just simple address-level self-detection cases. If a remote `hello` or `welcome` carries the local node's own Ed25519 identity, the session is treated as a self-connection and is stopped before it can churn through retries or pollute peer state.

This makes the node more resilient against NAT reflection, peer-exchange echoes, fallback-port aliases, and other scenarios where the transport address may look different while the peer identity is actually our own.

## Advertise Convergence Now Uses Passive Observed-IP Learning

Advertise-address convergence has been softened from an eager reject-and-correct model into a more passive observed-IP learning path. Instead of leaning on `observed-address-mismatch` as a normal runtime correction mechanism, the node now treats legacy mismatch notices as weak hints and converges more conservatively based on observed-IP evidence.

That reduces unnecessary handshake disruption while still moving the node toward a truthful self-advertise address over time. It also prepares the protocol for cleaner post-legacy behavior as older mismatch-driven flows are phased out.

## Calmer UI and Status Updates During Failure Storms

The event-driven status path now suppresses no-op publication storms for aggregate status and version policy updates, while still republishing on heartbeat so lossy subscribers cannot remain stale forever.

In practice, this keeps Desktop and other ebus consumers much calmer during reconnect and failure storms, reduces avoidable UI freezes, and preserves liveness signals through fresh heartbeat timestamps instead of noisy duplicate state traffic.

## More Precise `peer-banned` Scope and Recovery

The `peer-banned` protocol is now applied with clearer scoping and more durable recovery behavior. Per-peer bans and IP-wide blacklists are persisted separately, notice delivery stays on the managed live connection path, and successful outbound authentication now clears only the scopes that the handshake can legitimately prove are healthy again.

This helps stop retry storms without over-suppressing healthy sibling peers behind the same IP, and it makes ban recovery more symmetrical and trustworthy after the remote side starts accepting us again.

---

# Что нового в v0.38-alpha

## Self-identity handshake теперь отклоняется везде

В этом релизе закрыт важный класс self-loopback сбоев. Теперь нода отклоняет self-identity handshake на всех dial paths, а не только в простых случаях address-level self-detection. Если удалённый `hello` или `welcome` несёт собственную Ed25519 identity локальной ноды, сессия трактуется как self-connection и останавливается до того, как она успеет уйти в retry churn или загрязнить peer state.

Это делает ноду устойчивее к NAT reflection, peer-exchange echoes, fallback-port aliases и другим сценариям, где transport address может выглядеть иначе, хотя peer identity на самом деле наша собственная.

## Advertise convergence теперь использует passive observed-IP learning

Логика converge для advertise-address стала мягче: вместо eager reject-and-correct модели она перешла к более пассивному observed-IP learning path. Вместо того чтобы полагаться на `observed-address-mismatch` как на обычный runtime-механизм коррекции, нода теперь рассматривает legacy mismatch notices как слабые подсказки и сходится осторожнее, опираясь на observed-IP evidence.

Это уменьшает лишние handshake-срывы, но при этом всё равно со временем ведёт ноду к правдивому self-advertise address. Заодно это подготавливает протокол к более чистому post-legacy поведению по мере ухода старых mismatch-driven flows.

## Более спокойные UI и status updates во время failure storms

Event-driven путь статуса теперь подавляет no-op publication storms для aggregate status и version policy, но при этом продолжает делать heartbeat-republish, чтобы lossy subscribers не оставались устаревшими навсегда.

На практике Desktop и другие ebus-consumers ведут себя заметно спокойнее во время reconnect и failure storms, снижается количество лишних UI-freeze, а сигналы живости сохраняются через свежие heartbeat timestamps вместо шумного дублирования состояния.

## Более точный scope и recovery для `peer-banned`

Протокол `peer-banned` теперь применяется с более чётким scope и более надёжным recovery-поведением. Per-peer bans и IP-wide blacklists сохраняются раздельно, доставка notices остаётся на managed live connection path, а успешная исходящая аутентификация очищает только те scope, здоровье которых handshake действительно может подтвердить.

Это помогает останавливать retry storms, не подавляя лишний раз здоровых sibling peers за одним и тем же IP, и делает восстановление после повторного допуска со стороны удалённого узла более симметричным и надёжным.
