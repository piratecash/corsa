# What's New in v0.40-alpha

## Service State Is Split Across Domain Mutexes

This release continues the lock-contention cleanup inside `node.Service` by splitting the old shared service state across domain-specific mutexes and documenting a canonical lock ordering contract.

The practical effect is that unrelated readers and writers are much less likely to serialize each other just because they happen to share the same broad service lock. This gives the node a stronger foundation for staying responsive under reconnect storms and other concurrency-heavy workloads.

## Sync Inbound Writes Now Respect Caller Context

NetCore sync inbound writes now honor the caller's `context.Context` instead of only waiting out the full internal flush timeout.

That means request-scoped timeouts, fanout cancellations, and shutdown cancellation can now interrupt an in-flight inbound sync send promptly, rather than forcing the caller to sit through the whole sync-write budget even after the work is no longer useful.

## Inbound Routing Key Resolution No Longer Risks Recursive `peerMu` Read Locking

The inbound routing path now avoids a recursive `peerMu.RLock` pattern during routing-key resolution.

This removes another stall class from inbound routing and makes the node safer under concurrent inbound traffic, especially when routing decisions and connection-state lookups are happening at the same time.

---

# Что нового в v0.40-alpha

## Service state разделён по domain mutexes

В этом релизе продолжается работа по снижению lock contention внутри `node.Service`: старое общее service state теперь заметно сильнее разделено по domain-specific mutexes, а для них задокументирован канонический контракт lock ordering.

Практический эффект в том, что несвязанные readers и writers теперь гораздо реже сериализуют друг друга только потому, что раньше делили один широкий service lock. Это даёт ноде более прочную основу для сохранения отзывчивости во время reconnect storms и других concurrency-heavy сценариев.

## Sync inbound writes теперь уважают caller context

Sync inbound writes в NetCore теперь учитывают `context.Context` вызывающей стороны, а не просто ждут полный внутренний flush timeout.

Это значит, что request-scoped timeouts, отмена fanout-операций и shutdown cancellation теперь могут быстрее прерывать in-flight inbound sync send, вместо того чтобы заставлять caller'а досиживать весь sync-write budget даже тогда, когда работа уже потеряла смысл.

## Inbound routing key resolution больше не рискует recursive `peerMu` read-lock

Inbound routing path теперь избегает recursive `peerMu.RLock` паттерна во время routing-key resolution.

Это убирает ещё один класс stall'ов из inbound routing и делает ноду безопаснее при конкурентном входящем трафике, особенно когда routing decisions и connection-state lookups происходят одновременно.
