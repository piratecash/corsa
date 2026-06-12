# What's New in v1.0.59

## Message Delivery Now Belongs to the Sender

This release changes the delivery model for direct messages. Reliability no longer depends on relay nodes acting like mailboxes for other people's traffic. Instead, the sender now owns delivery retry until `delivered` or `seen` confirmation arrives, while relays keep transit envelopes only for the lifetime of their forwarding work.

That makes delivery behavior easier to reason about and removes a fragile storage burden from transit peers. The new model also adds stronger duplicate handling, finite retry terminal states, `seen_ack` for end-to-end read confirmation, and cleanup of older backlog-era machinery that is no longer needed.

## Routing and Sessions Behave Better Under Pressure

Several fixes in this release make the mesh less self-destructive during congestion and reconnect churn. Best-effort frames are now dropped under backpressure instead of tearing down the whole session, disconnect-storm logic is more cause-aware, and direct-session re-establish now clears stale black-hole cooldown that could otherwise hide a freshly healthy direct path.

Forwarding-only relay behavior was also tightened with in-flight transit retention, hop-budget enforcement, exact duplicate suppression, and bounded memory for transit traffic. The practical result is calmer recovery, fewer self-inflicted reconnect cascades, and less chance that routing state stays unnecessarily pessimistic after a peer comes back.

## Traffic Capture and Desktop Metrics Are More Useful

Operators can now opt into startup-wide traffic recording with `CORSA_RECORD_ALL_TRAFFIC`, so capture begins before the first handshakes instead of only after a manual command. Auto-started capture sessions also emit paired lifecycle events more reliably, including terminal stop diagnostics.

On the desktop side, the live traffic chart no longer produces phantom spikes caused by client-side delta reconstruction from cached snapshots. Traffic history is now pulled from collector samples through a `since` cursor, making the graph track real sampled data instead of freeze-and-jump artifacts.

---

# Что нового в v1.0.59

## Доставка сообщений теперь принадлежит отправителю

Этот релиз меняет модель доставки direct messages. Надёжность больше не зависит от того, что relay-ноды работают как mailbox для чужого трафика. Вместо этого sender теперь сам владеет delivery retry до прихода подтверждения `delivered` или `seen`, а relays держат transit envelopes только на время собственной forwarding-операции.

Это делает поведение доставки проще для понимания и снимает хрупкую storage-нагрузку с transit peers. Новая модель также усиливает обработку duplicate messages, вводит конечные terminal states для retry, добавляет `seen_ack` для end-to-end read confirmation и убирает часть старой backlog-era machinery, которая больше не нужна.

## Routing и сессии ведут себя лучше под нагрузкой

Несколько правок в этом релизе делают mesh менее саморазрушительной во время congestion и reconnect churn. Best-effort frames теперь дропаются при backpressure вместо teardown всей session, логика `disconnect_storm` стала более cause-aware, а повторное установление direct session теперь снимает stale black-hole cooldown, который раньше мог прятать уже снова здоровый direct path.

Поведение forwarding-only relays тоже стало строже: добавлены in-flight transit retention, enforcement hop budget, точное подавление дублей и ограничение памяти для transit traffic. Практический эффект — более спокойное восстановление, меньше self-inflicted reconnect cascades и меньше шансов, что routing state останется излишне пессимистичным после возвращения peer.

## Traffic capture и desktop metrics стали полезнее

Операторы теперь могут включать запись всего трафика со старта через `CORSA_RECORD_ALL_TRAFFIC`, чтобы capture начинался ещё до первых handshakes, а не только после ручной команды. У auto-started capture sessions также надёжнее публикуются парные lifecycle events, включая terminal stop diagnostics.

Со стороны desktop live traffic chart больше не рисует phantom spikes, которые появлялись из-за client-side delta reconstruction поверх cached snapshots. История трафика теперь берётся из collector samples через `since` cursor, поэтому график отражает реальные sampled data, а не freeze-and-jump артефакты.
