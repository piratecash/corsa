# What's New in v1.0.49

## Route-Flapping Protection No Longer Forces Reconnect Storms

This release reworks the node-side route-flapping protection so unstable routing behavior no longer turns into aggressive peer teardown. Instead of hard-dropping a peer and feeding a reconnect storm, the node now treats the peer more carefully: route state behind the peer can be quarantined, withdrawals are delayed long enough to cover the connection-manager retry budget, and stale transit paths are invalidated when a peer enters route quarantine.

The practical result is a routing plane that can absorb flapping routes without immediately amplifying the problem into repeated disconnect/reconnect cycles. Peers can remain known to the node while the questionable routes behind them are isolated, giving the mesh a calmer recovery path during unstable network periods.

## Routing Announce and Resync Paths Are Better Guarded

Routing announce handling now has stronger per-peer protection around chatty peers and request-resync traffic. `request_resync` is debounced and counted through the same inbound announce accounting path, reducing the risk that a noisy peer can force repeated full-sync work while bypassing the existing command limiter.

The release also adds regression coverage around route quarantine, withdrawal grace, poison handling, request-resync throttling, and reconnect-storm behavior.

## Bootstrap Seeds Refreshed

The built-in chainparams bootstrap seed list was refreshed from the masternode list so new nodes start with a more current peer set.

---

# Что нового в v1.0.49

## Route-flapping protection больше не разгоняет reconnect storms

Этот релиз перерабатывает node-side защиту от route flapping так, чтобы нестабильное routing поведение больше не превращалось в агрессивный peer teardown. Вместо жесткого drop peer'а, который подпитывает reconnect storm, node теперь обращается с ним аккуратнее: route state за peer'ом может уходить в quarantine, withdrawals откладываются достаточно долго, чтобы покрыть retry budget connection manager'а, а stale transit paths инвалидируются, когда peer попадает в route quarantine.

Практический эффект — routing plane может переживать flapping routes без немедленного усиления проблемы в repeated disconnect/reconnect cycles. Peer остается известен node, но сомнительные маршруты за ним изолируются, что дает mesh более спокойный recovery path во время нестабильных network периодов.

## Routing announce и resync paths лучше защищены

Routing announce handling получил более сильную per-peer защиту вокруг chatty peers и request-resync traffic. `request_resync` теперь debounced и проходит через тот же inbound announce accounting path, снижая риск, что шумный peer сможет запускать repeated full-sync work в обход существующего command limiter.

В релиз также добавлена regression coverage вокруг route quarantine, withdrawal grace, poison handling, request-resync throttling и reconnect-storm behavior.

## Bootstrap seeds обновлены

Встроенный chainparams bootstrap seed list был обновлен из masternode list, чтобы новые nodes стартовали с более актуальным peer set.
