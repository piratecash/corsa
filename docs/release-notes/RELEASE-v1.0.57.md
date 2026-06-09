# What's New in v1.0.57

## Node and Service Hot Paths Now Waste Less Work

This release puts a lot of attention on steady-state efficiency. The node cuts allocation churn on drain, gossip, inbound parse, and metrics paths, while the service side avoids unnecessary full `NodeStatus` rebuilds and deep copies on lightweight updates.

The practical result is lower background overhead during normal mesh activity, less snapshot churn when nothing important changed, and a smoother runtime profile for long-lived nodes under constant routing traffic.

## Routing Health, Quarantine, and Remote Bans Behave More Precisely

Routing protections were tightened so they react more proportionally to real problems. Route quarantine is now reason-aware, `chatty_routes` focuses on delta churn instead of punishing baseline sync traffic, and health probing is softer and more budgeted under large meshes.

Remote-ban behavior is also narrower and safer: single offenders no longer immediately poison an entire shared IP, while scoped IP-wide bans still engage when multiple distinct offenders behind the same egress keep misbehaving. Together, these changes make routing recovery less destructive and reduce the chance of collateral damage during large-mesh instability.

## Operators Get Better Runtime Controls and Diagnostics

This release adds a few useful operational improvements: runtime-only `addpeer` can now target local addresses, `CORSA_MEM_LIMIT_BYTES` can opt into a soft Go memory limit, and the debug server can expose opt-in contention profiling for mutex and block analysis.

That gives operators more ways to test local topologies, bound memory growth when needed, and investigate lock contention during heavier routing storms.

---

# Что нового в v1.0.57

## Hot paths у node и service теперь тратят меньше лишней работы

В этом релизе много внимания ушло в steady-state efficiency. Node уменьшает allocation churn на путях `drain`, `gossip`, inbound parse и metrics, а service больше не делает лишние полные rebuild/deep-copy для `NodeStatus` на лёгких обновлениях.

Практический эффект — меньше фонового overhead во время обычной mesh-активности, меньше snapshot churn там, где по сути ничего важного не изменилось, и более ровный runtime profile у long-lived nodes под постоянным routing traffic.

## Routing health, quarantine и remote bans стали точнее

Routing protections теперь реагируют более пропорционально реальной проблеме. Route quarantine стала reason-aware, `chatty_routes` теперь смотрит именно на delta churn вместо наказания baseline sync traffic, а health probing стал мягче и ограниченнее по бюджету в больших mesh.

Поведение remote bans тоже стало уже и безопаснее: одиночный offender больше не отравляет сразу весь shared IP, но scoped IP-wide ban всё ещё включается, если несколько разных offenders за одним и тем же egress продолжают слать проблемный трафик. В сумме это делает routing recovery менее разрушительным и уменьшает collateral damage во время нестабильности в больших mesh.

## У операторов стало больше runtime-контроля и диагностики

В релиз вошли и несколько полезных operational улучшений: runtime-only `addpeer` теперь может указывать local addresses, `CORSA_MEM_LIMIT_BYTES` позволяет опционально включить soft memory limit для Go, а debug server умеет отдавать opt-in contention profiling для mutex/block анализа.

Это даёт больше способов тестировать локальные topology, ограничивать рост памяти при необходимости и разбирать lock contention во время более тяжёлых routing storms.
