# What's New in v1.0.50

## Route-Flapping Storms Are Paced Instead of Fanning Out Immediately

This release hardens the routing announce loop against CPU pressure caused by rapid route churn. When many route updates arrive in a short burst, trigger-driven announce cycles are now paced and coalesced instead of running once per mutation.

The routing table still keeps the updated route state, and regular periodic announces continue to run as before. The pacing applies only to trigger-driven announce work, so route-flapping peers no longer force the node into tight announce loops while normal sync and recovery paths remain available.

The practical effect is lower CPU load during unstable routing periods, fewer self-amplifying route-update storms, and a calmer node when peers repeatedly connect, disconnect, or change transit paths.

## Toolchain and Dependency Refresh

The Go toolchain target was updated, and Gio, Fiber, and several `x/*` module dependencies were refreshed. This keeps the build base current while preserving the release focus on routing stability.

---

# Что нового в v1.0.50

## Route-flapping storms теперь сглаживаются, а не разгоняют announce fan-out

Этот релиз усиливает routing announce loop против CPU pressure, который возникает при быстром churn маршрутов. Когда много route updates приходит коротким burst'ом, trigger-driven announce cycles теперь paced и coalesced, вместо того чтобы запускаться на каждую mutation.

Routing table при этом продолжает хранить актуальное состояние маршрутов, а обычные periodic announces работают как раньше. Pacing применяется только к trigger-driven announce work, поэтому flapping peers больше не загоняют node в плотные announce loops, но normal sync и recovery paths остаются доступными.

Практический эффект — ниже CPU load во время нестабильных routing периодов, меньше риска self-amplifying route-update storms и более спокойное поведение node, когда peers многократно подключаются, отключаются или меняют transit paths.

## Обновление toolchain и зависимостей

Go toolchain target был обновлен, а Gio, Fiber и несколько `x/*` module dependencies были refreshed. Это держит build base актуальным, при этом главный фокус релиза остается на routing stability.
