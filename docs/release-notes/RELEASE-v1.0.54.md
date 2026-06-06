# What's New in v1.0.54

## Relay ConnID Lookup No Longer Clones Capabilities

This release removes another allocation source from the `sendMessageToPeer` relay hot path. Inbound connection lookups by overlay address now use a lightweight scanner that reads only the connection ID, address, direction, and tracked flag.

Previously, those lookups went through the full inbound-connection snapshot path, which also cloned each connection's capability slice. That capability data is useful for peer-health and diagnostics, but irrelevant when relay only needs "which tracked inbound connection ID belongs to this address?" Profiling showed those repeated clones still contributed GC churn after the broader queue and announce optimizations.

Behavior is unchanged: outbound connections remain invisible to inbound-only lookup paths, `inboundConnIDsLocked` still returns every inbound connection ID for an address, and `inboundConnIDForAddressLocked` still returns only a tracked inbound connection. The change is purely about avoiding unnecessary capability copies on a per-message relay path.

Regression coverage now pins the lightweight lookup behavior, including multiple inbound connections for the same address, outbound filtering, tracked-vs-untracked selection, and unknown-address handling.

---

# Что нового в v1.0.54

## Relay ConnID lookup больше не клонирует capabilities

Этот релиз убирает ещё один источник allocation churn из relay hot path `sendMessageToPeer`. Inbound connection lookups по overlay address теперь используют lightweight scanner, который читает только connection ID, address, direction и tracked flag.

Раньше эти lookups проходили через полный inbound-connection snapshot path, который заодно клонировал capability slice каждого connection. Эти capability данные нужны для peer-health и diagnostics, но не нужны relay-пути, которому требуется только ответ: "какой tracked inbound connection ID относится к этому address?" Профилирование показало, что повторные clones всё ещё давали GC churn после крупных queue/announce оптимизаций.

Поведение не меняется: outbound connections по-прежнему невидимы для inbound-only lookup paths, `inboundConnIDsLocked` всё ещё возвращает все inbound connection IDs для address, а `inboundConnIDForAddressLocked` всё ещё возвращает только tracked inbound connection. Изменение только в том, что per-message relay path больше не делает лишние capability copies.

Regression coverage теперь фиксирует lightweight lookup behavior: несколько inbound connections на один address, фильтрацию outbound, выбор tracked вместо untracked и unknown-address handling.
