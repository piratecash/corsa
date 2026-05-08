# What's New in v1.0.44

## Protocol Floor Moves to 14 and Legacy Compatibility Layers Fall Away

This release raises the minimum protocol version to 14 and uses that higher floor to remove several temporary compatibility bridges.

The biggest cleanup is in advertise-address handling: nodes no longer put their own external host on the wire, `observed-address-mismatch` is gone, and peer admission to announce/gossip paths now relies on the observed TCP source IP plus the advertised port. The legacy file-transfer protocol-version cutover is also removed, leaving capability negotiation as the remaining feature gate above the handshake floor.

## Routing Gets a RIB Cap and Atomic Snapshot Reads

Routing now gains a capped RIB model together with atomic snapshot reads for routing-facing queries and consumers.

This gives the node a tighter bound on routing-table growth while also making read-side access cheaper and more predictable. In practice, the routing layer becomes easier to observe and less likely to couple every reader to the full live mutation path.

## Version Metadata Is Now One Coherent `MAJOR.MINOR.BUILD` Model

Version metadata has been consolidated into a single `MAJOR.MINOR.BUILD` scheme. The old split between display version, wire-form version, and build number is gone; the node and tooling now derive their version identity from one clearer source of truth.

As part of that cleanup, `corsa-node` and `corsa-cli` now support `--version`, and the desktop info surface can show protocol/version information without the old duplicated formatting split.

## Release Artifacts Are Cleaner

Release builds now go further in stripping unnecessary path and symbol baggage from produced binaries.

Together with the earlier `trimpath` work, this makes release artifacts cleaner and more reproducible.

---

# Что нового в v1.0.44

## Минимальная версия протокола поднята до 14, а legacy-слои совместимости уходят

В этом релизе минимальная версия протокола поднимается до 14, и этот более высокий floor используется для удаления нескольких временных compatibility-слоёв.

Самая заметная очистка касается advertise-address логики: ноды больше не публикуют собственный внешний host на проводе, `observed-address-mismatch` удалён, а допуск peer'а в announce/gossip path теперь строится на наблюдаемом TCP source IP и advertised port. Одновременно убран legacy cutover в file transfer, поэтому capability negotiation остаётся главным feature-gate поверх handshake floor.

## Routing получает RIB cap и atomic snapshot reads

Routing теперь получает capped RIB model вместе с atomic snapshot reads для routing-ориентированных запросов и потребителей.

Это даёт ноде более жёсткую границу роста routing table и одновременно делает read-side доступ дешевле и предсказуемее. На практике routing layer становится проще для наблюдения и меньше привязывает каждого reader'а к полной live mutation path.

## Version metadata теперь сведены к единой схеме `MAJOR.MINOR.BUILD`

Version metadata теперь сведены к единой модели `MAJOR.MINOR.BUILD`. Старое разделение между display version, wire-form version и build number убрано; нода и инструменты теперь выводят свою version identity из одного, более понятного источника истины.

В рамках этой же очистки `corsa-node` и `corsa-cli` теперь поддерживают `--version`, а desktop info surface может показывать protocol/version information без прежнего дублирования форматов.

## Release artifacts стали чище

Release builds теперь сильнее очищают итоговые бинарники от лишней path- и symbol-нагрузки.

Вместе с предыдущей работой по `trimpath` это делает release artifacts чище и воспроизводимее.
