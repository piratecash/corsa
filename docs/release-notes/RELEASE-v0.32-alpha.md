# What's New in v0.32-alpha

## Connection Manager and Redesigned Peers Panel

This release replaces the old outbound peer-maintenance flow with a dedicated event-driven `ConnectionManager`. Instead of a loosely coordinated mix of bootstrap attempts, pending sessions, and ad-hoc retries, outbound connectivity now runs through a fixed-slot lifecycle with explicit states such as `Queued`, `Dialing`, `Initializing`, `Active`, `Reconnecting`, and `Retry Wait`.

The desktop peers panel has been redesigned around that real lifecycle. Rather than showing a flatter connected/pending snapshot, it now groups peers by effective slot state and surfaces retry counters, generation data, connected-via addresses, and richer health metadata. This makes it much easier to understand why a peer is active, waiting, retrying, or stalled.

## Transport Split: TCP for P2P, RPC for Commands

The node now cleanly separates transport responsibilities. The TCP peer port serves the P2P wire protocol only, while application commands are handled through the RPC layer instead of being opportunistically accepted over peer transport.

This simplifies the mental model for the system, removes the old `connauth` path, and hardens command handling by making transport boundaries explicit. It also reduces protocol ambiguity and helps keep peer sessions focused on routing, delivery, and network state rather than mixed command traffic.

## CamelCase RPC Commands and Console/RPC Hardening

RPC commands have been normalized to camelCase naming, and the surrounding parser, alias layer, CLI, desktop console, and tests were updated accordingly. The console passthrough path was also tightened: TCP fallback was removed, missing commands were registered properly, and unknown frame types are now rejected instead of being tolerated inconsistently.

In practice, this gives the command layer a more coherent external shape and makes behavior more predictable across desktop, CLI, SDK, and server-side RPC integrations.

## Better Desktop File and Image Handling

Desktop chat cards are now much more useful for transferred media. Image attachments can show thumbnail previews directly in chat, and completed file cards now expose actions to reveal the file in the system file manager or open it with the default system handler.

These additions make the desktop app feel much more complete for day-to-day file exchange: received images are easier to scan visually, and completed downloads no longer require manual hunting through the downloads directory.

## Stronger File-Transfer Integrity and Re-download Recovery

File transfer internals were significantly hardened in this release. The transfer layer now includes serving-epoch replay defense, split-horizon relay protections, and generation-safe verification logic aimed at making re-downloads and stalled-transfer recovery more trustworthy.

This is mainly an under-the-hood reliability release, but it matters directly for users: interrupted or retried downloads are better protected from stale or mismatched transfer state, and the verification path is much stricter about making sure resumed delivery still refers to the correct file generation.

## Docker Default Data Volume Update

The default Docker data volume path now uses `.corsacore`, bringing container defaults in line with the project's newer application-data conventions on desktop and Unix-like systems.

This helps reduce confusion when moving between native runs, documentation, and containerized deployments.

---

# Что нового в v0.32-alpha

## Connection Manager и переработанная панель peers

В этом релизе старый flow поддержки outbound peers заменён на отдельный event-driven `ConnectionManager`. Вместо слабо связанной смеси bootstrap-подключений, pending sessions и разрозненных retry теперь исходящие соединения живут в фиксированной slot-модели с явными состояниями `Queued`, `Dialing`, `Initializing`, `Active`, `Reconnecting` и `Retry Wait`.

Desktop-панель peers тоже была перестроена вокруг этого реального lifecycle. Вместо более плоского представления connected/pending она теперь группирует peers по фактическому состоянию слота и показывает retry counters, generation metadata, connected-via addresses и более подробную health-информацию. Из-за этого стало намного проще понять, почему peer сейчас активен, ждёт, переподключается или застрял.

## Разделение transport-слоёв: TCP только для P2P, команды только через RPC

Node теперь гораздо чище разделяет ответственность транспортов. TCP peer-port обслуживает только P2P wire protocol, а прикладные команды идут через RPC-слой вместо того, чтобы по старой логике частично приниматься поверх peer transport.

Это упрощает общую модель системы, убирает старый путь `connauth` и делает command handling жёстче за счёт явных границ между транспортами. Заодно уменьшается протокольная неоднозначность: peer sessions теперь сосредоточены на routing, delivery и network state, а не на смешанном command traffic.

## CamelCase RPC-команды и ужесточение console/RPC passthrough

RPC-команды были нормализованы к camelCase naming, а вместе с этим обновлены parser, alias layer, CLI, desktop console и тесты. Путь console passthrough тоже стал строже: TCP fallback удалён, недостающие команды зарегистрированы явно, а неизвестные frame types теперь отвергаются вместо того, чтобы обрабатываться непоследовательно.

На практике это делает внешний command layer более цельным и предсказуемым для desktop, CLI, SDK и server-side RPC integration.

## Улучшения desktop для файлов и изображений

Карточки файлов в desktop стали заметно полезнее. Для image attachments теперь доступны thumbnail previews прямо в чате, а завершённые file cards получили действия для открытия файла через системный обработчик и показа его в файловом менеджере.

Из-за этого desktop-клиент ощущается гораздо более законченным в повседневном file exchange: полученные изображения проще просматривать визуально, а завершённые downloads больше не требуют ручного поиска нужного файла в директории загрузок.

## Усиление целостности file transfer и восстановления re-download

Внутренности file transfer в этом релизе были серьёзно усилены. Слой передачи теперь включает serving-epoch replay defense, split-horizon relay protections и generation-safe verification logic, направленную на то, чтобы повторные загрузки и восстановление stalled transfers были надёжнее.

В основном это under-the-hood улучшение, но оно напрямую важно для пользователей: interrupted или retried downloads теперь лучше защищены от stale/mismatched transfer state, а verification path стал строже проверять, что resumed delivery действительно относится к корректной генерации файла.

## Обновление default data volume в Docker

Default Docker data volume теперь использует путь `.corsacore`, что приводит container defaults в соответствие с более новой схемой application-data directories на desktop и Unix-подобных системах.

Это уменьшает путаницу при переходе между обычными локальными запусками, документацией и containerized deployment.
