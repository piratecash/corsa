# What's New in v2.0.65

## Identities Can Now Be Found, Verified and Shared

Corsa now uses the datagram plane introduced in the previous release for its first application protocol: `identity_lookup_v1`. Nodes publish signed, self-certifying identity records and can resolve an address on demand through `get_identity`, `post_identity` and `push_identity`, without asking relays to understand or store the record itself.

Lookup is integrated into the messaging flow. Opening an unresolved contact starts a bounded two-phase search, unresolved work can survive a restart, and a verified record may safely replace old contact keys by sequence number. Direct messages can also recover after a legitimate key rotation through a guarded notice-and-resend flow.

Sharing an identity is much easier as well. The new My Identity panel shows the full address and a `corsa:` QR code with Copy and Share actions, while versioned identity backup and restore preserve both private keys and the identity-record sequence.

## Durable State Moves onto a Shared, Migrated Database

Chat history and related durable state now live behind one shared SQLite owner instead of independently managed stores. A forward-only migration catalog with checksums verifies the database, adopts existing pre-versioned installations in place and applies every schema step transactionally, so upgrades preserve existing history without silently accepting an unexpected schema.

Database access is context-aware throughout the chat, service and SDK layers, and shutdown now waits for background writers before closing their consumers and the database. New files receive owner-only permissions, while application and owner markers are verified rather than silently repaired.

## Message and Conversation Deletion Now Follows Through

Deleting a message removes the local copy immediately and records the peer-side deletion as a durable intent that survives restarts and temporary disconnections. Conversation wipes use the same per-message mechanism, so partial progress is safe and outstanding requests continue until they settle.

Delivery is frozen while content is classified and removed, recently deleted IDs reject late relay echoes, and attachment cleanup is durable too. SQLite secure deletion plus coalesced WAL truncation removes content from the underlying database pages rather than only hiding its row. The UI shows how many deletions are still waiting for the peer and the final result for each message.

Contact deletion was hardened around the same rules: history and received files leave with the contact, incoming messages that race with deletion are handled consistently, and a failed delete leaves the draft, attachment, alias and contact-list position intact.

## Contact Presence and Sidebar State Are Consistent

Last-online transitions are now persisted from the node's own observations, including the loss of the last route and an incoming message over the contact's authenticated session. When no node-owned observation exists, the sidebar falls back to the contact's last incoming message rather than whichever message happens to end the conversation; timestamps claimed in the future are ignored.

Preview text, unread counts, dates and presence now converge to the same result regardless of whether history loading or live events arrive first. Reading a chat no longer loses messages or file transfers arriving at the same time, and a message received during conversation deletion is held and reopens the conversation cleanly afterwards.

## The Interface Gains Emoji and a Unified Modal System

The composer now includes an emoji picker with categories, English and Russian name search, and a recent-emoji row. It preserves keyboard focus and caret position while selecting, adapts to narrow screens and touch keyboards, and pairs with a clearer arrow send button that explains why sending is blocked.

The console now opens as a modal inside the main window instead of creating a second operating-system window. Its tabs, history and selection survive closing it; Escape, Back and outside clicks dismiss nested menus before the console itself. This also makes the console available on Android. Identity details, dropdowns and toolbar actions now share the same modal, popup and button components for more predictable keyboard, pointer and compact-screen behaviour.

## Runtime and Dependencies Are Current

The build toolchain is consolidated on Go 1.27, with Gio, Fiber, SQLite and the supporting dependency set updated to their final versions for this release.

---

# Что нового в v2.0.65

## Identity теперь можно находить, проверять и удобно передавать

Corsa впервые использует datagram plane из предыдущего релиза для прикладного протокола — `identity_lookup_v1`. Ноды публикуют подписанные self-certifying identity records и умеют находить адрес по запросу через `get_identity`, `post_identity` и `push_identity`, не заставляя relay понимать или хранить саму запись.

Lookup встроен в messaging flow. Открытие контакта без ключей запускает ограниченный двухфазный поиск, незавершённая работа переживает restart, а проверенная запись может безопасно заменить старые ключи контакта по sequence number. Direct messages также умеют восстанавливаться после легитимной ротации ключей через защищённый notice-and-resend flow.

Делиться identity тоже стало гораздо проще. Новая панель My Identity показывает полный адрес и `corsa:` QR-код, предлагает действия Copy и Share, а versioned backup/restore сохраняет оба приватных ключа и sequence identity record.

## Durable state переехал на общую базу с миграциями

История чатов и связанное durable state теперь работают через одного владельца общей SQLite database вместо независимо управляемых stores. Forward-only каталог миграций с checksums проверяет базу, принимает существующие pre-versioned установки без переноса данных и применяет каждый schema step транзакционно, поэтому upgrade сохраняет историю и не принимает неожиданную схему молча.

Работа с базой стала context-aware во всех chat, service и SDK слоях, а shutdown теперь дожидается background writers до остановки их consumers и закрытия database. Новые файлы получают owner-only permissions, а application и owner markers только проверяются и никогда не «исправляются» скрытно.

## Удаление сообщений и диалогов теперь доводится до конца

Удаление сообщения сразу убирает локальную копию и записывает peer-side deletion как durable intent, который переживает рестарты и временную недоступность контакта. Полная очистка диалога использует тот же per-message механизм, поэтому частичный прогресс безопасен, а оставшиеся запросы продолжаются до завершения.

Delivery замораживается на время классификации и удаления контента, недавно удалённые IDs блокируют поздние relay echoes, а очистка attachments тоже стала durable. SQLite secure deletion вместе с coalesced WAL truncation удаляет содержимое из underlying database pages, а не просто скрывает строку. UI показывает количество удалений, ожидающих peer, и итоговый статус каждого сообщения.

Удаление контакта усилено теми же гарантиями: вместе с ним уходят история и полученные файлы, входящие сообщения, совпавшие по времени с удалением, обрабатываются последовательно, а неудачная операция оставляет draft, attachment, alias и позицию контакта в списке нетронутыми.

## Presence контактов и sidebar state стали согласованными

Last-online transitions теперь сохраняются по собственным наблюдениям ноды, включая потерю последнего маршрута и входящее сообщение через аутентифицированную сессию контакта. Если наблюдений ноды ещё нет, sidebar использует последнее входящее сообщение от контакта, а не просто последнюю строку диалога; timestamps из будущего игнорируются.

Preview, unread counts, dates и presence теперь приходят к одному результату независимо от порядка загрузки истории и live events. Чтение открытого чата больше не теряет одновременно пришедшие сообщения или file transfers, а сообщение, совпавшее с удалением диалога, удерживается и затем корректно открывает разговор заново.

## В интерфейсе появились emoji и единая modal system

Composer получил emoji picker с категориями, поиском по английским и русским названиям и строкой недавних emoji. При выборе сохраняются keyboard focus и caret position, layout адаптируется к узким экранам и touch keyboard, а рядом появилась более понятная кнопка отправки со стрелкой, которая объясняет причину блокировки.

Console теперь открывается как modal внутри главного окна вместо отдельного окна операционной системы. Tabs, history и selection переживают закрытие; Escape, Back и клик снаружи сначала закрывают вложенные menus, затем саму console. Благодаря этому console доступна и на Android. Identity details, dropdowns и toolbar actions теперь используют общие modal, popup и button components с более предсказуемым поведением клавиатуры, pointer и compact screens.

## Runtime и зависимости обновлены

Build toolchain окончательно переведён на Go 1.27, а Gio, Fiber, SQLite и supporting dependencies обновлены до итоговых для этого релиза версий.
