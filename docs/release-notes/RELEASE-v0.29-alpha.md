# What's New in v0.29-alpha

## File Transfer

Corsa now has its first full file-transfer stack. Sending a file starts with a regular DM command, `file_announce`, which travels through the normal encrypted DM pipeline, is stored in chatlog, and renders as a file card in the desktop UI. The actual transfer then switches to a dedicated file-command transport with `chunk_request`, `chunk_response`, `file_downloaded`, and `file_downloaded_ack`. These commands are not stored as chat messages and do not use gossip fallback; instead they travel through best-route delivery with explicit capability gating (`file_transfer_v1`).

The release adds sender and receiver transfer state machines, chunk retry and resume handling, waiting-route recovery, persistence, RPC endpoints for starting and monitoring transfers, and desktop attachment flow for preparing files in the transmit store. The transfer path is content-addressed and integrity-checked with SHA-256, while the receiver verifies the completed file before sending `file_downloaded`.

## Network Core and Transport Hardening

Inbound peer connections are now owned by a dedicated `PeerConn` abstraction. `PeerConn` becomes the single source of truth for inbound connection state and enforces the single-writer rule by routing writes through a dedicated writer goroutine with `Send` and `SendSync`. This reduces state fragmentation and closes a whole class of bugs caused by bypassing the managed write path.

Transport security is also substantially stronger in this release. File-command payloads now use X25519 + AES-256-GCM sealed-box encryption with domain-separated KDF labels. The new file-command frame format adds nonce binding, ed25519 signatures, freshness checks, TTL / MaxTTL enforcement, and an anti-replay cache with atomic commit. Inbound TCP protection is stricter as well: the node now rate-limits both connection attempts and per-connection command floods, returning `rate-limited` when the command bucket is exhausted and feeding that into ban scoring for abusive peers.

## Safer File Storage

The file-transfer implementation hardens local file handling on both transmit and download paths. File names are sanitised, directory boundaries are enforced, symlink and TOCTOU races are checked explicitly, file copies use atomic temp-file-to-rename flows, and completed downloads are resolved deterministically with content-aware naming. This prevents path traversal and symlink escape issues while keeping deduplication and restart recovery predictable.

## DMRouter Race Fixes

Several race conditions in `DMRouter` have been fixed. Fast peer switching could previously overwrite the wrong conversation cache, rebuild previews from another peer's message list, increment unread counts incorrectly, or leave reply-clear state behind after reset. The router now guards stale goroutines by verifying that the active peer still matches the peer the background work was started for, and same-peer selection has been reworked so no-op selection no longer mutates state or launches unnecessary goroutines. This makes unread badges, cached conversations, and preview updates much more stable under rapid UI interaction.

## Recipient Header in Composer

The desktop compose area now shows the selected recipient's contact name and identity in the message header line. This gives clearer context when chatting with peers whose display names are similar, and makes the current conversation target easier to verify before sending.

---

# Что нового в v0.29-alpha

## Передача файлов

В Corsa появился первый полноценный стек передачи файлов. Отправка файла начинается с обычной DM-команды `file_announce`, которая проходит через стандартный зашифрованный DM pipeline, сохраняется в chatlog и отображается в desktop UI как карточка файла. Сама передача затем переходит на отдельный транспорт файловых команд с `chunk_request`, `chunk_response`, `file_downloaded` и `file_downloaded_ack`. Эти команды не сохраняются как chat-сообщения и не используют gossip fallback; вместо этого они идут по best-route доставке с явной capability-проверкой (`file_transfer_v1`).

Релиз добавляет state machine для отправителя и получателя, retry и resume чанков, восстановление после `waiting_route`, persistence, RPC-эндпоинты для запуска и наблюдения за transfer, а также desktop flow для подготовки файлов в transmit-хранилище. Путь передачи использует content-addressed хранение и SHA-256 для проверки целостности, а получатель верифицирует собранный файл перед отправкой `file_downloaded`.

## Сетевое ядро и усиление транспорта

Входящие peer-соединения теперь управляются отдельной абстракцией `PeerConn`. `PeerConn` становится единственным источником истины для состояния inbound-соединения и жёстко соблюдает single-writer правило, направляя запись через выделенную writer-goroutine с `Send` и `SendSync`. Это уменьшает фрагментацию состояния и закрывает целый класс ошибок, возникавших при обходе управляемого write-path.

Transport security в этом релизе тоже существенно усилен. Payload файловых команд теперь использует sealed-box шифрование X25519 + AES-256-GCM с domain-separated KDF labels. Новый формат file-command frame добавляет nonce binding, ed25519-подписи, freshness checks, проверку TTL / MaxTTL и anti-replay cache с atomic commit. Защита входящего TCP также стала строже: нода теперь rate-limit'ит и попытки подключения, и flood команд на одном соединении, возвращает `rate-limited` при исчерпании command bucket и учитывает это в ban scoring для злоупотребляющих пиров.

## Более безопасное файловое хранение

Реализация file transfer усиливает локальную работу с файлами как на transmit-, так и на download-path. Имена файлов санитизируются, границы директорий строго проверяются, symlink- и TOCTOU-гонки отдельно детектируются, копирование использует атомарную схему temp-file → rename, а готовые скачивания получают детерминированное content-aware именование. Это защищает от path traversal и symlink escape проблем, сохраняя при этом предсказуемую дедупликацию и восстановление после перезапуска.

## Исправления гонок в DMRouter

Исправлено несколько race condition в `DMRouter`. При быстром переключении между peers раньше можно было перезаписать не тот conversation cache, пересобрать preview из списка сообщений другого peer, неправильно увеличить unread-счётчик или оставить неочищенное состояние reply после reset. Теперь роутер защищается от stale goroutine, проверяя, что active peer всё ещё совпадает с тем peer, для которого была запущена фоновая работа, а логика same-peer selection переработана так, чтобы no-op выбор больше не мутировал состояние и не запускал лишние goroutine. Это заметно стабилизирует unread badges, conversation cache и preview update при резком UI-взаимодействии.

## Заголовок получателя в composer

В desktop composer теперь показываются имя контакта и identity выбранного получателя в верхней строке ввода сообщения. Это даёт более ясный контекст при работе с peers с похожими display name и упрощает визуальную проверку адресата перед отправкой.
