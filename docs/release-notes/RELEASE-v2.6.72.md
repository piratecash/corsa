# What's New in v2.6.72

## Messages With the Same ID Stay Separate

A wire message ID is no longer treated as globally unique inside the client. Local state now identifies a message by its topic, ID, sender and recipient, so two different conversations can reuse the same ID without sharing a database row, delivery status, reaction, attachment or UI item.

Receipts, retries, deletion, recovery and file-transfer state now follow that complete identity. Older ID-only operations continue to work when there is only one possible match and refuse an ambiguous request instead of changing the first matching message.

Storage schema 9 applies this identity to the message database while preserving existing row order, message bodies and wire values. The migration is forward-only: after it is applied, a build whose migration catalog ends at schema 8 cannot open the database.

New DM v1 messages can carry their topic, ID, sender and recipient inside both encrypted copies. Upgraded clients validate this reference when reading the message. Messages without a negotiated receipt signature requirement use the original encoding if the reference would push the body over the legacy relay's 64 KiB limit. Older releases can read both forms.

## Identity Recovery Preserves Existing State

If the SDK cannot read or validate an existing identity file, it now leaves the file untouched and reports the error instead of replacing it. Identity restore also preserves the signed record sequence, preventing a restored identity from publishing an older record than peers have already seen.

Older identity files containing only an Ed25519 key are upgraded by deriving the X25519 encryption key without changing the signing identity. Published encryption keys now travel with their authoritative signatures, and sealed messages with an invalid nonce length are rejected at the cryptographic boundary.

On macOS, Corsa also checks extended ACLs protecting secret files and their directory path. Startup refusals are shown directly to the user so that ownership or permission problems can be corrected without removing identity or pin state.

## HTTP RPC in the SDK Requires Credentials

SDK applications that enable HTTP RPC must now provide both a username and password. Construction fails with `sdk.ErrRPCAuthenticationRequired` if either credential is missing, and Corsa never creates an unauthenticated HTTP control listener. In-process commands remain available when HTTP RPC is disabled.

## Android Keeps Its Identity and Secure Session State

Android startup now supports the app's private identity storage and persists secure-session pins inside application storage. Startup failures are written directly to logcat, making identity, permission and pin-store problems visible even when the application cannot finish initialization.

## Desktop Contacts and Identity Dialogs Are Clearer

Identities with saved names now remain visible in the sidebar after restart even when they have no conversation history. Search results also account for the contacts already shown and update correctly after a contact is removed.

The user's own identity and contact identities now use the same fitted dialog with consistent QR, copy and action controls. Buttons, the chat header and the composer share a simpler layout across desktop and compact screens.

## DHT Preparation

This release contains extensive preparation for the future DHT, but it does not activate DHT routing or anonymous transport. Protocol version remains 31, and ordinary delivery continues to use the current routing path.

The S6 and D1 test infrastructure can now observe first-contact timing, durable pin writes, datagram and gossip queue growth, session writer queues, clocks, route traffic and resource use. These probes are opt-in and bounded so that measurements do not create another unbounded source of state. The D1 load stand also gains RouteWire accounting, recovery checks and larger Full512 fixtures for later admission testing.

Direct DM v2 envelopes remain disabled in ordinary builds until the mixed-version delivery policy is approved. Ordinary delivery still uses DM v1. New DMs sent after both peers negotiate signed receipts on a direct proved session require signatures on `delivered`, `seen`, and `seen_ack`. The requirement stays with the message, and a signed receipt can pass through a legacy relay in an encrypted control DM. Messages sent without the negotiated requirement can still use ordinary receipts. If a protected message exceeds the relay limit with its encrypted reference, sending fails with an error so the sender can shorten the message.

The preparation also exposed issues in existing paths, and their fixes are included here. Peer activity and route handling stay bound to the authenticated transport that produced them; inbound sessions retain verified endpoint metadata; receipt admission follows the same transport rules; rejected legacy sessions fully purge pending legacy routes; and stored DM v2 data is verified before any delivery retry is rebuilt. The full message-identity repair described above came from the same integration work and is active in this release.

## Bounded Diagnostic and Notice Storage

Traffic captures now enforce storage limits, reserve free disk space and create protected files. Notice storage also has explicit bounds and expiry instead of growing with the lifetime of the process. The build toolchain and dependencies have been updated to Go 1.27.4.

## Before Upgrading

Stop Corsa and create a verified, SQLite-consistent backup of the complete data directory. Keep the identity file, pin store, pin-store marker and database together when moving or restoring a node.

When changing ports with default paths, rename all port-specific files together, including `identity-<port>.json`, `identity-<port>.json.pin-store-marker.json` and `secure-sessions-<port>.json`. To keep the original filenames, set `CORSA_IDENTITY_PATH` and `CORSA_SECURE_SESSION_STORE_PATH` to the existing files and update the other data paths as needed. Keep the backup outside the old live store path. If startup reports `store_missing`, restore the corresponding store from the backup or correct the configured path. If it reports `two_stores`, identify the current store and move the other copy aside before restarting. Keep the marker with the identity throughout the migration. [Secure session migration and startup refusals](https://github.com/piratecash/corsa/blob/c391b0873e382beaec97496e6c6aa3e15a1b1f79/docs/protocol/session_v2.md#downgrade-protection)

Corsa refuses to start before opening a network listener if the pin store, its marker or their directories have unsafe ownership, permissions or links. For Docker, move the data together into a private subdirectory owned by the container user and mount it at the configured data path. The standard image uses UID/GID `10001:10001`. Use mode `0700` for the private directory and `0600` for the pin store and marker. The startup error gives the path, cause and process UID/GID.

TLS-first is on by default. Setting `CORSA_V2_TLS_FIRST=off` on the daemon or `NodeConfig.V2TLSFirst` to a pointer to false in the SDK starts with an identity-less probe only where legacy is permitted. Endpoints and identities already required to use the secure session still require it. Switching off TLS-first retains the stored downgrade protection.

Rollback requires a build that supports the secure session and can read the current pin store format. Keep the current identity, pin store and marker together. After schema 9 commits, a build ending at schema 8 also requires the database from the consistent backup made before upgrading.

SDK integrations using `RPC.Enabled=true` must add both `RPC.Username` and `RPC.Password` before moving to this version.

---

# Что нового в v2.6.72

## Сообщения с одинаковым ID больше не смешиваются

Wire message ID больше не считается глобально уникальным внутри клиента. Локальное состояние теперь определяет сообщение по topic, ID, отправителю и получателю. Поэтому разные переписки могут использовать одинаковый ID, не разделяя строку базы, статус доставки, реакцию, вложение или элемент интерфейса.

Подтверждения, повторные попытки, удаление, восстановление и состояние передачи файлов теперь используют полный набор полей сообщения. Старые операции только по ID продолжают работать при единственном возможном совпадении, а при неоднозначности отказывают вместо изменения первого найденного сообщения.

Storage schema 9 переносит это правило в базу сообщений, сохраняя существующий порядок строк, тела сообщений и wire values. Миграция однонаправленная: после её применения сборка, чей каталог миграций заканчивается на schema 8, не сможет открыть базу.

Новые сообщения DM v1 могут содержать тему, ID, отправителя и получателя внутри обеих зашифрованных копий. Обновлённые клиенты проверяют эту ссылку при чтении сообщения. Сообщения без согласованного требования подписи используют исходное кодирование, если привязка увеличивает тело сверх лимита старых реле в 64 КиБ. Старые версии читают оба варианта.

## Восстановление identity сохраняет прежнее состояние

Если SDK не может прочитать или проверить существующий identity-файл, теперь он оставляет файл без изменений и сообщает об ошибке вместо его замены. Восстановление identity также сохраняет номер последовательности подписанных записей, чтобы восстановленная identity не опубликовала запись старше той, которую peers уже видели.

Старые identity-файлы только с ключом Ed25519 обновляются с выводом X25519-ключа шифрования без изменения signing identity. Публикуемые ключи шифрования теперь передаются вместе с подтверждающими их подписями, а sealed messages с неверной длиной nonce отклоняются на криптографической границе.

На macOS Corsa также проверяет extended ACL, защищающие secret-файлы и путь к ним. Отказ запуска показывается пользователю напрямую, чтобы проблему владельца или прав можно было исправить без удаления identity или pin state.

## HTTP RPC в SDK требует аутентификации

SDK-приложения, включающие HTTP RPC, теперь должны задать имя пользователя и пароль. Если одного из полей нет, создание завершается ошибкой `sdk.ErrRPCAuthenticationRequired`, а неаутентифицированный HTTP control listener не запускается. При выключенном HTTP RPC команды внутри процесса остаются доступны.

## Android сохраняет identity и состояние secure sessions

Android теперь корректно запускается с identity в приватном хранилище приложения и сохраняет secure-session pins внутри app storage. Ошибки запуска напрямую записываются в logcat, поэтому проблемы identity, прав или pin store видны даже при незавершённой инициализации приложения.

## Контакты и identity-диалоги стали понятнее

Identities с сохранёнными именами теперь остаются в sidebar после перезапуска даже без истории переписки. Поиск также учитывает уже показанные контакты и корректно обновляется после удаления контакта.

Собственная identity пользователя и identities контактов теперь открываются в одном общем диалоге с согласованными QR, copy и action controls. Кнопки, header переписки и composer используют более простой layout на desktop и compact screens.

## Подготовка к DHT

В этот релиз вошёл большой объём подготовки к будущему DHT, но DHT-маршрутизация и анонимный транспорт не активированы. Protocol version остаётся 31, а обычная доставка продолжает использовать текущий маршрут.

Инфраструктура S6 и D1 теперь может измерять время первого контакта, durable-запись pins, рост очередей datagram и gossip, очереди session writers, часы, route traffic и потребление ресурсов. Эти probes включаются явно и имеют ограничения, чтобы сами измерения не создавали неограниченно растущее состояние. Load stand D1 также получил учёт RouteWire, проверки восстановления и увеличенные Full512 fixtures для будущей приёмки.

Direct DM v2 envelopes выключены в обычных сборках до утверждения политики доставки между версиями. Обычная доставка по-прежнему использует DM v1. Если обе стороны согласовали подписанные квитанции на прямой подтверждённой сессии, новые сообщения требуют подписи для `delivered`, `seen` и `seen_ack`. Требование сохраняется вместе с сообщением, а подписанная квитанция может пройти через старое реле внутри зашифрованного служебного DM. Сообщения без согласованного требования по-прежнему могут использовать обычные квитанции. Если защищённое сообщение с привязкой превышает лимит реле, отправитель получает ошибку и может сократить сообщение.

Подготовительные работы также выявили проблемы в существующих путях, и исправления включены в релиз. Peer activity и обработка маршрутов остаются привязаны к тому authenticated transport, который их создал; входящие sessions сохраняют проверенные endpoint metadata; допуск receipts следует тем же transport rules; после отказа legacy-сессии полностью удаляются ожидающие legacy routes; сохранённые данные DM v2 проверяются до восстановления повторной доставки. Описанное выше разделение сообщений по полной identity появилось в рамках той же интеграции и активно в этом релизе.

## Ограниченное хранение диагностики и уведомлений

Traffic captures теперь соблюдают лимиты хранилища, оставляют резерв свободного места и создают защищённые файлы. Хранилище notices также получило явные границы и срок хранения вместо роста на протяжении всей работы процесса. Toolchain и зависимости обновлены до Go 1.27.4.

## Перед обновлением

Остановите Corsa и создайте проверенную SQLite-consistent резервную копию всего каталога данных. При переносе или восстановлении ноды держите вместе identity-файл, pin store, pin-store marker и базу данных.

При смене порта с путями по умолчанию переименуйте вместе все файлы, в именах которых есть порт, включая `identity-<port>.json`, `identity-<port>.json.pin-store-marker.json` и `secure-sessions-<port>.json`. Чтобы сохранить прежние имена, укажите существующие файлы в `CORSA_IDENTITY_PATH` и `CORSA_SECURE_SESSION_STORE_PATH` и при необходимости обновите пути к остальным данным. Храните резервную копию вне прежнего рабочего пути хранилища. При ошибке `store_missing` восстановите соответствующее хранилище из резервной копии или исправьте настроенный путь. При ошибке `two_stores` определите актуальное хранилище и переместите вторую копию в другое место перед перезапуском. Сохраняйте маркер рядом с файлом identity на всех этапах переноса. [Перенос и отказы запуска защищённой сессии](https://github.com/piratecash/corsa/blob/c391b0873e382beaec97496e6c6aa3e15a1b1f79/docs/protocol/session_v2.md#защита-от-понижения)

Corsa отказывается запускаться до открытия сетевого порта, если у pin store, маркера или их каталогов небезопасный владелец, права доступа или ссылки. В Docker перенесите данные вместе в приватный подкаталог пользователя контейнера и смонтируйте его по настроенному пути данных. Стандартный образ использует UID/GID `10001:10001`. Установите права `0700` на приватный каталог и `0600` на pin store и маркер. Сообщение об отказе указывает путь, причину и UID/GID процесса.

Режим TLS-first включён по умолчанию. `CORSA_V2_TLS_FIRST=off` у демона или указатель на `false` в `NodeConfig.V2TLSFirst` у SDK включает пробное соединение без отправки identity только там, где разрешён старый протокол. Для адресов и identity с требованием защищённой сессии оно продолжает действовать. При выключении TLS-first сохранённая защита от понижения остаётся в силе.

Для отката нужна сборка с поддержкой защищённой сессии и текущего формата pin store. Сохраните текущие identity, pin store и маркер вместе. После применения schema 9 сборке с последней schema 8 также потребуется база из согласованной копии, сделанной до обновления.

SDK-интеграции с `RPC.Enabled=true` должны до обновления добавить `RPC.Username` и `RPC.Password`.
