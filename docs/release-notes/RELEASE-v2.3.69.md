# What's New in v2.3.69

## Older Messages Reach Contacts When They Return

Messages that have already been sent but not confirmed are now retried as soon as their recipient becomes reachable again. Corsa no longer makes a returning contact wait for the regular retry interval, while reconnect replays and accelerated retries are deduplicated so that one return triggers one extra delivery attempt.

Protocol version 30 also removes message age as a reason for a current relay to reject transit. During the mixed-version rollout, Corsa temporarily refreshes an old message's transit timestamp when handing it to a pre-v30 peer, allowing messages queued for more than a day to cross older nodes without changing the original message stored and shown to the user.

## Online Status Now Means the Contact Was Actually Seen

A remembered route is no longer enough to show a contact as online. Presence now distinguishes unknown, offline, probing and verified-online states, and the green indicator requires a recent identity-signed response from the contact itself. Older peers that cannot answer a presence probe retain a routing-based fallback, shown separately rather than as confirmed online.

Presence checks are limited to known contacts and use a small, stable set of first-hop guards to reduce unnecessary exposure. Presence can wake queued delivery when a contact returns, but it never blocks normal delivery. Read-only diagnostics are available through `fetch_presence` and `fetch_first_hop_guards`.

## Private Keys Stay Inside the Process

Private-key handling has received another layer of proactive security hardening. Sensitive material now has fewer paths beyond its intended security boundary, while regular identity operations continue to work as before.

Types that own private keys now refuse generic serialization and redact every standard formatted representation. The SDK runtime and RPC server discard startup secrets once initialization is complete, and automated checks inspect produced artifacts for raw and commonly encoded key material.

Identity backup and restore are now restricted to authenticated loopback RPC connections. They accept a backup name instead of an arbitrary filesystem path, keep files inside the managed backup directory and reject links. Secret-file replacement now uses unpredictable temporary names, owner-only permissions, directory confinement and durable writes before rename, without exposing local paths in errors.

## DHT Preparation

This release includes several groundwork changes for the future transition to DHT. Corsa can now record where a route came from and how its next hop was confirmed, creating a common foundation for the current mesh and the future DHT routing. Current message delivery remains unchanged; DHT routing and anonymous transport are not enabled yet.

Diagnostics now make it easier to see whether connected peers are ready for new protocol capabilities, why connections or deliveries fail, and how much state each subsystem keeps in memory. Tests also confirm that the same message received through different routes is not duplicated. Removing an obsolete full routing copy for every peer reduced retained state in a reference case with 5,000 identities and 32 peers from about 39.5 MB to about 7.4 KB.

The optional `CORSA_MAX_TOTAL_CONNECTIONS` setting adds one shared limit for active connections, connection attempts and short-lived auxiliary connections, while reserving capacity for outgoing connectivity. The limit remains disabled by default (`0`) until load testing establishes a safe production value; the existing connection limits continue to apply.

## Image Previews and the Composer Stay in Their Own Bounds

Asynchronous image decoding and cache reuse can no longer attach one message's preview to a neighbouring picture. Thumbnails remain associated with their original messages while conversations update, scroll or remove content.

The composer now includes the selected attachment chip in its measured size. Its card and border therefore expand with what they draw instead of letting attachments overflow, including in narrow, compact and touch layouts.

---

# Что нового в v2.3.69

## Старые сообщения доходят после возвращения контакта

Уже отправленные, но ещё не подтверждённые сообщения теперь повторно отправляются сразу, как только получатель снова становится доступен. Вернувшемуся контакту больше не приходится ждать обычного интервала retry, а replay при переподключении и ускоренные попытки дедуплицируются: одно возвращение вызывает одну дополнительную попытку доставки.

Protocol version 30 также исключает возраст сообщения из причин, по которым текущий relay может отклонить transit. Пока в сети одновременно работают разные версии, Corsa временно обновляет transit timestamp старого сообщения при передаче peer до v30. Благодаря этому сообщения, проведшие в очереди больше суток, проходят через старые ноды, а исходное сообщение, сохранённое и показанное пользователю, не изменяется.

## Статус «онлайн» теперь означает, что контакт действительно на связи

Сохранённого маршрута больше недостаточно, чтобы показать контакт онлайн. Presence различает состояния unknown, offline, probing и подтверждённый online, а зелёный индикатор появляется только после недавнего ответа, подписанного самим identity контакта. Для старых peers, которые ещё не умеют отвечать на presence probe, сохраняется fallback по маршруту, но он отображается отдельно и не считается подтверждённым онлайном.

Presence-проверки ограничены знакомыми контактами и проходят через небольшой стабильный набор first-hop guards, чтобы не раскрывать лишнюю информацию. Presence может разбудить отложенную доставку после возвращения контакта, но никогда не блокирует обычную отправку. Read-only диагностика доступна через `fetch_presence` и `fetch_first_hop_guards`.

## Приватные ключи остаются внутри процесса

Работа с приватными ключами получила дополнительный уровень плановой защиты. Для чувствительных данных сокращено количество путей за пределы предназначенной для них границы безопасности, при этом обычные операции с identity продолжают работать как прежде.

Типы, владеющие приватными ключами, теперь запрещают generic serialization и скрывают значение при любом стандартном форматировании. SDK runtime и RPC server удаляют startup secrets после завершения инициализации, а автоматические проверки ищут в созданных артефактах ключи в сыром виде и распространённых кодировках.

Backup и restore identity теперь доступны только через authenticated loopback RPC. Команды принимают имя backup вместо произвольного пути, сохраняют файлы только в управляемом каталоге и отклоняют ссылки. Замена secret-файла использует непредсказуемое временное имя, owner-only permissions, привязку к каталогу и durable write перед rename, не раскрывая локальные пути в ошибках.

## Подготовка к DHT

В этот релиз вошла часть подготовительных изменений для будущего перехода на DHT. Corsa теперь сохраняет, откуда получен маршрут и чем подтверждён его следующий узел. Это создаёт общую основу для нынешней mesh-маршрутизации и будущей DHT-маршрутизации. Текущий механизм доставки сообщений не изменился; DHT-маршрутизация и анонимный транспорт пока не включены.

Диагностика теперь помогает понять, готовы ли подключённые peers к новым возможностям протокола, почему не удалось соединение или доставка и сколько памяти занимает каждая подсистема. Тесты также подтверждают, что одно сообщение, пришедшее по разным маршрутам, не дублируется. Удаление устаревшей полной копии маршрутов для каждого peer уменьшило объём удерживаемых данных в контрольном сценарии с 5 000 identities и 32 peers примерно с 39,5 МБ до 7,4 КБ.

Опциональная настройка `CORSA_MAX_TOTAL_CONNECTIONS` добавляет единый лимит для активных соединений, попыток подключения и короткоживущих вспомогательных соединений, сохраняя резерв для исходящих подключений. По умолчанию лимит отключён (`0`) до определения безопасного production-значения нагрузочными тестами; существующие лимиты соединений продолжают действовать.

## Preview изображений и composer остаются в своих границах

Асинхронное декодирование изображений и повторное использование cache больше не могут привязать preview одного сообщения к соседней картинке. Thumbnails остаются связаны со своими исходными сообщениями при обновлении переписки, прокрутке и удалении содержимого.

Composer теперь учитывает chip выбранного вложения при расчёте размера. Поэтому card и border расширяются вместе с содержимым, а вложения больше не выходят за их границы, в том числе в узких, compact и touch layouts.
