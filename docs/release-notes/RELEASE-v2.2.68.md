# What's New in v2.2.68

## Messages Now Wait for Offline Recipients

A message written while its recipient is offline is now kept in a per-recipient queue instead of spending its retry budget against an empty route. Corsa sends queued messages one at a time when that contact becomes reachable, so a night away no longer causes a message to expire before the recipient returns. The queue survives restarts, including on relay-style nodes that do not accept incoming direct messages but still send their own.

The interface now distinguishes a message saved on this device from one that has actually reached the network. Messages that have not gone out remain visibly queued, and the conversation header separately counts messages waiting for delivery and requests waiting for peer-side deletion. A reload can add stronger delivery evidence but can no longer move a status backwards or make an open conversation lose messages arriving at the same time.

## Delivery Confirmations Are Safer and Durable

A delivery or read confirmation is now trusted only when it comes from the identity the message was addressed to. Confirmations carry the confirmer, message and state as one identity-aware fact, so another party that learns a message ID cannot forge a receipt that stops the real delivery.

Corsa records a confirmation before using it to stop retries or discard related state. If that database write fails, both sides continue the exchange and get another chance instead of forgetting a message whose outcome was never persisted. Protocol version 29 carries the more precise confirmation acknowledgement; older peers remain compatible and continue using their previous wire form.

## The Sidebar Shows the Message That Actually Arrived Last

Conversation previews and sidebar ordering now follow this node's local arrival sequence rather than timestamps supplied by message senders. A contact with a slow or fast clock can no longer leave a newer incoming reply hidden behind an older preview or hold a conversation at the top of the list.

Messages created within the same second also keep a stable order, preview updates from history and live events converge by one rule, and an undecryptable preview cannot overwrite the last valid one. Arrival sounds are deduplicated by message ID, so a replay does not ring twice and a message first discovered without a sound does not unexpectedly ring later.

## Arabic and Chinese Text Render Reliably

Corsa now bundles Noto Sans Arabic and Noto Sans CJK alongside its existing text and emoji fonts. Arabic translations no longer appear as empty boxes on Android devices whose system font sits outside the directories scanned by the renderer, and Chinese glyph coverage no longer depends on the device's font layout.

The application now supplies its interface direction explicitly. Translated Arabic UI follows right-to-left reading order, technical values such as identities, addresses, file names and commands remain left-to-right, and user-written text such as messages, drafts, aliases and search queries chooses direction from its own content. Editors receive the same direction for input events as for drawing, so cursor movement follows the text being edited.

## Contact Menus No Longer Open the Chat Behind Them

Tapping or long-pressing a contact's `⋯` menu no longer also activates the contact row underneath. This is especially important in the single-pane phone layout, where the accidental selection used to navigate into a conversation, clear its unread badge and send seen receipts even though the user had only opened the menu.

---

# Что нового в v2.2.68

## Сообщения теперь дожидаются получателя офлайн

Сообщение, написанное пока получатель офлайн, теперь остаётся в отдельной очереди этого контакта, а не расходует лимит повторных попыток на отсутствующий маршрут. Corsa отправляет накопившиеся сообщения по одному, когда контакт снова становится доступен, поэтому ночь вне сети больше не приводит к истечению доставки до возвращения получателя. Очередь переживает перезапуск, в том числе на relay-нодах, которые не принимают входящие direct messages, но отправляют собственные.

Интерфейс теперь различает сообщение, сохранённое на этом устройстве, и сообщение, которое действительно дошло до сети. Не ушедшие сообщения остаются явно в очереди, а заголовок переписки отдельно считает ожидающие доставки сообщения и запросы, ожидающие удаления у собеседника. Перезагрузка может дополнить статус более сильным подтверждением, но больше не откатывает его назад и не теряет сообщения, одновременно приходящие в открытый чат.

## Подтверждения доставки стали безопаснее и надёжнее

Подтверждение доставки или прочтения теперь считается достоверным, только если пришло от identity, которому было адресовано сообщение. Подтверждение связывает автора, сообщение и состояние в один identity-aware факт, поэтому посторонний, узнавший message ID, больше не может подделать receipt и остановить настоящую доставку.

Corsa сначала сохраняет подтверждение и лишь затем прекращает retry или удаляет связанное состояние. Если запись в database не удалась, обе стороны продолжают обмен и получают ещё одну попытку вместо того, чтобы забыть сообщение с несохранённым результатом. Protocol version 29 передаёт более точное acknowledgement подтверждения; старые peers остаются совместимыми и продолжают использовать прежний wire format.

## Sidebar показывает действительно последнее пришедшее сообщение

Preview переписки и порядок контактов в sidebar теперь определяются локальной последовательностью получения этой нодой, а не timestamp с часов отправителя. Контакт с отстающими или спешащими часами больше не может спрятать новый входящий ответ за старым preview или надолго удержать переписку наверху списка.

Сообщения, созданные в пределах одной секунды, тоже сохраняют стабильный порядок, а обновления preview из истории и live events сходятся по одному правилу. Не расшифрованная строка не заменяет последний корректный preview. Звук прихода дедуплицируется по message ID: replay не звонит второй раз, а сообщение, впервые обработанное без звука, не начинает неожиданно звонить позже.

## Арабский и китайский текст отображаются надёжно

Corsa теперь включает Noto Sans Arabic и Noto Sans CJK вместе с уже встроенными текстовыми и emoji-шрифтами. Арабский перевод больше не превращается в пустые квадраты на Android-устройствах, где системный шрифт лежит вне каталогов, сканируемых renderer, а покрытие китайских glyphs больше не зависит от расположения шрифтов на устройстве.

Приложение теперь явно задаёт направление интерфейса. Переведённый Arabic UI следует порядку справа налево, технические значения — identity, адреса, имена файлов и команды — остаются слева направо, а пользовательский текст, включая сообщения, drafts, aliases и поисковые запросы, выбирает направление по собственному содержимому. Editor получает одинаковое направление для input events и отрисовки, поэтому курсор движется в соответствии с редактируемым текстом.

## Меню контакта больше не открывает чат под собой

Тап или долгое нажатие на меню `⋯` контакта больше не активирует одновременно строку под кнопкой. Особенно это важно в single-pane layout телефона: раньше случайный выбор переходил в переписку, очищал unread badge и отправлял seen receipts, хотя пользователь только хотел открыть меню.
