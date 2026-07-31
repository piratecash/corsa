# What's New in v1.0.63

## Messaging Feels Safer and More Forgiving

The desktop composer now keeps an independent draft for each conversation, including the selected attachment, so switching between contacts no longer discards unfinished work. Failed text and file sends are shown as a clear “not sent” banner with Retry and Dismiss actions instead of being pushed back into the composer.

Reply handling is more resilient too: if the original message is deleted while a reply is being composed, Corsa drops the stale quote and sends the new message normally instead of failing it. `Shift+Enter` now inserts a newline, while `Enter` keeps sending the message.

## Windows Tablets Get First-Class Touch Support

On Windows tablets, tapping an input now opens the touch keyboard automatically. When the keyboard is docked, the app adjusts the layout so the composer and console input remain visible, and hides an app-opened keyboard again when input focus is left.

Message and contact actions are also easier to reach without a mouse. Context menus can now be opened with a 500 ms long-press or the visible `⋯` button in addition to right-click, with the same reply, copy, delete, alias and contact actions available across input methods.

## Replies Are Easier to Recognize and the App Is Easier to Spot

Replies to images now include a compact thumbnail both inside the message quote and in the composer reply banner, making visual conversations much easier to follow at a glance.

Corsa also gains a proper cross-platform application icon, embedded in the desktop app and carried through the Windows, macOS and Linux release packaging.

## Long-Running Nodes Reclaim More Residual State

A focused memory-leak pass closes several places where state from departed peers, one-off identities, failed deliveries and expired rate-limit records could remain allocated indefinitely. Peer eviction now cleans up its dependent queues, votes and metadata more completely, key-knowledge maps follow the bounded identity cache, and stale routing-query and RPC-auth limiter entries are swept automatically.

Trusted contacts remain pinned while they are trusted, so bounding transient identity knowledge does not sacrifice the keys required to verify their messages. Together, these fixes keep long-lived and high-churn nodes from quietly accumulating unreachable state over time.

---

# Что нового в v1.0.63

## Работа с сообщениями стала надёжнее и спокойнее

Desktop composer теперь хранит отдельный черновик для каждого диалога, включая выбранное вложение, поэтому переключение между контактами больше не уничтожает незаконченный текст. Неудачно отправленные текстовые сообщения и файлы показываются в понятном баннере «не отправлено» с действиями «Повторить» и «Закрыть», а не возвращаются обратно в composer.

Ответы тоже стали устойчивее: если исходное сообщение удалили, пока пользователь набирал ответ, Corsa убирает устаревшую цитату и отправляет новое сообщение как обычное вместо ошибки. `Shift+Enter` теперь вставляет новую строку, а `Enter` по-прежнему отправляет сообщение.

## Windows-планшеты получили полноценную touch-поддержку

На Windows-планшетах тап по полю ввода теперь автоматически открывает экранную клавиатуру. Когда клавиатура пристыкована, приложение перестраивает layout так, чтобы composer и ввод в консоли оставались видимыми, а после ухода фокуса из полей ввода закрывает клавиатуру, если открыло её само.

Действия с сообщениями и контактами теперь проще вызывать без мыши. Помимо правого клика, context menu открывается долгим нажатием на 500 мс или видимой кнопкой `⋯`; reply, copy, delete, alias и остальные действия доступны одинаково при любом способе ввода.

## Ответы стали нагляднее, а приложение — заметнее

В ответах на изображения теперь показывается компактная миниатюра — и внутри цитаты сообщения, и в reply banner композера. Визуальные диалоги стало проще читать с первого взгляда.

У Corsa также появилась полноценная cross-platform иконка приложения: она встроена в desktop app и проходит через release packaging для Windows, macOS и Linux.

## Long-running nodes лучше освобождают остаточное состояние

Отдельный memory-leak pass закрыл несколько мест, где данные от ушедших peers, одноразовых identities, неудачных доставок и истёкших rate-limit записей могли оставаться в памяти бесконечно. Peer eviction теперь полнее очищает связанные очереди, votes и metadata, key-knowledge maps следуют за bounded identity cache, а устаревшие записи routing-query и RPC auth limiter автоматически удаляются.

При этом trusted contacts остаются pinned, пока им доверяют, поэтому ограничение transient identity knowledge не удаляет ключи, необходимые для проверки их сообщений. В сумме эти исправления не дают long-lived и high-churn nodes незаметно накапливать недостижимое состояние со временем.
