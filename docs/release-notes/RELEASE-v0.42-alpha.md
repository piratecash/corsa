# What's New in v0.42-alpha

## Direct Messages Can Now Be Deleted at Message and Conversation Level

This release adds full delete control flow for direct messages. Individual messages can now be deleted, and entire conversations can also be cleared through the new conversation-delete path.

This gives the messaging layer a much more complete lifecycle: not just send, receive, and track, but also explicitly remove content when the user or operator decides it should no longer remain in the conversation history.

## Desktop Gets a File Manager Tab

The desktop application now includes a dedicated file manager tab for working with transfers and related file activity from a clearer, more task-focused surface.

This makes file-oriented workflows easier to browse and reason about than treating everything as a side effect inside chat views alone.

## File Transfer Becomes More Flexible and More Observable

File transfer continues to mature in several directions at once. Dedicated transfer concurrency caps have been removed, newer peers remain eligible for file routes, and the transfer/routing stack keeps improving how it selects and explains file paths.

Together with the earlier route-explanation and self-authenticating file-command work, this makes file delivery less rigid, more diagnosable, and better able to take advantage of viable peers instead of excluding them too aggressively.

## Better Signals Around Node and Download State

RPC now exposes a PIP-0001 node status endpoint. PIP-0001, "Masternode Messenger Service Integration", defines a staged path for making Corsa service availability part of PirateCash masternode operation, and this endpoint gives that effort a clearer node-status surface for health and integration checks.

On the desktop side, completed file downloads can now trigger a sound notification, which gives the user a more immediate signal that a transfer has finished without needing to watch the UI constantly.

---

# Что нового в v0.42-alpha

## Direct messages теперь можно удалять и на уровне сообщения, и на уровне диалога

В этом релизе появился полный delete control flow для direct messages. Теперь можно удалять отдельные сообщения, а также полностью очищать целые conversation через новый conversation-delete path.

Это делает messaging layer заметно более завершённым: теперь он не только отправляет, принимает и отслеживает сообщения, но и позволяет явно убирать контент, когда пользователь или оператор решает, что он больше не должен оставаться в истории диалога.

## В Desktop появилась вкладка file manager

Desktop-приложение теперь включает отдельную вкладку file manager для работы с transfer'ами и связанной файловой активностью через более ясную и task-focused поверхность.

Это делает file-oriented workflows заметно удобнее для просмотра и понимания, чем когда всё воспринимается только как побочный эффект внутри chat views.

## File transfer стал гибче и наблюдаемее

File transfer продолжает заметно взрослеть сразу в нескольких направлениях. Убраны отдельные concurrency caps для transfers, более новые peers остаются допустимыми кандидатами для file routes, а сам transfer/routing stack продолжает улучшать выбор и объяснение путей доставки файла.

Вместе с прежними улучшениями вроде route explanation и self-authenticating file commands это делает доставку файлов менее жёсткой, более диагностируемой и лучше использующей пригодных peers вместо слишком агрессивного их исключения.

## Улучшены сигналы о состоянии ноды и завершении загрузки

В RPC появился PIP-0001 node status endpoint. PIP-0001, "Masternode Messenger Service Integration", задаёт поэтапный путь, по которому доступность сервиса Corsa становится частью работы masternodes PirateCash, а новый endpoint даёт этому направлению более явную node-status поверхность для health и integration checks.

Со стороны desktop завершение file download теперь может сопровождаться звуковым уведомлением, что даёт пользователю более быстрый сигнал о завершении transfer'а без необходимости постоянно следить за интерфейсом.
