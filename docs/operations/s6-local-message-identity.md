# Local message identity (S6 repair)

## English

### Identity and compatibility

A wire message ID is not a globally unique identifier. Local message state uses
`MessageRef{Topic, ID, Sender, Recipient}`. The original strings are preserved;
the in-memory key includes field lengths so separators inside a field cannot
merge two references. Two authors, recipients or topics may therefore reuse
the same wire ID without sharing delivery state, a stored row or a UI item.

This repair does not add wire fields or change protocol 31. A retry or recovery
may reseal the same message reference; ciphertext equality is not required.
The immediate relay peer is not treated as the original message author.
Existing signature and authority checks still apply.

Local tuple isolation does not authenticate unsigned outer IDs or receipt
parties, or add replay/attempt freshness. Conservative transit Bloom
suppression remains in place: distinct references do not imply that every
message will be admitted. These limits remain part of candidate review.

Exact local operations carry the selected reference through lookup and mutation.
Older ID-only entry points succeed only when the relevant local selection is
unambiguous. They refuse an ambiguous selection before changing state; they do
not select the first matching row. This does not add endpoint fields to old
wire controls: an exact local selection cannot prove that an older remote
recipient has an unambiguous selection.

### Database and upgrade

Migration `0009_message_identity.sql` gives `messages` the primary key
`(topic, id, sender, recipient)`. It preserves existing message rowids, bodies
and wire values. Rowids remain available for existing arrival-order queries.
Delivery journals bind to the full reference. Old journal rows without a
matching message remain in explicit legacy-unbound tables and cannot confirm
a different message. Recovery, deletion and reactions retain their appropriate
conversation/reference scope.

`AppendReportNew` ignores only a conflict on the full message key. Invalid enum
values are reported as `ErrInvalidEntry`; they are not treated as a duplicate.
The message-store adapter defers that invalid entry. Other storage failures
retain their separate failure behavior; this repair does not establish a new
guarantee of durable delivery during a database failure.

The migration runner applies the new migration transactionally and keeps the
checksums of previously published migrations. The upgrade is forward-only.
Older catalogs reject the newer schema; keep a SQLite-consistent pre-upgrade
backup for rollback. Replacing only the binary is not a supported downgrade
procedure for this database.

### Release boundary

The implementation, regression evidence and final candidate review must be
evaluated together. Library compilation or passing storage tests alone do not
establish client compatibility, a memory budget, independent cryptographic
review or release admission. The inactive K2/R1b/R2/N-in mechanisms remain
outside this repair's activation scope.

## Русский

### Идентичность и совместимость

Wire ID сообщения не является глобально уникальным идентификатором. Локальное
состояние использует `MessageRef{Topic, ID, Sender, Recipient}`. Исходные строки
сохраняются; ключ в памяти содержит длины полей, поэтому разделители внутри
поля не объединяют разные ссылки. Разные авторы, получатели или темы могут
использовать одинаковый wire ID, не разделяя состояние доставки, строку БД
или элемент интерфейса.

Исправление не добавляет wire-полей и не меняет protocol 31. Повтор или
восстановление может заново зашифровать сообщение с той же ссылкой; равенство
шифротекста не требуется. Непосредственный relay-пир не считается исходным
автором сообщения. Проверки подписи и полномочий сохраняются.

Изоляция локальных ссылок не аутентифицирует неподписанные внешние ID или
стороны квитанций и не добавляет свежесть replay/attempt. Консервативное
подавление transit через Bloom сохраняется: разные ссылки не гарантируют
допуск каждого сообщения. Эти ограничения остаются предметом ревью кандидата.

Точные локальные операции передают выбранную ссылку от поиска до изменения.
Старые входы только с ID работают, когда соответствующий локальный выбор
однозначен. При неоднозначности они отказывают до изменения состояния, а не
берут первую подходящую строку. Это не добавляет endpoints в старые wire-команды:
точный локальный выбор не доказывает однозначность выбора у старого удалённого
получателя.

### База данных и обновление

Миграция `0009_message_identity.sql` задаёт таблице `messages` первичный ключ
`(topic, id, sender, recipient)`. Исходные rowid, тела и wire-значения сообщений
сохраняются. Rowid по-прежнему доступны запросам порядка поступления.
Журналы доставки привязываются к полной ссылке. Старые записи журнала без
соответствующего сообщения сохраняются в отдельных legacy-unbound таблицах
и не могут подтвердить другое сообщение. Восстановление, удаление и реакции
сохраняют соответствующую область диалога или ссылки.

`AppendReportNew` игнорирует только конфликт полного ключа сообщения.
Недопустимые значения enum возвращают `ErrInvalidEntry` и не считаются
дубликатами. Адаптер хранилища откладывает такую недопустимую запись. Другие
ошибки хранилища сохраняют отдельную обработку; исправление не вводит новую
гарантию надёжной доставки при отказе БД.

Runner применяет новую миграцию транзакционно и сохраняет контрольные суммы
ранее опубликованных миграций. Обновление однонаправленное. Старый каталог
откажется открывать новую схему; для отката нужна согласованная SQLite-копия,
сделанная до обновления. Замена только бинарника не является поддерживаемым
откатом этой базы.

### Граница выпуска

Реализация, регрессионные свидетельства и ревью итогового кандидата оцениваются
совместно. Компиляция библиотек или успешные тесты хранилища сами по себе не
подтверждают совместимость клиентов, бюджет памяти, независимую
криптоэкспертизу или допуск выпуска. Неактивные K2/R1b/R2/N-in остаются вне
области активации этого исправления.
