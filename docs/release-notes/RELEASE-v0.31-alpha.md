# What's New in v0.31-alpha

## Stable App Data Directory

Corsa now uses a platform-specific application data directory by default instead of relying on a relative `.corsa` folder in the current working directory. On Windows this maps to the user's roaming app-data area under `CorsaCore`, on macOS to `~/Library/Application Support/CorsaCore`, and on Unix-like systems to `~/.corsacore`.

This makes node-local state more predictable across desktop launches and removes an entire class of bugs where runtime files could end up in whichever directory the process happened to consider its current working directory. Identity, trust, queue, peers, chatlog, downloads, transfer state, and crash logs now all share the same default base location unless explicitly overridden through environment variables.

## File Picker Working-Directory Fix

The desktop app now anchors default state paths to the startup data directory even if a native file picker changes the process working directory during file selection. This addresses the real root cause behind the remaining Windows file-send reports where `.corsa` state could suddenly start appearing next to the file being attached.

With this fix, selecting or sending a file no longer causes `peers-*.json`, `queue-*.json`, or `transfers-*.json` to be written into the source file's folder. The desktop transmit flow continues to use the configured data directory consistently even when platform file dialogs behave differently across systems.

## Selectable Send Errors in Desktop

Desktop send-status errors are now rendered as selectable text. When file preparation or sending fails, the user can highlight and copy the exact error message directly from the composer status area instead of only reading it as a static label.

This improves support and debugging workflows, especially for Windows-specific path and file-dialog failures where users need to share the full error string verbatim.

## Shared App-Data Resolution

The default app-data path logic is now centralized in a dedicated internal package so `config` and `crashlog` resolve their default directories the same way. This removes duplicated platform-path logic and makes it less likely that runtime state and logging drift into different locations over time.

Test binaries keep using a local `.corsa` directory by default, so repository-local test runs remain self-contained and do not pollute the real user profile directories during `go test`.

---

# Что нового в v0.31-alpha

## Стабильная директория данных приложения

Теперь Corsa по умолчанию использует platform-specific директорию данных приложения вместо относительной папки `.corsa` в текущей рабочей директории. На Windows это roaming app-data пользователя с подпапкой `CorsaCore`, на macOS — `~/Library/Application Support/CorsaCore`, а на Unix-подобных системах — `~/.corsacore`.

Это делает расположение node-local state предсказуемым при обычном запуске desktop-клиента и убирает целый класс багов, при котором runtime-файлы могли оказаться в любой директории, которую процесс в данный момент считал своей рабочей. Identity, trust, queue, peers, chatlog, downloads, transfer state и crash logs теперь используют одну и ту же базовую директорию по умолчанию, если путь не переопределён через переменные окружения.

## Исправление смены working directory через file picker

Desktop-клиент теперь жёстко привязывает default state paths к стартовой директории данных даже если native file picker меняет working directory процесса во время выбора файла. Это как раз устраняет реальный корень оставшихся Windows-репортов по отправке файлов, где `.corsa`-state внезапно начинал появляться рядом с выбранным файлом.

После этого исправления выбор или отправка файла больше не приводит к записи `peers-*.json`, `queue-*.json` или `transfers-*.json` в папку исходного файла. Desktop transmit flow продолжает последовательно использовать настроенную data-директорию даже если системные file dialog на разных платформах ведут себя по-разному.

## Выделяемые ошибки отправки в desktop

Ошибки send-status в desktop теперь рендерятся как выделяемый текст. Если подготовка файла или отправка завершается ошибкой, пользователь может выделить и скопировать точный текст ошибки прямо из области статуса в composer, а не только читать его как статическую подпись.

Это упрощает поддержку и отладку, особенно для Windows-специфичных path/file-dialog ошибок, когда нужно передать полный текст ошибки без ручного перепечатывания.

## Единая логика app-data directory

Логика выбора default app-data path теперь централизована в отдельном internal-пакете, поэтому `config` и `crashlog` рассчитывают свои директории одинаково. Это убирает дублирование platform-path логики и снижает риск того, что runtime-state и логи со временем начнут расходиться по разным каталогам.

Для test binaries по умолчанию сохранено использование локальной `.corsa`, поэтому обычные `go test` в репозитории остаются self-contained и не засоряют реальные пользовательские директории профиля.
