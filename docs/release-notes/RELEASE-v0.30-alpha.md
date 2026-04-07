# What's New in v0.30-alpha

## Public Go SDK for Apps and Bots

Corsa can now be used not only as a desktop app or standalone node, but also as an embeddable Go SDK. The new public `sdk` package lets external projects start a CORSA node from Go code, configure it through explicit Go structs instead of environment variables, execute the same command layer used by the desktop console, and subscribe to decrypted incoming direct messages for bot-style automation.

This makes the repository usable as a foundation for third-party CORSA applications, service integrations, and autonomous bots. The release also adds SDK documentation with Mermaid diagrams and a minimal `Sic Parvis Magna` bot example that starts a node and replies to every incoming message. The module path is now aligned with the public GitHub repository so the SDK can be consumed as a normal Go module via `github.com/piratecash/corsa`.

## Donate Documentation and Desktop Donate Tab

Corsa now includes a dedicated donation page in the documentation and a matching `Donate` tab in the desktop console. The new tab collects the main project donation addresses in one place, keeps them selectable for manual copy, adds per-address copy buttons, and links directly to the PirateCash donation page. README now also exposes the main donation addresses so support options are visible directly from the repository front page.

This release also expands localization coverage for the donate UI so the tab title and supporting text are translated across the desktop's supported languages. The goal is simple: make it easier for users to discover how the project is funded and support development without hunting through external pages.

## Windows File Attach Fix

File sending on Windows is more reliable in this release. The desktop transmit-store path no longer relies on a manually concatenated `.tmp` file name for the temporary copy step. Instead it creates temp files through the platform-safe temp-file flow in the transmit directory before renaming them into place.

This removes a Windows-specific failure mode where selecting a file for sending could fail during the temporary transmit copy step with a missing-path error. The transmit copy logic is now more robust across platforms while preserving the same content-addressed storage model.

## Console Space-Key Fix

The desktop console no longer treats the space key like an implicit command action. Previously, pressing space in some autocomplete states could submit the current command instead of inserting a normal space into the editor, which made argument entry fragile and could also leave malformed command text behind.

The console now lets the editor own regular space input, so adding whitespace between the command name and its arguments behaves like normal text editing. This fixes both accidental execution on repeated spaces and the broken case where inserting a space between a command and an address still left them glued together as a single unknown command.

---

# Что нового в v0.30-alpha

## Публичный Go SDK для приложений и ботов

Теперь Corsa можно использовать не только как desktop-приложение или отдельную ноду, но и как встраиваемый Go SDK. Новый публичный пакет `sdk` позволяет внешним проектам поднимать CORSA-ноду из Go-кода, настраивать её через явные Go-структуры вместо `env`, выполнять тот же слой команд, который использует desktop-консоль, и подписываться на расшифрованные входящие direct messages для bot-style automation.

Это делает репозиторий пригодным как базу для сторонних CORSA-приложений, сервисных интеграций и автономных ботов. В релиз также вошли документация по SDK с Mermaid-диаграммами и минимальный пример бота `Sic Parvis Magna`, который поднимает ноду и отвечает на каждое входящее сообщение. Module path теперь выровнен под публичный GitHub-репозиторий, поэтому SDK можно подключать как обычный Go-модуль через `github.com/piratecash/corsa`.

## Документация по донатам и вкладка Donate в desktop

В Corsa появилась отдельная страница с донатами в документации и соответствующая вкладка `Donate` в desktop-консоли. Новая вкладка собирает основные адреса для поддержки проекта в одном месте, оставляет их выделяемыми для ручного копирования, добавляет отдельные кнопки copy для каждого адреса и даёт прямую ссылку на страницу донатов PirateCash. В README также вынесены основные donation-адреса, чтобы способы поддержки были видны сразу с главной страницы репозитория.

В этом релизе также расширено покрытие локализацией для donate-интерфейса: название вкладки и сопровождающий текст переведены для поддерживаемых языков desktop-клиента. Цель простая: сделать финансирование проекта более прозрачным и упростить поддержку разработки без необходимости искать адреса на внешних страницах.

## Исправление отправки файлов на Windows

Отправка файлов на Windows в этом релизе стала надёжнее. Desktop transmit-store больше не опирается на вручную склеенный `.tmp`-путь для временного шага копирования. Вместо этого временные файлы создаются через platform-safe temp-file flow прямо внутри transmit-директории с последующим rename на целевой путь.

Это устраняет Windows-специфичный сценарий ошибки, при котором выбор файла для отправки мог падать на этапе временного копирования в transmit с сообщением о несуществующем пути. Логика копирования в transmit теперь устойчивее на разных платформах и при этом сохраняет ту же content-addressed модель хранения.

## Исправление пробела в desktop-консоли

Desktop-консоль больше не воспринимает клавишу пробела как неявное действие команды. Раньше в некоторых состояниях autocomplete нажатие пробела могло отправить текущую команду вместо обычной вставки пробела в editor, из-за чего ввод аргументов становился хрупким и мог оставлять некорректный текст команды.

Теперь обычный ввод пробела полностью остаётся на стороне editor, поэтому разделение имени команды и аргументов работает как нормальное текстовое редактирование. Это исправляет и случайное выполнение команды при повторных пробелах, и сценарий, где вставка пробела между командой и адресом всё равно оставляла их слитыми в одну неизвестную команду.
