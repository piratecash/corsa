# What's New in v2.0.66

## Windows Can Open the Shared State Database Again

This hotfix restores startup on Windows after the shared SQLite state database introduced in v2.0.65. Windows drive-letter paths such as `C:\Users\…` were being encoded as file URIs in a form that made SQLite interpret the drive letter as a remote authority and reject the database before the node could start.

Corsa now normalizes Windows separators and keeps drive-letter and UNC paths entirely inside the URI path component, while preserving POSIX path semantics and special characters in file names. The same database options and safety checks remain in effect on every platform.

## Emoji Render Correctly on Windows

Emoji in the picker, messages and other interface text now render on Windows instead of appearing as blank spaces, while flags no longer fall back to letter pairs such as `UA` or `DE`.

Corsa now bundles a bitmap build of Noto Color Emoji under its own font family and uses it consistently across platforms instead of relying on an incompatible system font. The bundled font is checked at build time for both the required bitmap format and complete coverage of every emoji offered by the picker.

---

# Что нового в v2.0.66

## Windows снова открывает общую state database

Этот hotfix восстанавливает запуск на Windows после появления общей SQLite state database в v2.0.65. Пути с буквой диска, например `C:\Users\…`, кодировались как file URI в форме, из-за которой SQLite принимал букву диска за remote authority и отклонял базу ещё до запуска node.

Теперь Corsa нормализует Windows separators и полностью сохраняет drive-letter и UNC paths внутри path component URI, не меняя семантику POSIX paths и специальных символов в именах файлов. Database options и проверки безопасности остаются одинаковыми на всех платформах.

## Emoji корректно отображаются на Windows

Emoji в picker, сообщениях и остальных элементах интерфейса теперь отображаются на Windows вместо пустых мест, а флаги больше не превращаются в пары букв вроде `UA` или `DE`.

Corsa теперь включает bitmap-сборку Noto Color Emoji под собственным font family и использует её одинаково на всех платформах вместо несовместимого системного шрифта. Во время сборки проверяются необходимый bitmap format и наличие всех emoji, предлагаемых picker.
