# C3d: исходный приём запроса и dormant owned offer

Приватный кандидат соединяет оба настоящих bounded read loops с одноразовым receive receipt, строгим parser после admission и dispatcher, затем с OfferReceived того же исходного владельца. Успешный вызов сохраняет provisional row для уже известной исходной target Session. Неизвестный target и локальный T пока дают консервативный отказ.

Received берётся одним genuine Sample после существующего bounded raw чтения, до успешного line admission и последующих Node/coordinator/pending waits. Исходные transport lifetime и F1/CM Session не подменяются поиском нового ConnID. До успешного admission cell хранит только raw bytes и point, без strict parse. Строгий parser в dispatcher после admission не выдаёт partial ingress. Повторный вызов исходного receipt после успеха, отказа или cancellation запрещён.

Все allocation и housekeeping выполняются до R. После coordinator→pending waits используется один свежий f и проверяются все исходные members. Fixed cut равен received + Round; равенство даёт отказ. Received не обновляется. Если между историческим q и свежим f любой pending row потребовал cleanup, новый offer отказывает без новой строки, замены или final debt. Calendar cleanup не выполняется под R; скрытого retry/resample нет.

Immutable tuple owner/source/stamp/received/request вместе с pointer копируется в pending и C3b snapshot, затем сравнивается в C3a/C3c. Единственное mutable поле cell — consumed atomic. Подмена pointer и изменение tuple на месте дают отказ. Без provenance допустимы только существующие tagged logical FixtureDomain tests; Runtime row такого права не получает.

Ready остаётся false. До него нужны fixed-cut scheduler (включая idle last request), strict answer/local T dispatcher, original writer Dispatch/Take/Finish, answer/cancel/expiry/End на тех же книгах, закрытие inverse mutation edges и loaded core/auth/tracked-writer closure. Только после этого возможен отдельный construction Ready seal. Offer не увеличивает лимиты, не списывает source windows, не выдаёт target ID/alias и не пишет в сеть.

Добавлены 12 behavioral test roots и два конечных Source guards; существующие guards расширены поверх текущей исправленной форматированной основы. Автор выполнял только bounded text/JSON/hash проверки. Go/FORMAT/compile/ordinary/race и окончательная release readiness требуют новых ROOT facts на итоговом артефакте.
