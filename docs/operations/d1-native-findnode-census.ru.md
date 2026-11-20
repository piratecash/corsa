# Закрытый native-кандидат FIND_NODE с census

Под тегом `d1_o1a_native_responder` добавлен отдельный FullResponder-вход
для native-стенда. Он создаёт исходный RuntimeDomain/Ready и census в том же
первом Service, до constructor callbacks. Скаляры запроса копируются;
повторное принятие, подмена источника/identity/lifetime и перезапись отказа
не позволяют финализировать Ready. Обычные конструкторы остаются Off;
loopback/small2 diagnostic сохраняется отдельно.

Профиль повторяет ранее проверенное распределение SG512: active512,
members512, registry visits1024 и прежние timing/signing. Native-стенд
сохраняет два Full, 10 000 маршрутов и исходные dial slots1/0.
Это параметры испытуемого закрытого конструктора, не результат нагрузки512.

В существующем live census callback проверяется настоящий опубликованный F1
той же Ready-службы, с исходными token/lifetime/ConnID/peer/direction/exporter.
Закрытые исторические строки сохраняют прежний V2-смысл. Финальный census
после остановки, формат отчёта, RPC-команды и route-wire traffic не меняются.
FIND_NODE-запросы в quiet-сценарий не добавлены.

Постоянный ownIntro hash/flags не добавлялся: ранняя идея инструментирования
отброшена как лишняя для этого сценария. `find_node_connection.go` остался
побайтно исходным. Genuine TLS unit проверяет, что peer действительно увидел
F1/R; собственная R в каждом будущем runtime-сеансе здесь **UNKNOWN**.
Forwarding в этом сценарии **NOT_EXERCISED**. Область результата —
**ENABLED_IDLE_CONSTRUCTION_COST**, а не доказательство всех wire-путей.

На изолированном снимке commit `2188dfb8fcb74174fa4d1df2084784bd07d436bf`
плюс ровно26 posts прошли **164 именованных выполнения и353 вложенных случая**,
без FAIL/SKIP. Все20 команд exit0, 2642 исходных файла неизменны; собственные
процессы отсутствуют и lock снят. Node/App test ELF и FullNode main собраны
Linux arm64/CGO0 на Go1.27.4, SHA/static/BuildInfo проверены, ELF не исполнялись.
Два прежних отказа конечных guard-инвентарей сохранены. Исправлены только
точный raw-body pin, точный tagged-import case и прежний Service pin драйвера;
scanner и отрицательные проверки не отключались.

[Переносимые свидетельства](evidence/d1-native-responder-2026-11-20/README.md).
Runtime-замер текущего кандидата ещё нужен. [Отказ baseline census](d1-o1a-baseline-census.ru.md)
остаётся отдельным результатом; этот QA не закрывает О‑1а, память512МБ,
оставшиеся13b-D1, G2, публичную активацию или выпуск.
