# Тестовый инициатор D1 и 13b

Инициатор D1 существует только в тестах и стенде (18a §5.9), публичный API D2
не добавлен. Helper компилируется в Node.test; владелец исходного bounded
reader передаёт ответы. Новых reader/goroutine/рекурсивной маршрутизации,
advertisement, Ready и отрицательных сетевых кадров нет. Функциональная
обвязка не означает готовый T5 launcher или принятые нагрузочные замеры.

Перебор использует peerSendableConnectionsLocked с responder capability,
затем точные ProvenSession/common/published Session/handshake capability/core.
До трёх разных R, по одному исходному соединению на identity. Metadata claim
не заменяет proof. После Node waits перед очередью повторно проверяются
исходный common/core/session, context и первоначальный timeout.

У lookup один свежий challenge, ID, target, context, start и три исходных R.
Пул —16; конечный список целей workload хранит по три начала/30с. Cancel и
локальная ошибка отправки не возвращают уже списанный lookup или ID. ID не
переиспользуется при переполнении. Poll освобождает context-cancelled и
истёкшие12с попытки; поздний ответ не завершает попытку повторно.

Исходы: Found/NoOverlayNeighbours/Timeout/InvalidAnswer/Limited. Cancel —
context error без нового wire status. Проверяются тот же original, ID, target,
строгий key и signature над retained challenge; чужой ID игнорируется.
Невалидный ответ одного R не отменяет оставшийся исходный R.

Семь roots проверяют enumeration/proof/capability/3R, identity-rebind/End,
настоящий protected-stream Found, no-neighbor/timeout equality/cancel,
invalid/foreign original/ID, invalid→valid у двух R, pending16 и3lookup/30с
без возврата лимита при cancel. Optional capability входит в первоначальный
TLS handshake тестового fixture; прежние вызовы не меняются.

T5 process/изоляция/transport, final13b-D1 и тяжёлые пороги, PR5, construction/
inverse closure, QA и решение выпуска остаются отдельными работами. Native
факты прошлых артефактов не переносятся.
