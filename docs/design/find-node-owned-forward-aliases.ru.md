# Неактивный ForwardCalendar: алиасы собственного owner

Срез меняет только внутреннее хранение алиасов. Обычный constructor, публичные
сигнатуры и существующий максимум16 не меняются. FIND_NODE не активируется,
bridge не переводится в Ready, admission и ресурсы не квалифицированы. Virtual
AUTHOR postimages ещё не компилировались и не исполнялись.

Приватно созданный calendar существующего inactive intake хранит неизменяемый
construction tag, nil публичную map и плотный префикс16 указателей. Публичный
constructor сохраняет прежнюю map, false tag и пустой массив. Нет преобразования,
резервной книги, setter режима или повторного использования retired объекта.
Reservation сохраняет исходный calendar и отдельный heap slot. Равные at/latest
не означают равенство алиасов. Удаление переставляет ячейки указателей, обнуляет
неактивный хвост, не обнуляет удерживаемый slot и не возвращает списанный бюджет.

Публичные Reserve/Cancel/Consume/Pending/Expire работают и на удерживаемом owned
calendar. Сохраняется порядок clock/error/expiry, строгий now>latest и прежние
изменения last/expiry до отказа по неправильному latest. Тело публичного
constructor с map сохранено точно.

Порядок locks: intake -> pending -> один исходный calendar -> target Session.
Под ТЕМ ЖЕ calendar.mu до source CAS проверяются owned-mode, nil map, membership
и свободная ячейка. Mutex удерживается через CAS до фиксированной вставки. Alias
apply меняет только last/projected/ячейку/count без lookup, allocation и нового
отказа. Это не закрывает весь commit: pending.byTarget остаётся map, остальные
scratch/preparation и final sampling являются отдельными зависимостями.

Take/cancel/expiry/answer/Finish сохраняют исходные pointer/generation/phase/
source-reference проверки. Retirement по-прежнему требует отсутствия pending
обязательств writer/алиасов и полного восстановления копий buckets по времени.
Удаление не возвращает projected/actual расход. Старый calendar, target, mutex
и долг существуют до исчезновения внешних ссылок; поздний Reserve на нём не
меняет replacement или его учёт.

512*16=8192 означает ёмкость ячеек указателей, а не число принятых алиасов:
действующие owner occupancy ограничены global pending512 и per-target16. Внешние
ссылки не ограничены этой таблицей. Размер публичного объекта растёт; allocation
bytes, GC/RSS, CPU и длительность locks не измерены. Нет заявления о полном
отсутствии allocations, ресурсном admission или безусловной гарантии шести секунд.

Тестовый oracle содержит полную прежнюю map/bucket реализацию с механически
переименованными идентификаторами, без вызова новых helpers. Tickets сопоставляют
различные указатели, не склеивая одинаковые значения. После каждого шага
сравниваются результаты, scalars, membership и retained handles; отдельно
проверяется nil хвоста. Genuine coordinator fixtures сохраняют реальные refusal/
retirement gates. Full16 combined fixture проверяет компонент, не выдуманный
семнадцатый offer. SOURCE controls не заменяют physical functional mutations.
TYPE, ordinary/race и controls в этом virtual AUTHOR packet остаются NOTRUN.

Исторический NodeORD97513 FAIL, custody и repair отдельно. Continuous final
lease/sample, pending correlation, loaded hint/bootstrap, G2/S6/полный D1 и
activation остаются открытыми. Новых profile/wire/RPC/limits/policy/reset или
ресурсных разрешений нет.
