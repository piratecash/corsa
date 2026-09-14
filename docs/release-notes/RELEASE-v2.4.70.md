# What's New in v2.4.70

## Replies Stay in the Order They Arrived

Conversations are now displayed in local arrival order instead of being rearranged by timestamps supplied by each sender. A reply therefore stays below the message it answers even when the other device's clock is ahead or behind, and the order no longer changes after reopening or reloading the conversation.

Delivery confirmations also use the time this device received them for the status shown under a message. Clock differences between devices can no longer make a message appear to have been delivered before it was sent.

## Quote Links Find the Right Message

Clicking or tapping a reply quote now centres the original message and highlights it briefly. The jump remains accurate with messages of different heights, late-loading image previews and new messages arriving at the same time. Scrolling manually immediately returns control to the reader.

## Settings Have Their Own Tab

Installation-wide options now live in a dedicated Settings tab in the console. The language selector has moved there from the header, leaving more room for conversations, especially on narrow screens. Console tabs now fit according to their actual translated labels and move into the More menu when necessary.

The desktop client can also check GitHub for a newer Corsa release and show the result in Settings. This check is optional and disabled by default because it connects directly to GitHub outside the peer-to-peer network, which exposes the node's IP address to GitHub. It can be run manually, and disabling it stops future checks, cancels an active request and clears the previous result.

## DHT Preparation

Work continues on the foundation for the future DHT. A documented and test-checked contract now defines how verified identities will be assigned to the two roles of the planned overlay. Classification alone does not grant participation: a node must also prove that it owns the key, that the key matches its identity and that the proof belongs to the active connection.

Additional tests ensure that the requested destination cannot expand the pinned first-hop guard set or bring back a guard that policy has excluded. A separate topology simulator now evaluates connection policies before they reach the running node. Its results confirm that slot-allocation policy has a major effect on connectivity and that the largest tested topology still needs further design work.

These changes are limited to contracts, tests and simulation. They do not enable DHT routing or anonymous transport and do not change current message delivery.

---

# Что нового в v2.4.70

## Ответы сохраняют порядок получения

Сообщения в переписке теперь располагаются в локальном порядке получения, а не переупорядочиваются по времени, указанному отправителями. Поэтому ответ остаётся ниже сообщения, на которое он отвечает, даже если часы на другом устройстве спешат или отстают. Порядок также больше не меняется после повторного открытия или перезагрузки переписки.

Для статуса доставки под сообщением теперь используется время, когда подтверждение было получено этим устройством. Разница часов между устройствами больше не приводит к отображению доставки раньше отправки.

## Переход по цитате находит нужное сообщение

Клик или нажатие на цитату ответа теперь переносит к исходному сообщению, размещает его по центру и ненадолго подсвечивает. Переход остаётся точным при разной высоте сообщений, поздней загрузке preview изображений и одновременном получении новых сообщений. Ручная прокрутка сразу возвращает управление пользователю.

## Настройки получили отдельную вкладку

Параметры, относящиеся ко всей установке, теперь собраны на отдельной вкладке «Настройки» в консоли. Выбор языка перенесён туда из верхней панели, освобождая больше места для переписки, особенно на узких экранах. Вкладки консоли теперь размещаются с учётом реальной ширины переведённых названий и при необходимости переходят в меню «Ещё».

Desktop-клиент также может проверить наличие новой версии Corsa на GitHub и показать результат в настройках. Проверка опциональна и по умолчанию отключена: запрос идёт напрямую к GitHub за пределами p2p-сети, поэтому GitHub видит IP-адрес ноды. Проверку можно запустить вручную; её отключение прекращает будущие проверки, отменяет текущий запрос и очищает предыдущий результат.

## Подготовка к DHT

Продолжается создание основы для будущего DHT. Зафиксирован и покрыт тестами контракт, по которому проверенные identities будут распределяться между двумя ролями планируемого overlay. Одной классификации недостаточно для участия: нода также должна доказать владение ключом, соответствие ключа своей identity и привязку доказательства к активному соединению.

Дополнительные тесты гарантируют, что запрашиваемый получатель не может расширить закреплённый набор first-hop guards или вернуть в него guard, исключённый политикой. Отдельный симулятор топологии теперь позволяет проверять правила построения соединений до их появления в работающей ноде. Результаты подтвердили, что способ распределения доступных соединений существенно влияет на связность, а самая крупная проверенная топология всё ещё требует дальнейшей проработки.

Эти изменения ограничены контрактами, тестами и симуляцией. Они не включают DHT-маршрутизацию или анонимный транспорт и не меняют текущую доставку сообщений.
