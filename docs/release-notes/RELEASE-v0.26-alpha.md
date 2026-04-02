# What's New in v0.26-alpha

## Identity Management in Desktop

Right-clicking (or two-finger tap on Mac) on any identity in the sidebar now opens a context menu with three actions: copy the fingerprint, set a custom alias, or delete the identity along with its chat history. Aliases are saved locally and replace the raw fingerprint everywhere in the interface — sidebar, chat header, and message bubbles. When an identity is deleted, the app automatically selects the nearest neighbor in the list so you're never left staring at an empty screen.

## Stronger Type Safety

Internal peer representation has been replaced with typed domain models. Identity fingerprints, network groups, node types, and capabilities are now distinct types instead of plain strings. This eliminates an entire class of bugs where an address could be confused with an identity or a capability flag passed where a node type was expected.

## Mesh Architecture Overhaul

The monolithic node service has been split into focused modules: peer management, metering, relay, and routing integration. Each module has a clear boundary and responsibility. This doesn't change any external behavior, but makes the codebase significantly easier to work with and reduces the risk of unintended side effects when changing one area.

## Connection Reliability

Two fixes improve how the mesh handles real-world network conditions. Inbound pings on outbound sessions are now answered correctly, which prevents healthy connections from being dropped due to heartbeat timeouts. Parallel inbound handshakes from the same peer are now rejected, eliminating duplicate entries in the peer list that could cause inconsistent routing state.

## Route Stability

Direct routes between peers no longer expire while both peers are actively connected. The announce cycle now refreshes the route TTL, so a long-lived session won't suddenly lose its direct path and fall back to relayed delivery.

---

# Что нового в v0.26-alpha

## Управление identity в десктопе

Правый клик (или касание двумя пальцами на Mac) на любой identity в боковой панели теперь открывает контекстное меню с тремя действиями: скопировать fingerprint, задать пользовательское имя (alias) или удалить identity вместе с историей чата. Alias сохраняется локально и заменяет сырой fingerprint во всём интерфейсе — в боковой панели, заголовке чата и в пузырях сообщений. При удалении identity приложение автоматически выбирает ближайшего соседа в списке, чтобы экран не оставался пустым.

## Строгая типизация

Внутреннее представление пиров заменено на типизированные доменные модели. Fingerprint identity, сетевые группы, типы узлов и capability теперь являются отдельными типами вместо обычных строк. Это исключает целый класс ошибок, когда адрес мог быть перепутан с identity или флаг capability передан вместо типа узла.

## Архитектурный рефакторинг mesh

Монолитный сервис узла разделён на отдельные модули: управление пирами, метрики, ретрансляция и интеграция маршрутизации. У каждого модуля чёткие границы и ответственность. Внешнее поведение не меняется, но работать с кодовой базой стало значительно проще, а риск непредвиденных побочных эффектов при изменении одной области снижен.

## Надёжность соединений

Два исправления улучшают работу mesh в реальных сетевых условиях. Входящие ping на исходящих сессиях теперь корректно обрабатываются, что предотвращает разрыв здоровых соединений из-за таймаута heartbeat. Параллельные входящие рукопожатия от одного и того же пира теперь отклоняются, исключая дублирование записей в списке пиров, которое могло приводить к несогласованному состоянию маршрутов.

## Стабильность маршрутов

Прямые маршруты между пирами больше не истекают, пока оба пира активно подключены. Цикл анонсирования теперь обновляет TTL маршрута, так что долгоживущая сессия не потеряет прямой путь и не откатится к ретранслируемой доставке.
