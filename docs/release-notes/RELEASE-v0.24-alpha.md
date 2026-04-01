# What's New in v0.24-alpha

## Smart Message Routing

Messages now travel through the network much more efficiently. Instead of being sent to everyone in hopes of reaching the right person, Corsa now builds a map of the network and sends messages along the shortest path to the recipient. If the direct route isn't available, the old method kicks in automatically — so nothing breaks.

In practice this means faster delivery, less unnecessary traffic, and better performance on larger networks.

## Relay Improvements

When a message needs to pass through intermediate nodes to reach someone behind NAT or on a different part of the network, the relay system is now smarter. It reuses existing connections instead of opening new ones, which makes delivery more reliable — especially for users who are not directly reachable.

## Network Health Monitoring

The console now shows more useful information about your connections. You can see how long you've been connected to each peer (uptime) and what protocol version they're running. New commands let you inspect the routing map in real time — helpful for understanding how your node sees the network.

## Better Stability

Several improvements make the network more resilient to unstable connections. If a peer keeps disconnecting and reconnecting rapidly, the system temporarily reduces trust in that connection to prevent cascading disruptions. Invalid or malformed data from other nodes is now caught and rejected earlier, before it can cause issues.

---

# Что нового в v0.24-alpha

## Умная маршрутизация сообщений

Сообщения теперь доставляются по сети намного эффективнее. Вместо того чтобы рассылать сообщение всем подряд в надежде, что оно дойдёт до нужного человека, Corsa теперь строит карту сети и отправляет сообщения по кратчайшему маршруту. Если прямой путь недоступен, автоматически используется старый способ — так что ничего не ломается.

На практике это означает более быструю доставку, меньше лишнего трафика и лучшую производительность в больших сетях.

## Улучшения ретрансляции

Когда сообщение проходит через промежуточные узлы, чтобы добраться до пользователя за NAT или в другой части сети, система ретрансляции теперь работает умнее. Она переиспользует уже открытые соединения, а не создаёт новые, что делает доставку надёжнее — особенно для пользователей, которые не доступны напрямую.

## Мониторинг состояния сети

Консоль теперь показывает больше полезной информации о подключениях. Видно, как долго вы подключены к каждому пиру (аптайм) и какую версию протокола он использует. Новые команды позволяют в реальном времени просматривать карту маршрутов — удобно, чтобы понимать, как ваш узел видит сеть.

## Лучшая стабильность

Несколько улучшений делают сеть устойчивее к нестабильным соединениям. Если пир постоянно отключается и подключается, система временно снижает доверие к этому соединению, чтобы избежать каскадных сбоев. Некорректные или повреждённые данные от других узлов теперь отсекаются раньше, до того как могут вызвать проблемы.
