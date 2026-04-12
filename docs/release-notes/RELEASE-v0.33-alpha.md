# What's New in v0.33-alpha

## Better Bootstrap Seeding on Startup

This release improves how a node finds its first useful peers. Corsa now ships with generated chain-parameter seed lists and primes them through the normal `add_peer` flow during startup instead of relying only on previously persisted state or manual bootstrap configuration.

In practice, this makes fresh starts more reliable and gives the network a stronger baseline path toward initial connectivity, especially when a node does not yet have a good local peer set.

## Safer and Smarter Peer Exchange

Peer discovery is now more conservative about what it shares and how it reacts to announcements. Persisted private IPv4 peers are no longer advertised through peer exchange, which reduces accidental leakage of local or non-routable addresses into the wider mesh.

At the same time, `announce_peer` is now treated as advisory gossip rather than something that should immediately dominate peer state. The peer-selection logic also adds a bias toward fresher candidates, helping the node converge more quickly on currently useful peers instead of overcommitting to stale announcements.

## Clearer Desktop Network Health Warnings

The desktop app now excludes reconnecting peers from its aggregate network warning state. Previously, a peer that was already in the process of reconnecting could still contribute to a broader warning that made the network look more unhealthy than it really was.

This change makes the peers view and connection status feel calmer and more accurate during transient recovery periods.

## More Accurate File-Transfer Sender Progress

File-transfer sender progress is now tracked by the highest served offset rather than by a less precise intermediate notion of progress. This improves the correctness of persisted transfer snapshots and better reflects what data has actually been served to the receiver.

The result is more trustworthy sender-side progress reporting and stronger recovery behavior when transfers are resumed from saved state.

## Smarter File-Datagram Route Selection

File-command delivery now makes better choices when multiple relay paths are available. After confirming that a next-hop can actually carry file transfer traffic, the router prefers the shortest available route, and when multiple candidates have the same hop count it now breaks ties in favor of the peer that has been connected longer.

Stalled peers are also excluded from file-command routing decisions, which helps avoid wasting retries on connections that still exist in state but are no longer healthy enough to carry file traffic.

---

# Что нового в v0.33-alpha

## Улучшенный bootstrap seeding при старте

В этом релизе улучшено то, как нода находит первых полезных peers. Теперь Corsa поставляется с сгенерированными seed-списками в chain params и при старте добавляет их через обычный flow `add_peer`, а не полагается только на ранее сохранённое состояние или ручную bootstrap-конфигурацию.

На практике это делает свежий старт надёжнее и даёт сети более устойчивый базовый путь к первоначальной связности, особенно когда у ноды ещё нет хорошего локального набора peers.

## Более безопасный и умный peer exchange

Peer discovery стал осторожнее в том, что именно он распространяет, и в том, как он реагирует на анонсы. Persisted private IPv4 peers больше не публикуются через peer exchange, что уменьшает риск случайной утечки локальных или non-routable адресов в более широкую mesh-сеть.

Одновременно с этим `announce_peer` теперь трактуется как advisory gossip, а не как сигнал, который должен немедленно доминировать над peer state. Логика выбора peers также получила bias в сторону более свежих кандидатов, поэтому нода быстрее сходится к реально полезным на данный момент узлам и меньше переоценивает stale announcements.

## Более точные desktop-предупреждения о состоянии сети

Desktop-приложение теперь исключает reconnecting peers из aggregate network warning. Раньше peer, который уже находился в процессе переподключения, всё ещё мог усиливать общее предупреждение и создавать ощущение, что состояние сети хуже, чем есть на самом деле.

После этого изменения peers view и общий connection status выглядят спокойнее и точнее в короткие периоды восстановления соединений.

## Более точный sender progress в file transfer

Прогресс отправителя в file transfer теперь рассчитывается по максимальному уже обслуженному offset, а не по менее точному промежуточному представлению прогресса. Это улучшает корректность persisted transfer snapshots и лучше отражает, какой объём данных действительно был отдан получателю.

В итоге sender-side progress reporting становится надёжнее, а восстановление передач из сохранённого состояния работает увереннее.

## Более умный выбор маршрута для file datagram

Доставка file-command теперь лучше выбирает путь, когда доступно несколько relay-маршрутов. После проверки того, что next-hop действительно поддерживает file transfer, роутер предпочитает самый короткий доступный маршрут, а если у нескольких кандидатов одинаковый hop count, tie-break теперь идёт в пользу peer'а, который подключён дольше.

Зависшие peers также исключаются из решений по маршрутизации file-command, что помогает не тратить retry на соединения, которые ещё присутствуют в состоянии, но уже недостаточно здоровы для передачи file traffic.
