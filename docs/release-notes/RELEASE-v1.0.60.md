# What's New in v1.0.60

## Sender-Owned DM Delivery Gets Stricter and Safer

This release continues the shift to sender-owned direct-message delivery and closes several ways old relay behavior could still create storms or wasted traffic. Origin-authored DMs are no longer re-gossiped indefinitely by relay-retry logic, sender-owned messages are now held until the recipient is actually reachable, and transit or broadcast envelopes get bounded lifetime rules so stale traffic cannot circulate forever.

The result is a delivery model that is more deliberate: relays do less blind work, old envelopes age out predictably, and sender-owned retries stay focused on real delivery instead of feeding another gossip loop.

## Relay and Node Policy Become More Controlled

Operators can now run relay-only nodes with `CORSA_ACCEPT_DM=0`, which drops inbound DMs and keeps the node's box key off the contact plane. Automatic outbound peer selection also gains a subnet-diversity gate, limiting connections to at most one automatic outbound per `/24` on IPv4 or `/64` on IPv6, while manual `addpeer` stays exempt.

Together, these changes make node roles clearer and help the mesh avoid over-concentrating automatic connections on the same network slice.

## Routing and Runtime Performance Improve in Steady State

Routing snapshots now use a copy-on-write incremental model, so unchanged identities can be reused instead of deep-copying the whole table every rebuild. Announce-state cleanup was also tightened so dead peer state is reclaimed by reconciling against the live peer set instead of waiting for a matching disconnect event.

Outside routing, peer-state persistence is now debounced and coalesced instead of flushing a full snapshot on every small change, `PeerIdentity` is stored as a fixed binary value rather than a hex string, and the desktop peers tab caches its derived rows by snapshot generation. In practice, this should reduce allocation churn, GC pressure, and unnecessary UI recomputation.

## Logging and Crash Output Are Easier to Operate

Crash logging now supports `CORSA_LOG_FORMAT` with `console` or `json` output, defaults to `console`, and handles log capping more cleanly by shrinking in place and cleaning up older rotated copies.

That makes long-running logs easier to manage and gives operators a clearer path when they want structured output for external tooling.

---

# Что нового в v1.0.60

## Sender-owned доставка DM стала строже и безопаснее

Этот релиз продолжает переход к sender-owned модели доставки direct messages и закрывает несколько мест, где старое relay-поведение всё ещё могло создавать storms или лишний трафик. Origin-authored DMs больше не re-gossip'ятся бесконечно через relay-retry, sender-owned сообщения теперь удерживаются до тех пор, пока recipient действительно не достижим, а для transit и broadcast envelopes появились ограниченные lifetime policy, чтобы stale traffic не мог ходить по сети вечно.

В результате модель доставки становится более осмысленной: relays делают меньше слепой работы, старые envelopes предсказуемо aging out, а sender-owned retries сосредоточены на реальной доставке вместо подпитки очередного gossip loop.

## Relay и node policy стали более управляемыми

Операторы теперь могут запускать relay-only nodes с `CORSA_ACCEPT_DM=0`: такие узлы дропают inbound DMs и не выставляют box key в contact plane. Automatic outbound candidate selection также получил subnet-diversity gate: не более одного automatic outbound на `/24` в IPv4 или `/64` в IPv6, при этом manual `addpeer` остаётся exempt.

Вместе эти изменения делают роли нод понятнее и помогают mesh не переуплотнять automatic connections внутри одного сетевого сегмента.

## Routing и runtime performance улучшились в steady state

Routing snapshots теперь используют copy-on-write incremental model, поэтому для неизменившихся identities можно переиспользовать существующие slices вместо полного deep copy всей таблицы на каждом rebuild. Cleanup announce-state тоже стал точнее: состояние умерших peers теперь reclaim'ится через reconciliation с live peer set, а не ждёт обязательного matching disconnect event.

За пределами routing peer-state persistence теперь debounced и coalesced вместо полного flush на каждое мелкое изменение, `PeerIdentity` хранится как фиксированное бинарное значение вместо hex string, а desktop peers tab кеширует derived rows по snapshot generation. На практике это должно уменьшить allocation churn, GC pressure и лишние UI recomputation.

## Логи и crash output стали удобнее в эксплуатации

Crash logging теперь поддерживает `CORSA_LOG_FORMAT` со значениями `console` или `json`, по умолчанию использует `console`, а cap логов обрабатывается аккуратнее: shrink-in-place плюс cleanup старых rotated copies.

Это упрощает жизнь long-running инсталляциям и даёт более удобный путь для structured output, если логи дальше забирает внешний tooling.
