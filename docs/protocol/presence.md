# Contact presence

## English

### 1. Purpose and scope

This document is the normative specification of **contact presence** — what
this node believes about whether a contact is currently there, what that
belief rests on, and how it crosses the local RPC boundary.

Presence answers a question asked by a person. It is deliberately NOT the
routing question. "Do I have a path to X" is answered by the routing table
and remains the input to delivery decisions; the two were the same value for
a long time, and that is what made a contact look present for up to ten
minutes after they had gone — a route outlives its owner.

Implemented: the state model, the source attribution, the liveness probe, the
route fallback, the local RPC command, the interface treatment, and the sealed
reciprocity claim that gates a probe (§4.1), and the first-hop guard set
(§4.2). Not in this revision: fixed padding.

### 2. The state model

A contact's presence is two independent facts.

**State** — one of four:

| State | Meaning |
|---|---|
| `unknown` | No answer. Not a degraded `offline`; see the reason below. |
| `offline` | Believed absent, on an observation ABOUT the contact. |
| `probing` | A path exists and liveness is not established yet. |
| `online` | Believed present. |

**Source** — what the state rests on: `proof`, `passive`, `session_closed`,
`route_observation`, `probe_timeout`, `route_fallback`, or `none`.

Only `proof` and `passive` are *proven*: they are the contact's own
signature. Everything else is this node's inference. The distinction is
normative, not cosmetic — the interface is required to render a proven
presence differently from an inferred one.

When the state is `unknown`, a **reason** says which kind of not-knowing it
is: `no_local_connectivity`, `route_suppressed_locally`, `stale`, or
`not_probeable`.

An identity with no record is `unknown`. Reading an absent entry as `offline`
is forbidden: it turns this node's own outage into a claim about everybody
else.

### 3. Rules

The state of one contact is derived in this order. The order is the contract.

1. **Our own connectivity gates everything.** With no connected peer, an
   empty routing table describes THIS node, so every contact is `unknown`
   (`no_local_connectivity`). A node with a single usable peer is not gated:
   it still routes, probes and receives.
2. **Evidence from the contact outranks every inference.** A valid
   `target_proof`, or a frame the contact handed over on their OWN
   authenticated session, makes them `online` until the validity window
   expires.

   A relayed copy does **not** count, and the distinction is not pedantry: a
   signature proves who wrote a message, not that they are awake. Relays store
   and forward, so a transit copy can arrive long after its author left — and
   can arrive repeatedly, refreshing the window each time, which would leave a
   contact green for as long as the network keeps replaying them. Contacts
   reachable only through transit are covered by the probe, which is a round
   trip and cannot be replayed, and by the fallback (§5) until they can answer
   one.
3. **Consecutive unanswered probes are an observation about them.** Three in
   a row (`Detect Mult`, RFC 5880 §6.8.4) is `offline` (`probe_timeout`). Any
   evidence of life resets the count. A probe that never reached the network
   is not counted — a local failure is not the contact's silence.
4. **An attributable session close ends the green immediately.** It is
   recorded when the close is observed, NOT when the deferred route
   withdrawal executes: the withdrawal is delayed on purpose so a reconnect
   does not produce a withdrawal storm, and applying that delay to presence
   would keep a departed contact green for the whole grace period.
   The close that counts is the contact's LAST session of any kind, not their
   last relay-capable one. Routing withdraws on the latter and is right to; a
   statement about the person is not a statement about their relay capability,
   and reading it off the routing condition was wrong in both directions — a
   contact whose build has no relay capability was never recorded as gone at
   all, and one who closed a relay session beside a live second session was
   recorded as gone while we were still talking to them.
   A close does not by itself claim absence while a path is still visible:
   with a path, the contact is `probing` (and probed at once); with none,
   `offline` (`session_closed`).
   A close is **spent** once the route it was recorded against has been seen
   to disappear and a new one has appeared, or once a session with the contact
   is up again — the mirror of the condition that recorded it. The second half
   is what covers a contact with no direct route to watch at all.
   A close and a re-established session are ordered by a **transition number**,
   minted where the session count crosses 0, and never by a clock. The two
   events are causally ordered in the world and not in the process — a close
   travels through session accounting, teardown and route bookkeeping before it
   lands — and a clock cannot settle it: the reading has to be taken before the
   lock that decides the transition, so overlapping sessions can invert it; a
   coarse clock ties them; a clock stepped backwards reverses them. A transition
   whose number is not greater than the newest one applied is **dropped whole**.
   The number is recorded even when a reconnect finds no close to withdraw,
   because that is precisely the case where a reconnect overtook the close of
   the previous session — the record is what makes that close lose when it
   arrives.
   Against evidence of LIFE there is no such shared point, so every one of these
   events carries the time it was OBSERVED rather than the time it was recorded,
   and **whichever of them is older is dropped whole** — in both directions. A
   close laid on top of a later proof greys out a contact who has just signed
   for us; a proof laid on top of a later close, or a later unanswered probe,
   puts a contact who has left back online for the whole validity window. The
   two orderings are equally reachable, because the goroutines that observe them
   do not take turns. Ties go to the negative observation: a false grey is
   resolved by the next probe, a false green lasts 450 s.
   Every presence instant — this one, the validity window, the probe timeout and
   the probe cadence — is read from ONE clock and keeps its monotonic component
   from that read to whatever uses it. Conversion to a calendar time happens
   only where an instant leaves the process, in persistence.
   In the implementation these rules are carried by a TYPE rather than by
   convention: a presence moment is not a plain timestamp, and the spellings
   that get it wrong — subtracting, comparing as a deadline, converting away the
   monotonic component — are not operations it offers.
   A DURATION is then measured as **whichever clock saw more time**, monotonic
   or wall, because each covers the other's blind spot. The monotonic clock
   stops while the machine sleeps, so on it alone a laptop closed for three
   hours wakes with a proof still inside its 450 s window, an open probe that
   never times out and a cadence resuming as if nothing happened — the long
   false green this feature exists to remove, arriving through ordinary use of a
   laptop. The wall clock sees the sleep but is the one a step corrupts, and on
   it alone a clock moved backwards extends the window and stalls the timeouts.
   The larger of the two is right in both cases. The cost is that a wall clock
   jumped FORWARD spuriously expires a window early: one probe and one round
   trip, against 450 s of claiming somebody is there who is not.
5. **A route WE suppressed says nothing about the contact.** Quarantine, flap
   hold-down, seq hold-down, K-cap eviction and an unresolvable next hop all
   yield `unknown` (`route_suppressed_locally`), never `offline`. These last
   up to thirty minutes; reporting them as absence would be a half-hour lie.
6. **A route that vanished while we were healthy is `offline`**
   (`route_observation`). This is the one thing presence still takes from
   routing.
7. **A path with nothing proven along it** is `probing` for a contact that
   can be probed, and the route fallback (§5) for one that cannot.

### 4. The liveness probe

The probe is an ordinary `get_identity` datagram with `target_proof` set, and
its answer is a `post_identity` carrying the signature. No new datagram type,
no protocol version bump, no capability: a contact that has never heard of
presence answers correctly, because it is answering the request it already
implements.

- **What a valid proof establishes.** The holder of the contact's secret key
  processed this attempt. It is bound to a fresh per-attempt label and to the
  hash of this exact question, so a relay cannot forge it and a cache cannot
  replay it into another attempt. It contains no timestamp, so the honest
  reading is "alive within this attempt's window".
- **`ping` is not a substitute.** A pong proves a process holds a socket. The
  proof is a signature by the key owner.
- **Requirements.** The payload requires `target_proof` and nothing else.
  That name shipped with the `required` mechanism itself, so every build that
  can answer `get_identity` understands it. Adding a second requirement is
  forbidden: an unrecognised requirement obliges the target to stay silent,
  and silence from an old build is indistinguishable from silence from a dead
  one.
- **Cadence.** Base interval 150 s with ±25 % jitter (an exactly periodic
  emitter is a fingerprint), attempt timeout 30 s, validity 450 s, at most 8
  attempts in flight. A contact whose proof still has margin is not asked
  again, so an active conversation produces no probe traffic at all.
- **The renewal margin decides both the cadence and the safety of `online`.**
  A proof is renewed once less than 330 s of it is left, and that number is
  sized to fit MORE THAN ONE attempt: a contact must survive a lost renewal
  without falling out of `online`. The three-strike rule does not help here —
  it guards the way into `offline`, and nothing guards the way out of `online`.
  A margin that fits a single attempt also silently replaces the cadence,
  because the skip above suppresses every probe until it: probes then leave at
  validity minus the margin rather than at the base interval, and reaching
  `offline` takes far longer than validity × Detect Mult suggests.
- **Triggered probes.** A contact who enters `probing` is asked immediately
  rather than at the next periodic slot.
- **Silence counts only when the question left this node.** A send the layer
  refused is not an attempt at all, and neither is one the layer accepted and
  then lost — its own queue can drop a frame on its send deadline, and the
  writer can refuse it, neither of which reports back. Such an attempt is
  discarded rather than counted, because a strike is a claim about the CONTACT
  and this silence was ours.
- **Separate from identity resolution.** Both send `get_identity` and are
  told apart by whose per-attempt label an answer carries. A resolution ENDS
  when it succeeds and arms a cooldown; a liveness question never ends. An
  answer to a label the prober did not issue is passed on to the resolver
  untouched.

#### 4.1 The reciprocity claim

A probe may carry a `sealed` field: a payload encrypted to the target's box key
containing who is asking, an epoch, and a token.

- **The token** is a MAC over (epoch, attempt, asker, target) keyed by the
  X25519 shared secret of the two box keys. Computing it needs one of the two
  PRIVATE keys, so a valid one proves the sender holds the key material of the
  identity it names. It is bound to the direction, so an A→B token cannot be
  reflected as B→A, and to a coarse time window, so a captured one expires; the
  verifier accepts the neighbouring windows because neither side's clock is
  negotiated.
- **One claim answers one question.** The attempt label is inside the MAC, so a
  hop that captured a probe cannot move the ciphertext into a question of its
  own; and the target remembers the attempt labels it has answered for the
  length of the acceptance window, so it cannot re-present the identical
  request either. A repeat gets silence, like every other refusal here — saying
  "you already asked" would confirm both the identity and the earlier probe.
- **The asker's name travels INSIDE the ciphertext**, never in the plaintext
  `requester` triple. A plaintext requester would publish the pair (asker,
  target) to every hop the frame crosses — a stronger leak than the `routed`
  mode this plane avoids for exactly that reason.
- **What the target does with it.** No `sealed` at all is a public lookup and
  is answered exactly as before: the record, and the proof if asked for. This
  is what keeps first contact by 40-hex address and by `corsa:`-link working,
  and it is why the identity resolver — which always asks for a proof and
  refuses an answer without one — needed no change. A `sealed` claim whose
  token verifies against a contact's stored box key is answered the same way. A
  `sealed` claim that does not verify gets **silence**: not a refusal, which
  would confirm the identity exists.
- **What this does and does not close.** It closes the oracle on the PROBE
  path: a stranger cannot use a liveness probe to watch a contact. It does not
  close the timing oracle on the public lookup, where the mere fact of a prompt
  answer still shows the target processed the request. Closing that requires
  the public path to stop answering strangers, which trades away first contact
  and is a separate decision (owner, 2026-09-04: keep the public path open).

#### 4.2 First hops (guards)

A probe is handed to a neighbour, and that neighbour sees `dst`. The node
therefore chooses WHICH neighbours ever see that, and keeps choosing the same
ones.

- **Not rotation — pinning.** Each new first hop is an independent draw: with an
  adversary holding a share of the network, the per-draw risk is fixed and the
  probability of at least one bad draw rises towards one with the NUMBER of
  draws. A probe cadence produces dozens of sends an hour, so re-choosing per
  send reaches that limit fast. Tor measured this and reversed its own position;
  the guard model is what came out of it.
- **A persistent SAMPLED set**, capped both by count and by a share of the
  observed neighbourhood, with a floor so a very small neighbourhood still gets
  a policy. From it, three PRIMARY, of which one carries the traffic and the
  rest are a warm reserve.
- **Confirmed by an ANSWER, not by a send.** The class queue, its send
  deadline and the writer all sit between "the layer took the frame" and "the
  neighbour saw it", and none of them reports back — so a queued frame confirms
  nothing. A verified reply that came home through a hop does: it proves the
  frame crossed it. A guard that never carried anything has told nobody
  anything, which is the whole point of the distinction.
- **Blamed only for what it was OFFERED.** A guard with no route to a given
  destination never enters that send's candidate list, and the walk stops at
  the first acceptance, so both ends of the preference can go untried. Only the
  hops the walk really offered the frame to and that passed it over are held
  responsible; anything else puts a working neighbour into back-off, and since
  a guard in back-off is skipped, the set then replaces it — widening exposure
  through an accounting error.
- **A neighbour outside the set that carries a frame is recorded anyway.** The
  cap is a claim about how many neighbours ever learn we are asking about
  somebody, and one that carried a frame has learned exactly that. Leaving it
  out would make the stored set an understatement and the cap a number about
  bookkeeping. When the set is full such a use is counted instead, as the
  policy's own miss rate.
- **A failure does not change the set.** It arms a retry schedule. When OUR
  connectivity is what failed, not even that — otherwise one local outage
  rebuilds the whole set.
- **Dates are randomised**, because the moment of a change is itself metadata.
- **Inbound neighbours are preferred**: on a session THEY dialled, our identity
  was never proven to them.
- **Order comes from a sequence, never from a stored date.** Every date here is
  deliberately fuzzed, so ranking by one is ranking by a random number: a spare
  confirmed a minute after the primary would sort ahead of it about half the
  time, and the primary would never return to the front after a hiccup — a
  rotation produced by the anti-correlation measure itself.

The set is DURABLE, and its stored form carries the confirmation ORDER as well
as the dates. A node that re-sampled on every start would rotate its first hops
once per launch, which is the failure this exists to prevent; one that restored
the dates but not the order would re-rank itself from fuzzed timestamps on every
restart, which is the same failure a level down.

**The policy is checked by observation, not only by tests.** Its promises are
about rates — how often the leading hop changed, how far the set grew, how much
traffic left through a neighbour outside it — and every one of those failures
leaves an ordinary-looking set behind. The counters are published with the set
over local RPC for that reason.

Two limits are stated rather than implied. The preference is never a filter: a
guard with no route to the destination is passed over rather than turned into a
delivery failure. And it selects a NEIGHBOUR, not a connection — the send path
separately prefers the outbound tier when both exist, so a neighbour we also
dialled still receives the frame over the session where our identity is proven.
Closing either needs onion routing, which is out of scope here.

### 5. The route fallback (temporary)

A contact that cannot answer a probe at all — an older build, a node with the
datagram layer disabled, a contact whose identity record was never resolved —
is shown from the routing table, with source `route_fallback`.

- It is **online**, because a path exists and none of the negative rules
  applies; and it is **never proven**, so the interface must draw it
  differently from a witnessed presence — today as a striped green avatar
  against the plain green of a proven one.
- It is only for contacts that CANNOT be probed. A probeable contact that
  merely has not answered goes `offline` after three silent probes, even
  while a route to them remains in the table.
- It does not override rule 5: a route we suppressed stays `unknown`.

The fallback exists because the alternative — reporting nothing for most of
the network — replaces one wrong answer with another. It rests on this node
holding a full routing table, which the structural overlay removes by design,
and it is removed with it.

### 6. Presence and delivery

**Presence never gates sending.** A message to a contact believed absent is
sent exactly as before; delivery decides by asking routing whether a frame
can be handed over. Gating on presence would make every contact whose
liveness cannot be proven permanently unreachable.

**Presence does wake delivery.** A contact becoming present is the
centralized signal that a return happened, and it triggers both halves of the
existing return handling: what was held because the recipient was unreachable,
and what was already sent and never confirmed. Each then re-asks routing
itself. The route-driven wakeups remain as the fallback for contacts presence
cannot speak about.

They are **not** what survives the overlay cutover, and an earlier revision said
they were. Those wakeups are driven by routing EVENTS, so they disappear
together with the table that emits them — a mechanism cannot be its own
successor. What survives is presence; the route-driven half needs a replacement
built on whatever the overlay signals in place of a route appearing, and that
replacement is not designed yet.

A return is also recognised when an assumed presence becomes a proven one. A
contact whose route never disappeared is `online` by the fallback for the whole
time they are away, so their actual return changes no state and no routing
answer; the proof is the only event that marks it.

And a third case is recognised that neither of the first two can see: evidence
of life that ends a DOUBT without changing the state. A proof is valid for
450 s and a probe goes out every 150 s, so a contact can miss one or two probes
— being genuinely unreachable in between — and answer again while the earlier
window is still open. Both projections read `online`, and the route through a
transit hop never moved either, so the state comparison and the routing events
are blind to the SAME event. What marks it is that the evidence arrived while
strikes or a recorded close were outstanding; evidence that merely extends a
window is not a return, which is what keeps this a wake-up and not a poll.

What the delivery retry counts, though, is the ABSENCE and not the return, and
presence counts its own departures beside the evidence that records them — in
the same write, not as a separate step. Anything else leaves a window in which a
proof publishes a return that spends an occasion nobody has opened yet. A
message gets one accelerated attempt per departure, and any observer of the
return — the delivery pass measuring a path again, a session becoming usable, a
proof arriving — spends that one. Presence contributes the departures routing
cannot see: a probe going unanswered, or a session closing, while a transit
path stayed visible throughout, which is exactly the recipient whose
reachability never changes.

The side the counter lives on is not a detail. Counting RETURNS gives one
physical return as many accelerated attempts as there are observers of it,
because each observer is new to what the others spent; counting absences cannot
misfire that way, since several observations of one departure still leave a
single unspent occasion. It remains a wake-up in the sense above — what is
counted is whether this return has been answered, never whether a message may
go out.

**Planned: presence becomes the gate, `online` only.** Today a send proceeds in
every state — `probing` and `online` alike, and the rest too. The end state is
that only a proven `online` contact is sent to.

Its precondition is a COMPATIBILITY invariant, and it is **not** the condition
that retires the route fallback (§5). The two are different steps of the
overlay rollout and answer different questions: the fallback goes when this
node stops holding a full routing table, which says nothing about whether the
peers still in the network can answer a probe. Turning the gate on then would
leave every peer that cannot prove liveness permanently unreachable.

The invariant this needs is narrower: *every peer this node will accept a
session from is able to reach a proven `online`*, so that "not online" always
means "not there" and never "too old to say". That is a statement about the
accepted population, so it belongs to the protocol floor — it becomes true when
`MinimumProtocolVersion` is at or past the version that made probe support
mandatory.

### 7. Local RPC

`fetch_presence` returns a `presence` frame whose `presence` array carries one
row per contact:

| Field | Meaning |
|---|---|
| `identity` | 40-hex fingerprint |
| `state` | `unknown` / `offline` / `probing` / `online` |
| `source` | as in §2 |
| `reason` | present only when `state` is `unknown` |

The frame also carries `presence_generation`: the number of the projection the
rows came from, within the answering node's process, starting at 1. It describes
the WHOLE set and never a single row, and it is absent (zero) until the first
projection has run — which is not the same as an empty projection: nothing is
known, rather than nobody is present.

It is a COUNTER and not a timestamp on purpose. Two projections can carry the
same instant (on Windows the clock advances in steps longer than a projection
takes) and a clock can step backwards; either makes a genuinely newer projection
look stale to a reader that orders by time. A reader holding two answers keeps
the one with the higher generation and discards the other whole — the rows are
never merged per contact, because each answer already covers every contact.

The names are strings, not numbers: a state added later must decode to
`unknown` in an older reader rather than be mis-read as a neighbouring value.
An unrecognised source decodes to `none`, which is not proven, so an
uninterpretable row can never be rendered as evidence.

`fetch_reachable_ids` is unchanged and still answers the routing question.

The two are refreshed by SEPARATE events and may briefly come from different
generations. That is deliberate: each field has exactly one writer, which is
worth more than a shared instant. When both were refreshed together, two
independent subscribers each fetched outside the lock, and an older fetch could
land after a newer one and stick until the next event. Each field is internally
consistent; presence is what a person is shown, reachability is what delivery
asks.


`fetch_first_hop_guards` returns a `first_hop_guards` frame: one row per sampled
neighbour, plus the counters.

| Field | Meaning |
|---|---|
| `identity` | the neighbour, 40-hex fingerprint |
| `sampled_at` | when it entered the set, RFC3339 — fuzzed at the source, so a period rather than an instant |
| `confirmed_at` | when it first carried a frame; absent means it never has |
| `confirmed_seq` | the confirmation ORDER, which is what ranks the set. Published beside the date because the two can disagree: the date is fuzzed, the sequence is exact |
| `retry_at` | when a guard in back-off may be offered again; absent when it is not |
| `failures` | the consecutive-failure count behind `retry_at` |
| `inbound` | this neighbour dialled US — the direction our identity was never proven in |
| `primary` | eligible to carry traffic right now, computed against the live neighbourhood |

| Counter | What a rising value means |
|---|---|
| `admitted` | neighbours ever taken into the set |
| `confirmed` | guards that have carried a frame |
| `primary_changes` | how often the LEADING hop changed. Settles at a small number on a healthy node; one that keeps climbing is the rotation §4.2 exists to prevent |
| `back_offs` | failures that armed a retry delay |
| `outside_set_uses` | frames carried by a neighbour that was not in the set — the policy's own miss rate. A node where this dominates has a stated bound that is not holding |
| `retired` | entries dropped at the end of their lifetime |
| `cap`, `primary_target` | the constants the numbers above are read against, so a reader need not know the build |

The command is READ-ONLY and says so deliberately: an earlier revision built the
`primary` flags by running the selection, which sampled guards, moved the very
counters printed next to them and wrote to disk — so the first request could
report rows it had just created, and observing the policy changed it.

It is a LOCAL frame. It never crosses to a peer, what it names is already
visible to the operator in the peer list, and it says nothing about which
CONTACT any traffic concerned.

---

## Русский

### 1. Назначение

Этот документ — нормативная спецификация **присутствия контакта**: что узел
думает о том, здесь ли контакт сейчас, на чём это мнение стоит и в каком виде
пересекает границу локального RPC.

Присутствие отвечает на вопрос человека. Оно намеренно НЕ является маршрутным
вопросом. «Есть ли путь до X» отвечает таблица маршрутов, и она остаётся
входом для решений о доставке. Долгое время это было одно и то же значение —
поэтому контакт выглядел присутствующим до десяти минут после ухода: маршрут
переживает своего владельца.

Реализовано: модель состояний, атрибуция источника, liveness-проба, фаллбек на
маршрут, команда локального RPC, отображение в интерфейсе и запечатанная
заявка взаимности, гейтящая пробу (§4.1), и набор guard-ов первого хопа
(§4.2). В эту редакцию не входит: фиксированный паддинг.

### 2. Модель состояния

Присутствие контакта — два независимых факта.

**Состояние** — одно из четырёх:

| Состояние | Значение |
|---|---|
| `unknown` | Ответа нет. Это не ухудшенный `offline`; см. причину ниже. |
| `offline` | Считаем, что его нет, — по наблюдению О КОНТАКТЕ. |
| `probing` | Путь есть, живость ещё не установлена. |
| `online` | Считаем, что он здесь. |

**Источник** — на чём стоит состояние: `proof`, `passive`, `session_closed`,
`route_observation`, `probe_timeout`, `route_fallback` или `none`.

Доказанными являются только `proof` и `passive` — это подпись самого контакта.
Всё остальное — вывод этого узла. Различие нормативно, а не косметично:
интерфейс ОБЯЗАН рисовать доказанное присутствие иначе, чем выведенное.

Когда состояние `unknown`, **причина** говорит, какого рода это незнание:
`no_local_connectivity`, `route_suppressed_locally`, `stale`, `not_probeable`.

Идентичность без записи — `unknown`. Читать отсутствующую запись как `offline`
запрещено: так собственная авария узла превращается в утверждение обо всех
остальных.

### 3. Правила

Состояние выводится в следующем порядке. Порядок — часть контракта.

1. **Собственная связность гейтит всё.** Без единого подключённого пира пустая
   таблица маршрутов описывает ЭТОТ узел, поэтому все контакты — `unknown`
   (`no_local_connectivity`). Узел с одним пригодным соседом не гейтится: он
   по-прежнему маршрутизирует, пробит и принимает.
2. **Свидетельство от контакта сильнее любого вывода.** Валидный
   `target_proof` либо кадр, который контакт передал по СВОЕЙ аутентифицированной
   сессии, делает его `online` до истечения окна годности.

   Транзитная копия свидетельством **не** является, и это не педантизм: подпись
   доказывает, кто написал сообщение, а не то, что он не спит. Релей хранит и
   пересылает, поэтому транзитная копия может прийти сильно позже ухода автора —
   и приходить повторно, каждый раз продлевая окно, из-за чего контакт остался бы
   зелёным ровно столько, сколько сеть его переповторяет. Контакты, доступные
   только транзитом, покрываются пробой (она round-trip и не воспроизводится
   повтором) и фаллбеком (§5), пока отвечать на пробу не научатся.
3. **Подряд не отвеченные пробы — наблюдение о нём.** Три подряд (`Detect
   Mult`, RFC 5880 §6.8.4) дают `offline` (`probe_timeout`). Любое
   свидетельство жизни обнуляет счётчик. Проба, не дошедшая до сети, не
   считается: локальный отказ — не молчание контакта.
4. **Атрибутированное закрытие сессии немедленно прекращает зелёное.** Оно
   записывается в момент наблюдения, а НЕ при выполнении отложенного снятия
   маршрута: снятие отложено намеренно, чтобы реконнект не порождал шторм
   withdrawal, и перенос этой задержки на присутствие держал бы ушедший
   контакт зелёным весь grace-период.
   Считается закрытие ПОСЛЕДНЕЙ сессии контакта любого вида, а не последней
   relay-способной. Маршрутизация снимает маршрут по второму условию и права в
   этом; утверждение о человеке — не утверждение о его relay-способности, и
   вывод его из маршрутного условия был неверен в обе стороны: контакт, чей
   билд relay не умеет, не записывался ушедшим вовсе, а контакт, закрывший
   relay-сессию при живой второй, записывался ушедшим, пока мы с ним ещё
   разговаривали.
   Само по себе закрытие не утверждает отсутствие, пока виден путь: с путём
   контакт переходит в `probing` (и пробится немедленно), без пути — в
   `offline` (`session_closed`).
   Закрытие **исчерпано**, как только маршрут, против которого оно записано,
   был замечен исчезнувшим и появился новый, либо как только сессия с контактом
   снова поднялась, — зеркало того условия, по которому оно записывалось. Вторая
   половина закрывает контакт, у которого прямого маршрута нет вовсе и следить
   не за чем.
   Закрытие и поднявшуюся заново сессию упорядочивает **номер перехода**,
   выдаваемый там, где счётчик сессий пересекает ноль, и никогда не часы.
   События причинно упорядочены в мире и не упорядочены в процессе — закрытие
   идёт через учёт сессий, разбор соединения и маршрутную бухгалтерию, прежде
   чем дойти, — а часы этого не решают: показание приходится снимать до лока, в
   котором переход и определяется, поэтому перекрывающиеся сессии его
   инвертируют; грубые часы дают равенство; переведённые назад — переворачивают.
   Переход, чей номер не больше уже применённого, отбрасывается целиком. Номер
   записывается и тогда, когда реконнекту нечего отменять: это ровно тот случай,
   когда реконнект обогнал закрытие предыдущей сессии, и именно эта запись
   заставит закрытие проиграть, когда оно дойдёт.
   Против свидетельства ЖИЗНИ общей точки нет, поэтому каждое из этих событий
   несёт время, когда оно было НАБЛЮДЕНО, а не когда записано, и **то из них,
   что старше, отбрасывается целиком** — в обе стороны. Закрытие, положенное
   поверх более позднего доказательства, сереет контакт, который только что нам
   подписал; доказательство, положенное поверх более позднего закрытия или более
   поздней неотвеченной пробы, возвращает ушедший контакт в online на всё окно
   валидности. Оба порядка одинаково достижимы: горутины, которые их наблюдают,
   очереди не соблюдают. Ничья — за отрицательным наблюдением: ложное серое
   лечится следующей пробой, ложное зелёное живёт 450 с.
   Любой момент времени в присутствии — этот, окно валидности, таймаут пробы и
   каденция проб — читается из ОДНИХ часов и сохраняет монотонную составляющую
   от чтения до того, кто ею пользуется. Перевод в календарное время происходит
   только там, где момент покидает процесс, — в персистентности.
   В реализации эти правила несёт ТИП, а не соглашение: момент присутствия —
   не обычная временная метка, и написания, которые ломают правило (вычитание,
   сравнение как дедлайна, отбрасывание монотонной составляющей), у него просто
   отсутствуют.
   ДЛИТЕЛЬНОСТЬ же меряется как **та из двух, что увидела больше времени**:
   монотонная или стенная, потому что каждая закрывает слепое пятно другой.
   Монотонные часы останавливаются, пока машина спит, поэтому на них одних
   ноутбук, закрытый на три часа, просыпается с доказательством внутри окна
   450 с, с открытой пробой, которая никогда не истечёт, и с каденцией,
   продолжающейся как ни в чём не бывало, — то самое долгое ложное зелёное, ради
   устранения которого всё и делалось, и приходящее оно через обычное
   использование ноутбука. Стенные часы сон видят, но именно их портит перевод:
   на них одних переведённые назад часы продлевают окно и останавливают таймауты.
   Большая из двух верна в обоих случаях. Цена: часы, ошибочно прыгнувшие
   ВПЕРЁД, закрывают окно раньше — одна проба и один round trip против 450 с
   утверждения, что человек на связи, когда его нет.
5. **Маршрут, снятый НАМИ, не говорит о контакте ничего.** Карантин, flap
   hold-down, seq hold-down, вытеснение K-cap и неразрешимый next-hop дают
   `unknown` (`route_suppressed_locally`), но не `offline`. Они длятся до
   тридцати минут; сообщать их как отсутствие — получасовая ложь.
6. **Маршрут, исчезнувший при живой нашей связности, — `offline`**
   (`route_observation`). Это единственное, что присутствие всё ещё берёт из
   маршрутизации.
7. **Путь без доказательства** — `probing` для пробируемого контакта и фаллбек
   на маршрут (§5) для непробируемого.

### 4. Liveness-проба

Проба — обычная датаграмма `get_identity` с флагом `target_proof`, ответ —
`post_identity` с подписью. Нового типа датаграммы нет, версия протокола не
поднимается, capability не требуется: контакт, никогда не слышавший о
присутствии, отвечает корректно, потому что отвечает на запрос, который уже
умеет.

- **Что доказывает валидный proof.** Владелец секретного ключа контакта
  обработал именно эту попытку. Подпись привязана к свежему ярлыку попытки и к
  хешу этого вопроса, поэтому релей не подделает, а кэш не переиграет её в
  другую попытку. Метки времени внутри нет, поэтому честное чтение — «жив
  внутри окна этой попытки».
- **`ping` не заменяет пробу.** Pong доказывает, что процесс держит сокет.
  Proof — подпись владельца ключа.
- **Требования.** Payload требует `target_proof` и больше ничего. Это имя
  появилось вместе с самим механизмом `required`, поэтому его понимает любая
  сборка, способная ответить на `get_identity`. Добавлять второе требование
  запрещено: непонятое требование обязывает адресата молчать, а молчание
  старой сборки неотличимо от молчания мёртвой.
- **Каденция.** База 150 с с джиттером ±25 % (ровная периодичность сама по
  себе является отпечатком узла), таймаут попытки 30 с, годность 450 с, не
  более 8 попыток одновременно. Контакт, чьё доказательство ещё имеет запас,
  повторно не спрашивается — поэтому активная переписка не порождает проб
  вовсе.
- **Запас на продление задаёт и каденцию, и устойчивость `online`.**
  Доказательство продлевают, когда его остаётся меньше 330 с, и это число
  подобрано так, чтобы вместить БОЛЬШЕ ОДНОЙ попытки: контакт обязан пережить
  потерянное продление, не выпав из `online`. Правило трёх страйков здесь не
  помогает — оно охраняет вход в `offline`, а выход из `online` не охраняет
  ничто. Запас, вмещающий одну попытку, вдобавок незаметно подменяет каденцию:
  пропуск выше подавляет все пробы до него, поэтому пробы уходят не через
  базовый интервал, а через «годность минус запас», и до `offline` дело
  доходит куда позже, чем следует из «годность × Detect Mult».
- **Немедленные пробы.** Контакт, попавший в `probing`, спрашивается сразу, а
  не в следующем периодическом слоте.
- **Молчание считается, только если вопрос покинул узел.** Отправка, в которой
  слой отказал, попыткой не является; не является ею и та, которую слой принял,
  а затем потерял — его собственная очередь может выбросить кадр по дедлайну, а
  writer может его не взять, и ни то ни другое не сообщается обратно. Такая
  попытка отбрасывается, а не засчитывается: страйк — это утверждение о
  КОНТАКТЕ, а это молчание было нашим.
- **Отдельно от резолвинга identity.** Оба шлют `get_identity` и различаются
  тем, чей ярлык попытки несёт ответ. Резолюция ЗАВЕРШАЕТСЯ при успехе и
  взводит cooldown; вопрос о живости не завершается никогда. Ответ на ярлык,
  который пробер не выдавал, передаётся резолверу нетронутым.

#### 4.1 Заявка взаимности

Проба может нести поле `sealed`: полезную нагрузку, зашифрованную на box-ключ
цели, внутри которой — кто спрашивает, эпоха и токен.

- **Токен** — это MAC от (эпоха, попытка, спрашивающий, цель) на ключе,
  выведенном из общего секрета X25519 двух box-ключей. Чтобы его вычислить,
  нужен один из двух ПРИВАТНЫХ ключей, поэтому валидный токен доказывает, что
  отправитель владеет ключевым материалом названной им идентичности. Он
  привязан к направлению, поэтому токен A→B нельзя отразить как B→A, и к
  грубому окну времени, поэтому перехваченный протухает; проверяющий принимает
  соседние окна, потому что часы сторон не согласуются.
- **Одна заявка отвечает на один вопрос.** Метка попытки входит в MAC, поэтому
  хоп, перехвативший пробу, не может перенести шифротекст в собственный вопрос;
  а цель помнит отвеченные метки попыток на длину окна приёма, поэтому повторно
  предъявить тот же самый запрос он тоже не может. Повтор получает молчание,
  как и любой другой отказ здесь: ответ «ты уже спрашивал» подтвердил бы и
  идентичность, и предыдущую пробу.
- **Имя спрашивающего едет ВНУТРИ шифротекста**, а не в открытой тройке
  `requester`. Открытый requester опубликовал бы пару (спрашивающий, цель)
  каждому хопу на пути — утечка сильнее, чем у режима `routed`, которого эта
  плоскость избегает ровно по той же причине.
- **Что делает цель.** Отсутствие `sealed` — это публичный lookup, и он
  отвечается ровно как раньше: запись и, если просили, proof. Именно это
  сохраняет работу первого контакта по 40-hex адресу и по `corsa:`-ссылке, и
  поэтому резолверу identity — который всегда просит proof и отвергает ответ
  без него — менять ничего не пришлось. Заявка, чей токен сходится с
  сохранённым box-ключом контакта, отвечается так же. Заявка, чей токен не
  сходится, получает **молчание**: не отказ, потому что отказ подтвердил бы,
  что идентичность существует.
- **Что это закрывает и что нет.** Закрывает oracle на пути ПРОБЫ: чужой не
  может следить за контактом с помощью liveness-пробы. Не закрывает временной
  oracle на публичном lookup, где сам факт быстрого ответа по-прежнему
  показывает, что цель обработала запрос. Закрытие последнего требует, чтобы
  публичный путь перестал отвечать незнакомцам, — это размен на первый контакт
  и отдельное решение (владелец, 2026-09-04: публичный путь оставляем
  открытым).

#### 4.2 Первые хопы (guards)

Проба передаётся соседу, и этот сосед видит `dst`. Поэтому узел выбирает, КАКИЕ
соседи вообще это увидят, и продолжает выбирать одних и тех же.

- **Не ротация, а закрепление.** Каждый новый первый хоп — независимый бросок:
  при противнике, держащем долю сети, риск на бросок фиксирован, а вероятность
  хотя бы одного плохого броска растёт к единице с ЧИСЛОМ бросков. Каденция
  пробы даёт десятки отправок в час, поэтому выбор заново на каждую отправку
  достигает этого предела быстро. Tor это измерил и развернул собственную
  позицию; модель guard-ов — то, что из этого вышло.
- **Персистентный набор SAMPLED**, ограниченный и по числу, и по доле
  наблюдаемого окружения, с полом — чтобы у очень маленького окружения политика
  всё-таки была. Из него три PRIMARY, из которых один везёт трафик, а остальные
  — горячий резерв.
- **Подтверждение ОТВЕТОМ, а не отправкой.** Между «слой принял кадр» и «сосед
  его увидел» стоят очередь классов, её дедлайн и writer, и ни один из них не
  отчитывается обратно, — поэтому поставленный в очередь кадр не подтверждает
  ничего. Проверенный ответ, вернувшийся через хоп, подтверждает: он
  доказывает, что кадр через него прошёл. Guard, который ничего не вёз, никому
  ничего не сообщил — ради этого различия всё и делается.
- **Винить можно только за то, что ПРЕДЛАГАЛИ.** Guard без маршрута до данной
  цели вообще не попадает в список кандидатов этой отправки, а обход
  останавливается на первом принявшем, поэтому непроверенными могут остаться оба
  конца предпочтения. Отвечают только те хопы, которым обход действительно
  предложил кадр и которые его не взяли; всё прочее загоняет рабочего соседа в
  backoff, а поскольку guard в backoff пропускается, набор его заменяет —
  расширяя экспозицию из-за ошибки учёта.
- **Сосед вне набора, который повёз кадр, всё равно записывается.** Потолок —
  это утверждение о том, сколько соседей вообще узнаёт, что мы о ком-то
  спрашиваем, а повёзший кадр узнал ровно это. Не записать его значило бы
  занизить сохранённый набор и превратить потолок в число про бухгалтерию. Если
  набор уже полон, такое использование считается отдельно — как собственный
  процент промахов политики.
- **Отказ не меняет набор.** Он взводит retry-график. А когда отказала НАША
  связность — не взводит и его: иначе один локальный сбой перестроит весь набор.
- **Даты рандомизируются**, потому что момент смены сам по себе метаданное.
- **Входящие соседи предпочтительнее**: на сессии, которую набрали ОНИ, наша
  identity им не доказывалась.
- **Порядок задаёт последовательность, а не сохранённая дата.** Все даты здесь
  намеренно зафуззены, поэтому сортировка по дате — это сортировка по
  случайному числу: резерв, подтверждённый через минуту после основного, встанет
  впереди него примерно в половине случаев, и основной уже не вернётся в начало
  после сбоя — ротация, порождённая самой мерой против корреляции.

Набор ДОЛГОВЕЧЕН, и его сохранённая форма несёт ПОРЯДОК подтверждения наравне с
датами. Узел, пересобирающий набор при каждом старте, ротировал бы первые хопы
раз в запуск — ровно тот провал, ради которого всё это и есть; узел, восстановивший
даты, но не порядок, переранжировал бы себя по зафуззенным меткам при каждом
рестарте — тот же провал этажом ниже.

**Политика проверяется наблюдением, а не только тестами.** Её обещания — про
частоты: как часто менялся ведущий хоп, насколько вырос набор, сколько трафика
ушло через соседа вне набора. Каждый из этих отказов оставляет за собой
совершенно обычный на вид набор, поэтому счётчики публикуются вместе с ним по
локальному RPC.

Два ограничения названы прямо, а не подразумеваются. Предпочтение никогда не
фильтр: guard без маршрута до цели пропускается, а не превращается в отказ
доставки. И оно выбирает СОСЕДА, а не соединение — путь отправки отдельно
предпочитает исходящий тир, когда есть оба, поэтому сосед, которому мы тоже
звонили, всё равно получит кадр по сессии, где наша identity доказана. Закрытие
любого из двух требует onion-маршрутизации и в этот документ не входит.

### 5. Фаллбек на маршрут (временный)

Контакт, который в принципе не может ответить на пробу — старая сборка, узел с
выключенным слоем датаграмм, контакт, чью identity-запись мы не резолвили, —
показывается по таблице маршрутов с источником `route_fallback`.

- Он **online**, потому что путь есть и ни одно из отрицательных правил не
  сработало; и он **никогда не доказан**, поэтому интерфейс обязан рисовать его
  иначе, чем засвидетельствованное присутствие, — сегодня полосатым зелёным
  против сплошного зелёного у доказанного.
- Он только для тех, кого нельзя пробить. Пробируемый контакт, который просто
  не ответил, уходит в `offline` после трёх молчаливых проб — даже если
  маршрут к нему остаётся в таблице.
- Он не отменяет правило 5: маршрут, снятый нами, остаётся `unknown`.

Фаллбек существует потому, что альтернатива — не показывать ничего о большей
части сети — меняет один неверный ответ на другой. Он стоит на том, что узел
держит полную таблицу маршрутов, — а структурный оверлей убирает её намеренно,
и вместе с ней уходит фаллбек.

### 6. Присутствие и доставка

**Присутствие никогда не гейтит отправку.** Сообщение контакту, которого мы
считаем отсутствующим, отправляется ровно как раньше; доставка решает,
спрашивая у маршрутизации, есть ли куда отдать кадр. Гейт по присутствию сделал
бы недостижимым навсегда любой контакт, чью живость нельзя доказать.

**Присутствие будит доставку.** Появление контакта — централизованный сигнал о
возвращении, и он запускает обе половины существующей обработки возврата: то,
что удерживалось из-за недостижимости получателя, и то, что уже отправлено и не
подтверждено. Каждая затем сама переспрашивает маршрутизацию. Маршрутные
будильники остаются как фаллбек для контактов, о которых присутствие ничего
сказать не может.

Они **не** то, что переживает переход на оверлей, — прошлая редакция утверждала
обратное. Эти будильники приводятся в действие маршрутными СОБЫТИЯМИ, поэтому
исчезают вместе с таблицей, которая их порождает: механизм не может быть
собственным преемником. Переживает присутствие; маршрутной половине нужна
замена, построенная на том, что оверлей сигналит вместо появления маршрута, и
эта замена ещё не спроектирована.

Возвращением считается и превращение предполагаемого присутствия в доказанное.
Контакт, чей маршрут не исчезал, всё время отсутствия остаётся `online` по
фаллбеку — поэтому его настоящее возвращение не меняет ни состояния, ни
маршрутного ответа; единственное событие, отмечающее этот момент, — proof.

И распознаётся третий случай, невидимый для первых двух: свидетельство жизни,
закрывающее СОМНЕНИЕ, но не меняющее состояния. Доказательство живёт 450 с, а
проба уходит раз в 150 с, поэтому контакт может пропустить одну-две пробы —
будучи в этот промежуток по-настоящему недостижим — и ответить снова, пока
прежнее окно ещё открыто. Обе проекции читаются как `online`, а маршрут через
транзитный хоп тоже никуда не девался: и сравнение состояний, и маршрутные
события слепы к ОДНОМУ И ТОМУ ЖЕ событию. Отмечает его то, что свидетельство
пришло при непогашенных страйках или записанном закрытии сессии; свидетельство,
которое лишь продлевает окно, возвращением не является — именно это удерживает
механизм будильником, а не поллингом.

Считает при этом ретрай доставки не возвращения, а ОТСУТСТВИЯ, и присутствие
ведёт счёт своих уходов рядом со свидетельством, которое их записывает, — одной
записью, а не отдельным шагом. Иначе остаётся окно, в котором доказательство
публикует возвращение, тратящее повод, которого ещё никто не открыл. Сообщение
получает одну ускоренную попытку на один уход, и тратит её любой наблюдатель
возвращения — проход доставки, снова увидевший путь; ставшая пригодной сессия;
пришедшее доказательство. Присутствие поставляет те уходы, которых не видит
маршрутизация: молчание на пробу или закрытие сессии при всё это время видимом
транзитном пути — то есть ровно того получателя, чья достижимость не меняется.

Сторона, на которой живёт счётчик, — не деталь. Счёт ВОЗВРАЩЕНИЙ выдаёт одному
физическому возвращению столько ускоренных попыток, сколько у него нашлось
наблюдателей: каждый из них не знает, что потратили остальные. Счёт отсутствий
так промахнуться не может — несколько наблюдений одного ухода всё равно
оставляют один непотраченный повод. Будильником это остаётся в прежнем смысле —
считается, отвечено ли на это возвращение, и никогда не то, можно ли отправлять.

**Планируется: присутствие станет гейтом, и только `online`.** Сегодня отправка
идёт при любом состоянии — и при `probing`, и при `online`, и при остальных.
Целевое состояние: отправлять только доказанно `online`-контакту.

Его предусловие — инвариант СОВМЕСТИМОСТИ, и это **не** то условие, по которому
удаляется фаллбек (§5). Это разные шаги перехода на оверлей и разные вопросы:
фаллбек уходит, когда узел перестаёт держать полную таблицу маршрутов, — что
ничего не говорит о том, умеют ли оставшиеся в сети пиры отвечать на пробу.
Включение гейта в этот момент оставило бы каждого, кто не может доказать
живость, недостижимым навсегда.

Нужный инвариант уже: *каждый пир, с которым узел вообще согласится установить
сессию, способен достичь доказанного `online`*, чтобы «не online» всегда значило
«его нет», а не «слишком стар, чтобы сказать». Это утверждение о принимаемой
популяции, поэтому оно принадлежит порогу протокола — становится истинным, когда
`MinimumProtocolVersion` достигает версии, сделавшей поддержку пробы
обязательной.

### 7. Локальный RPC

`fetch_presence` возвращает кадр `presence`, в массиве `presence` — по строке на
контакт:

| Поле | Значение |
|---|---|
| `identity` | отпечаток, 40 hex |
| `state` | `unknown` / `offline` / `probing` / `online` |
| `source` | как в §2 |
| `reason` | только когда `state` равен `unknown` |

Кадр несёт также `presence_generation` — номер проекции, из которой взяты
строки, в пределах процесса отвечающего узла, начиная с 1. Он описывает ВЕСЬ
набор, а не отдельную строку, и отсутствует (равен нулю) до первой проекции —
что не то же самое, что пустая проекция: ничего не известно, а не «никого нет».

Это СЧЁТЧИК, а не метка времени, и намеренно. Две проекции могут получить один и
тот же момент (на Windows часы идут шагами длиннее, чем занимает проекция), а
часы могут откатиться назад; в обоих случаях действительно новая проекция
выглядит устаревшей для читателя, упорядочивающего по времени. Читатель,
держащий два ответа, оставляет тот, у кого поколение больше, и отбрасывает
второй целиком — строки не сливаются по контактам, потому что каждый ответ и так
покрывает всех.

Имена — строки, а не числа: состояние, добавленное позже, должно в старом
читателе декодироваться в `unknown`, а не быть прочитанным как соседнее
значение. Нераспознанный источник декодируется в `none`, который не является
доказанным, поэтому неинтерпретируемая строка никогда не будет показана как
свидетельство.



`fetch_reachable_ids` не изменился и по-прежнему отвечает на маршрутный вопрос.

Обновляются они РАЗНЫМИ событиями и могут кратко приходить из разных поколений.
Это осознанно: у каждого поля ровно один писатель, и это дороже общего мгновения.
Когда оба обновлялись вместе, два независимых подписчика забирали снапшот вне
локов, и более старая выборка могла лечь поверх более новой и застрять до
следующего события. Каждое поле внутренне согласовано; присутствие — то, что
показывают человеку, достижимость — то, что спрашивает доставка.

`fetch_first_hop_guards` возвращает кадр `first_hop_guards`: по строке на
каждого соседа в наборе плюс счётчики.

| Поле | Значение |
|---|---|
| `identity` | сосед, отпечаток 40 hex |
| `sampled_at` | когда попал в набор, RFC3339 — зафуззено у источника, поэтому период, а не момент |
| `confirmed_at` | когда впервые повёз кадр; отсутствует — не вёз ни разу |
| `confirmed_seq` | ПОРЯДОК подтверждения, который и ранжирует набор. Публикуется рядом с датой именно потому, что они могут расходиться: дата зафуззена, последовательность точна |
| `retry_at` | когда guard в backoff можно предложить снова; отсутствует, если он не в backoff |
| `failures` | счётчик подряд идущих отказов, стоящий за `retry_at` |
| `inbound` | этот сосед набрал НАС — направление, в котором наша identity ему не доказывалась |
| `primary` | пригоден везти трафик прямо сейчас; вычисляется по живому окружению |

| Счётчик | Что значит его рост |
|---|---|
| `admitted` | сколько соседей вообще попадало в набор |
| `confirmed` | guard-ы, которые везли кадр |
| `primary_changes` | как часто менялся ВЕДУЩИЙ хоп. На здоровом узле останавливается на малом числе; растущее — та самая ротация, ради предотвращения которой существует §4.2 |
| `back_offs` | отказы, взведшие задержку повтора |
| `outside_set_uses` | кадры, увезённые соседом вне набора — собственный процент промахов политики. Узел, где это доминирует, имеет заявленную границу, которая не держится |
| `retired` | записи, выброшенные по истечении срока жизни |
| `cap`, `primary_target` | константы, относительно которых читаются числа выше, чтобы читателю не требовалось знать сборку |

Команда ТОЛЬКО ЧИТАЕТ, и это сказано намеренно: прошлая редакция строила флаги
`primary`, запуская отбор, — а он добирал guard-ов, двигал те самые счётчики,
что печатались рядом, и писал на диск, так что первый же запрос мог показать
строки, которые сам только что создал, а наблюдение за политикой меняло её.

Кадр ЛОКАЛЬНЫЙ. Он не уходит к пиру, названное в нём и так видно оператору в
списке пиров, и он ничего не говорит о том, какого КОНТАКТА касался трафик.
