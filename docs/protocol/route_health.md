# Route Health, Probes, and Targeted Queries (Phase 2)

## English

This document specifies the Phase 2 wire frames and state machine that enrich
the Corsa routing plane with active reachability signals beyond plain
hop-count. It supersedes the original iter-1.5 sketch in `docs/roadmap.md`
which described the same protocol against a pre-Phase-1
`(Identity, Origin, NextHop)` triple storage model — Phase 2 uses the
post-Phase-1 `(Identity, Uplink)` pair model (see `docs/routing.md` "Storage
model").

### Scope

Phase 2 introduces:

- A per-`(Identity, Uplink)` health state machine
  (`Good → Questionable → Bad → Dead`) driven by `relay_hop_ack` reception
  and the active probe path defined below.
- An RTT estimate per `(Identity, Uplink)` maintained as an exponentially
  weighted moving average (EWMA, α=0.3). The primary sample source
  is the active probe round-trip (sender-measured at
  `route_probe_v1` send → `route_probe_ack_v1` receive in
  `probeRegistry.Resolve`). The hop-ack API (`Table.MarkHopAck`,
  `Table.ConfirmHopAck`) accepts a non-zero `rtt` argument for
  future plumbing, but the production node path currently passes
  `rtt=0` (`internal/core/node/routing_hop_ack.go::confirmRouteViaHopAck`)
  because the original message send timestamp is not threaded
  through the relay pipeline; `rtt=0` preserves the prior EWMA
  value untouched, so the field never carries a forged sample.
- A composite ranking score that replaces the legacy by-hops sort in
  `routing.Table.Lookup`. Hops, RTT, health state, and source trust
  combine additively (see `docs/routing.md` "Composite scoring" section).
- Two new wire frame pairs gated by independent capabilities:
  - `route_probe_v1` / `route_probe_ack_v1` (gated by
    `mesh_route_probe_v1`) — active reachability checks.
  - `route_query_v1` / `route_query_response_v1` (gated by
    `mesh_route_query_v1`) — on-demand single-hop route lookup used for
    fast recovery after all known uplinks for an identity transition to
    Bad/Dead.

Phase 2 is **additive on the wire**. New capabilities (`mesh_route_probe_v1`,
`mesh_route_query_v1`) gate the new frame pairs only — peers that did not
negotiate them never receive probes or queries. Local health bookkeeping
runs unconditionally on the receiver for LEARNED claims: every accepted
learned `(Identity, Uplink)` claim is seeded as `Questionable` in
`RouteHealthState` and aged through the passive timeline (Good →
Questionable → Bad → Dead) regardless of the announcing peer's capability
set. Direct (own-origin, `Identity == Uplink`) claims are the exception:
`AddDirectPeer` seeds them as `HealthGood` because the session-establishment
handshake is the proof of liveness, and `TickHealth` skips Direct pairs
entirely — their lifecycle is session-bound (`AddDirectPeer` /
`RemoveDirectPeer`), not traffic-driven. A quiet direct peer stays `Good`
for the lifetime of the session. See the "Capability gating" section for
the full mixed-version contract.

### Health state machine

`RouteHealthState` lives in `routing.Table` under `t.mu` and is keyed
per `(Identity, Uplink)` pair. Transitions are driven by:

- **Hop-ack reception** (passive): a `relay_hop_ack` arriving for a
  message routed to `Identity` through `Uplink` is the strongest
  passive confirmation. Always restores `Good`, resets
  `ProbeFailures`, refreshes `LastHopAck`.
- **Probe-ack reception** (active): a `route_probe_ack_v1` carrying
  `reachable=true` behaves like hop-ack reception. `reachable=false`
  keeps the uplink itself alive (it answered) but increments
  `ProbeFailures` — crossing the threshold (3 consecutive failures)
  forces `Bad` regardless of the hop-ack timeline.
- **Probe timeout** (active): no `route_probe_ack_v1` arrived within
  `HealthProbeTimeout` (5 s). Increments `ProbeFailures`.
- **Periodic idle tick**: every `HealthProbeInterval` (15 s) the
  health refresher walks tracked pairs and applies the passive
  timeline:
  - `Good → Questionable` when hop-ack idle > 60 s.
  - `Questionable → Bad` when hop-ack idle > 122 s.
  - `Bad → Dead` when hop-ack idle > 182 s.

The active and passive timelines are deliberately asymmetric. Three
probe failures reach `Bad` in ~45 s; the same transition through the
passive idle path takes 122 s. Active checks should detect failure
faster than passive observation — gaps in passive traffic can be
legitimate, while a probe answered with `reachable=false` (or no
answer at all within 5 s) is a stronger signal.

A `Dead` pair is filtered out of `Lookup` entirely AND from the
announce-side projection (`AnnounceableFor` / `AnnounceProjectionFor`)
so neighbours stop refreshing the broken path through us. The
exemption is `RouteSourceDirect`: Direct (own-origin) claims are
session-bound — their lifecycle is driven by AddDirectPeer /
RemoveDirectPeer, and a `Dead` label on a Direct pair signals
"no organic hop_ack lately", not "session is gone". Excluding
Direct from Lookup would force the relay path to fall back to
gossip while announce keeps advertising the same Direct route;
silently dropping Direct from announce would deny neighbours an
authoritative withdrawal. Direct Dead therefore stays in both
sides; ranking applies the `Bad`-equivalent penalty
(`scoreHealthBadPenalty`) rather than the `Dead`-as-sentinel −1
so a confirmed alternative wins but Direct remains selectable
as last resort.

`Bad` and `Questionable` remain selectable as a last resort but
sit strictly below every higher-tier alternative on the composite
score (see scoring below). The strict-tier ordering is a
release-blocking invariant: TableRouter triggers
`route_query_v1` recovery on every Bad-route send under the
assumption that selecting Bad means no Good / Questionable
alternative exists. The penalty magnitudes (`scoreHealthQPenalty`
= 250, `scoreHealthBadPenalty` = 500) are sized to dominate the
combined hops / RTT / source within-tier spread.

#### Transition table

| From | To | Trigger |
|---|---|---|
| any | `Good` | `relay_hop_ack` received OR `route_probe_ack_v1(reachable=true)` |
| `Good` | `Questionable` | hop-ack idle > 60 s |
| `Questionable` | `Bad` | hop-ack idle > 122 s OR `ProbeFailures >= 3` |
| `Bad` | `Dead` | hop-ack idle > 182 s |
| `Bad` | `Questionable` | `route_probe_ack_v1(reachable=false)` arrived (peer alive, just no route) |
| `Dead` | — | Dead is **sticky** under negative evidence. `route_probe_ack_v1(reachable=false)`, probe timeouts, and idle ticks do NOT pull Dead back to Questionable — only a real confirmation (`relay_hop_ack` or `route_probe_ack_v1(reachable=true)`) can resurrect a Dead pair to Good. Rationale: `Lookup` filters Dead from selection, so an answer of "I cannot reach the target through this uplink" must not silently re-enable a route that was previously excluded — otherwise a flapping peer can keep oscillating an evicted route back into the candidate set. |

### RTT EWMA

`UpdateRTT(prev, sample)` folds a fresh round-trip measurement into
the running average:

```
new = prev × (1−α) + sample × α
where α = 0.3
```

Cold-start: when `prev = 0` (no measurement yet) the function returns
`sample` verbatim. Outlier risk is acceptable because the EWMA
converges within ~5 samples to within 5% of a stable baseline.

`sample ≤ 0` is treated as "no useful measurement" and preserves
`prev` untouched. Senders compute `sample` as `now() − probeSentAt`
(or `now() − relaySentAt` for hop-ack ingest in the relay path).
**Senders MUST NOT trust the `rtt_ms` field on incoming
`route_probe_ack_v1` for their own ranking decisions** — that field
carries the responder's local estimate as informational, but the
zero-trust budget requires sender-measured round-trip time for any
quality decision.

### Composite scoring

`CompositeScore(hops, source, health)` produces a float ranking
score, higher is better:

```
score = 100 − hops × 10
      + rttBonus(health.RTT)
      + healthPenalty(health.Health)
      + sourceBonus(source)
```

- `rttBonus`: +30 when RTT ≤ 20 ms, linear taper to 0 at 100 ms, 0
  above 100 ms.
- `healthPenalty`: 0 for `Good`, −250 for `Questionable`, −500 for
  `Bad`, and `−1` sentinel (excluded from selection) for `Dead`.
  The Questionable / Bad penalties strictly dominate the within-tier
  spread (≈190 units across hops ∈ [1,15], RTT, source), giving
  strict-tier ordering: every Good outranks every Questionable, and
  every Questionable outranks every Bad. The TableRouter relay path
  relies on this invariant — a "selected Bad route" reading is the
  recovery trigger for `route_query_v1`.
- `sourceBonus`: +20 for `Direct` or `Local`, +10 for `HopAck`, 0
  for `Announcement`.

`Lookup` filters out transit (`Source != RouteSourceDirect`)
claims whose health is `Dead` and ranks the rest in descending
score order. Direct (own-origin) claims are exempt from the Dead
filter — Direct lifecycle is session-bound, so a Dead label
signals "no recent organic hop_ack" rather than "session is gone".
When a Direct claim is Dead the ranking substitutes the `Bad`
penalty instead of the excluded sentinel (−1) so the claim stays
selectable as last resort but loses to any confirmed alternative. The same Dead-filter with Direct exemption applies on
the announce projection (`AnnounceableFor` / `AnnounceProjectionFor`)
so neighbours and the local relay path agree on which routes are
selectable. The synthetic self-route (`Source=Local`, `Hops=0`)
naturally ranks first because its component values dominate any
storage-side claim.

Nil-health safe: when no health state has been recorded yet for a
pair, the formula skips the RTT and health-penalty terms and falls
back to `base + sourceBonus`. This reproduces the pre-Phase-2
`isBetter`-style ordering modulo trust-tier ties.

### `route_probe_v1` / `route_probe_ack_v1` wire format

#### Request (`route_probe_v1`)

```json
{
  "type":            "route_probe_v1",
  "probe_id":        12345678,
  "target_identity": "<X_fp>"
}
```

| Field | Type | Description |
|---|---|---|
| `type` | string | Must equal `"route_probe_v1"`. |
| `probe_id` | uint64 | Sender-chosen opaque correlator. Echoed unchanged in the ack. The minted ID is non-zero so that wire frames with `probe_id=0` (the JSON default for a missing field) are unambiguously distinct from a registered probe. |
| `target_identity` | string | Ed25519 fingerprint of the destination whose reachability is being probed. Required. |

#### Response (`route_probe_ack_v1`)

```json
{
  "type":       "route_probe_ack_v1",
  "probe_id":   12345678,
  "reachable":  true,
  "rtt_ms":     45
}
```

| Field | Type | Description |
|---|---|---|
| `type` | string | Must equal `"route_probe_ack_v1"`. |
| `probe_id` | uint64 | Echo of the request's `probe_id`. |
| `reachable` | bool | True when the responder's local `routing.Table.Lookup(target_identity)` returned at least one non-Dead route. False otherwise. |
| `rtt_ms` | uint32 | Responder's local RTT estimate (best-effort, informational). Senders MUST NOT trust this for their own ranking — measure locally as `now() − probeSentAt`. |

#### Probe protocol

- Probes are scheduled by the sender every `HealthProbeInterval`
  (15 s) for every `(Identity, Uplink)` pair currently in
  `Questionable`. A pair never has more than one outstanding probe
  at a time; the sender's outstanding-probe registry is the
  in-process bookkeeping.
- Verify-before-trust for newly observed uplinks is implemented by
  seeding the freshly admitted `(Identity, Uplink)` pair as
  `Questionable` (see `Table.UpdateRoute` in
  `internal/core/routing/table_mutation.go`). The probe-sender
  ticker picks it up on its next 15 s tick and sends the first
  probe then; there is no immediate one-shot probe at admission
  time. The pair is ranked with the `Questionable` penalty
  (`scoreHealthQPenalty` in `CompositeScore`) until that first
  probe ack lands, so an unverified uplink cannot outrank any
  Good alternative regardless of its hop count, RTT, or source.
- Sending honours the Phase 0 overload gate. When the host is in
  overload (goroutine count above operator-configured threshold),
  only the ACTIVE probe send phase is skipped — the passive
  `TickHealth()` sweep that ages every tracked pair through
  `applyIdleTick` still runs at the top of the probe tick (see
  `internal/core/node/routing_probe_loop.go::probeTick`), so the
  state machine keeps advancing on the Good→Questionable→Bad→Dead
  timeline. Affected pairs that would have been probed during
  overload converge to `Bad` through the slower passive 122 s
  timeline instead of via the active 45 s probe path.
- Probes are strictly single-hop. Receivers MUST NOT forward
  `route_probe_v1` to any other peer.
- An ack the sender cannot resolve — either because the `probe_id`
  is not in the outstanding registry (typically stale ack after
  the timeout watcher fired) or because it arrived from a peer
  different from the original probe's target uplink (potential
  spoof) — is dropped without state mutation and logged at debug
  level. The two cases collapse into one log line by design:
  `probeRegistry.ResolveMatching` returns `ok=false` for both and
  intentionally hides which one occurred, because distinguishing
  them would either require leaking registry internals into the
  handler or providing a side channel for an attacker probing the
  monotonic `probe_id` space. The pending registry entry for the
  real probe is left intact in the mismatched-uplink case so the
  legitimate ack (or its timeout) still fires unchanged. This is
  the receive-side zero-trust guard against ack spoofing.

### `route_query_v1` / `route_query_response_v1` wire format

#### Request (`route_query_v1`)

```json
{
  "type":            "route_query_v1",
  "query_id":        87654321,
  "target_identity": "<X_fp>",
  "max_hops":        8,
  "issued_at":       "2026-05-23T12:00:00Z"
}
```

| Field | Type | Description |
|---|---|---|
| `type` | string | Must equal `"route_query_v1"`. |
| `query_id` | uint64 | Sender-chosen correlator. Echoed in the response. Non-zero by convention. |
| `target_identity` | string | Ed25519 fingerprint of the destination being queried. Required. |
| `max_hops` | uint8 | Optional. Advisory cap on transit depth; responder still performs a single-hop Lookup, this field is informational for future protocol extensions. |
| `issued_at` | string | Optional RFC 3339 timestamp for freshness. |

#### Response (`route_query_response_v1`)

```json
{
  "type":            "route_query_response_v1",
  "query_id":        87654321,
  "target_identity": "<X_fp>",
  "found":           true,
  "best_uplink":     "<peer_fp>",
  "best_hops":       2,
  "best_seq_no":     55,
  "issued_at":       "2026-05-23T12:00:01Z"
}
```

| Field | Type | Description |
|---|---|---|
| `type` | string | Must equal `"route_query_response_v1"`. |
| `query_id` | uint64 | Echo of the request's `query_id`. |
| `target_identity` | string | Echo of the queried target. |
| `found` | bool | True when the responder has at least one non-Dead route to `target_identity`. |
| `best_uplink` | string | Informational. The responder's best uplink for the target. Receivers do NOT use this for their own NextHop — that is always the sender. |
| `best_hops` | uint8 | The responder's hop count to the target. Receivers add +1 on ingest (receive-side hop convention). |
| `best_seq_no` | uint64 | The wire SeqNo the responder has for the chosen claim. Standard SeqNo monotonicity in `UpdateRoute` decides whether to accept. |
| `issued_at` | string | Optional RFC 3339 timestamp. |

When `found=false`, the `best_*` fields are zero-valued / omitted on
the wire (the schema uses `omitempty`).

#### Query protocol

- Queries are sent on demand, not on a periodic ticker. The
  intended caller is a relay path whose `Lookup` returned only
  `Bad`/`Dead` claims or an empty result.
- Each `SendRouteQuery(target)` call fans out to at most three
  directly connected peers that advertise the FULL triplet
  `mesh_route_query_v1 + mesh_relay_v1 + mesh_routing_v1`. The
  triplet is required because the ingested response always lands
  as a transit next-hop (`Hops = BestHops + 1`): `mesh_relay_v1`
  is needed for the peer to accept `relay_message` frames
  addressed to us through that next-hop, and `mesh_routing_v1`
  is needed for the peer to act as transit at all. A peer
  advertising only `mesh_route_query_v1` is filtered out by
  `peersWithRouteQueryCap` (see
  `internal/core/node/routing_query_sender.go`). Outbound
  sessions are preferred; inbound connections are used as
  fall-back.
- Per-target rate limit: at most three emissions per 30 s sliding
  window. The window is per-target — filling the budget for target
  A does not affect target B.
- The protocol is strictly single-hop. Receivers MUST NOT forward
  `route_query_v1` to other peers.
- Responses are ingested into the receiver's routing table as
  `RouteSourceAnnouncement` claims — the same trust class as a
  regular `announce_routes` frame. Standard admission rules
  (per-Origin high-water, SeqNo monotonicity, cap eviction,
  fast invalidation) apply unchanged. There is no privileged
  query-response ingestion path.
- Defensive ingest guards: `found=true` with `best_hops=0` is
  treated as invalid (a hops=0 route would be a direct route, not
  a transit-query answer) and dropped. A response whose
  `target_identity` equals the sender identity is dropped (pathological
  "I reach myself through someone else" claim).

### Capability negotiation and mixed-version mesh

Both wire frame pairs are gated by independent capabilities:

- `mesh_route_probe_v1` — gates the probe/ack pair.
- `mesh_route_query_v1` — gates the query/response pair.

A node may advertise either, both, or neither. The reference
implementation advertises both by default (see
`internal/core/node/capabilities.go`). Peers that did not negotiate
the capability never receive frames gated by it — `sendFrameToIdentity`
enforces this filter on the send side, and the dispatcher rejects
inbound frames whose connection lacks the capability.

Mixed-version interop. Phase 2 capabilities gate WIRE traffic
(probe sends, query fan-out) but NOT local health bookkeeping.
Every accepted LEARNED `(Identity, Uplink)` claim — including
claims announced by a `mesh_routing_v1`-only peer — is seeded as
`HealthQuestionable` in `RouteHealthState` and aged through the
passive idle timeline (Good → Questionable → Bad → Dead, see
`Table.UpdateRoute` and `TickHealth` in
`internal/core/routing/`). Direct (own-origin, `Identity ==
Uplink`) claims are the exception: `AddDirectPeer` seeds them as
`HealthGood` because the handshake itself is proof of session
liveness, and `TickHealth` skips Direct pairs entirely because
their lifecycle is session-bound (`AddDirectPeer` /
`RemoveDirectPeer`) rather than traffic-driven. A quiet direct
peer therefore stays `Good` forever until the session ends, even
without organic hop_ack traffic — operators reading
`fetchRouteHealth` should not expect a healthy direct peer to
decay; the absence of decay is the contract. What changes for a
peer lacking the Phase 2 caps is strictly the active wire paths:
without `mesh_route_probe_v1` we never send probes to it, so its
pairs stay `Questionable` (or escalate via passive timeline)
until organic relay traffic generates a `hop_ack`; without
`mesh_route_query_v1` it is not eligible as a fan-out target.
`Lookup` still ranks those pairs through `CompositeScore`, so a
never-probed `mesh_routing_v1`-only uplink carries the standard
`Questionable` penalty (`scoreHealthQPenalty`) and ranks below
every Good alternative under strict-tier ordering, but remains
selectable — `Lookup` filters Dead strictly and keeps
Questionable/Bad in the result.

## Русский

Документ описывает wire-форматы и state machine Phase 2 plana
cluster-mesh: per-`(Identity, Uplink)` health, RTT EWMA, composite
scoring, `route_probe_v1` / `route_probe_ack_v1` и `route_query_v1`
/ `route_query_response_v1`. Phase 2 заменяет первоначальный набросок
из roadmap iter-1.5, который использовал устаревшую per-triple
модель хранения; Phase 2 работает на per-pair модели после
Phase 1 storage refactor'а (см. `docs/routing.md` «Модель хранения»).

### Объём

Phase 2 вводит:

- State machine `RouteHealthState` per `(Identity, Uplink)` пара
  (`Good → Questionable → Bad → Dead`).
- RTT EWMA per `(Identity, Uplink)`, α=0.3. Основной источник
  samples — активный probe round-trip (sender-measured на
  `route_probe_v1` send → `route_probe_ack_v1` receive в
  `probeRegistry.Resolve`). Hop-ack API (`Table.MarkHopAck`,
  `Table.ConfirmHopAck`) принимает non-zero `rtt` аргумент для
  будущей plumbing, но production node path сейчас передаёт
  `rtt=0` (`internal/core/node/routing_hop_ack.go::confirmRouteViaHopAck`)
  потому что оригинальный message send timestamp не прокинут
  через relay pipeline; `rtt=0` сохраняет предыдущее значение
  EWMA без изменений, поэтому поле никогда не несёт forged sample.
- `CompositeScore` функцию, заменяющую legacy by-hops sort в
  `routing.Table.Lookup`.
- Две новых пары wire-фреймов:
  - `route_probe_v1` / `route_probe_ack_v1` (gated
    `mesh_route_probe_v1`) — активные reachability check'и.
  - `route_query_v1` / `route_query_response_v1` (gated
    `mesh_route_query_v1`) — on-demand single-hop lookup для
    быстрого recovery когда все uplink'и identity → Bad/Dead.

Phase 2 — **аддитивна на проводе**. Новые capability
(`mesh_route_probe_v1`, `mesh_route_query_v1`) гейтят только
новые пары wire-фреймов — peers без них никогда не получают
probes или queries. Локальное health-bookkeeping запускается на
receiver'е безусловно для LEARNED claims: каждый принятый learned
`(Identity, Uplink)` claim посевается как `Questionable` в
`RouteHealthState` и стареет по passive timeline (Good →
Questionable → Bad → Dead) независимо от capability-набора
объявителя. Direct (own-origin, `Identity == Uplink`) claims —
исключение: `AddDirectPeer` посевает их как `HealthGood`, потому
что handshake сам по себе — доказательство liveness сессии, а
`TickHealth` полностью пропускает Direct pairs, поскольку их
lifecycle session-bound (`AddDirectPeer` / `RemoveDirectPeer`), а
не traffic-driven. Тихий direct peer остаётся `Good` до конца
сессии, даже без organic hop_ack трафика — операторы, читающие
`fetchRouteHealth`, не должны ожидать decay'я для здорового
direct peer'а; отсутствие decay'я и есть контракт. Полный
mixed-version контракт — в секции "Capability gating".

### State machine

`RouteHealthState` живёт в `routing.Table` под `t.mu` и ключ — пара
`(Identity, Uplink)`. Переходы:

- **Получение hop-ack** (passive): `relay_hop_ack` для сообщения
  через `(Identity, Uplink)` — самое сильное passive
  подтверждение. Всегда восстанавливает `Good`, сбрасывает
  `ProbeFailures`, refresh'ит `LastHopAck`.
- **Получение probe-ack** (active): `route_probe_ack_v1` с
  `reachable=true` действует как hop-ack. `reachable=false`
  оставляет uplink «живым» (он ответил), но инкрементирует
  `ProbeFailures` — пересечение порога (3 подряд failures) форсит
  переход в `Bad` независимо от passive timeline.
- **Timeout probe** (active): не пришёл `route_probe_ack_v1` за
  `HealthProbeTimeout` (5s). Инкрементирует `ProbeFailures`.
- **Периодический idle tick**: каждые `HealthProbeInterval`
  (15s) refresher проходит по парам и применяет passive timeline:
  - `Good → Questionable` когда idle > 60s.
  - `Questionable → Bad` когда idle > 122s.
  - `Bad → Dead` когда idle > 182s.

Active и passive timelines намеренно асимметричны. 3 probe failures
выводят на `Bad` за ~45s; passive idle path — за 122s. Active
проверки должны детектировать failure быстрее, чем passive
наблюдение — gaps в passive трафике могут быть legitimate.

`Dead` pair исключается из `Lookup` И из announce-проекции
(`AnnounceableFor` / `AnnounceProjectionFor`) — соседи перестают
рефрешить битый путь через нас. Исключение: `RouteSourceDirect`.
Direct (own-origin) claims session-bound: их lifecycle driver'ится
через `AddDirectPeer` / `RemoveDirectPeer`, и `Dead`-метка на
Direct означает «давно не было organic hop_ack», а не «session
ушла». Исключение Direct из Lookup заставило бы relay path
fall-back'нуть в gossip, пока announce продолжает рекламировать
тот же direct route; silent drop из announce лишил бы соседей
authoritative withdrawal. Direct Dead остаётся в обеих сторонах;
ranking применяет `Bad`-equivalent penalty
(`scoreHealthBadPenalty`) вместо `Dead`-as-sentinel −1, так что
подтверждённая альтернатива выигрывает, но Direct остаётся
selectable как last resort.

`Bad` и `Questionable` остаются selectable как last resort, но
строго ниже любой альтернативы из более высокой tier'ы по
composite score. Strict-tier ordering — release-blocking
инвариант: TableRouter триггерит `route_query_v1` recovery на
каждой Bad-route отправке, опираясь на «selected Bad ⇒ нет
Good/Questionable альтернативы». Значения штрафов
(`scoreHealthQPenalty` = 250, `scoreHealthBadPenalty` = 500)
подобраны так, чтобы доминировать суммарный within-tier spread
(hops / RTT / source).

#### Таблица переходов

| Из | В | Триггер |
|---|---|---|
| любое | `Good` | пришёл `relay_hop_ack` ИЛИ `route_probe_ack_v1(reachable=true)` |
| `Good` | `Questionable` | hop-ack idle > 60s |
| `Questionable` | `Bad` | hop-ack idle > 122s ИЛИ `ProbeFailures >= 3` |
| `Bad` | `Dead` | hop-ack idle > 182s |
| `Bad` | `Questionable` | пришёл `route_probe_ack_v1(reachable=false)` (peer жив, но нет маршрута) |
| `Dead` | — | Dead **липкий** под негативными свидетельствами. `route_probe_ack_v1(reachable=false)`, таймауты проб и idle-тики НЕ возвращают Dead в Questionable — резюрекция возможна только реальным подтверждением (`relay_hop_ack` или `route_probe_ack_v1(reachable=true)`). Обоснование: `Lookup` исключает Dead из selection, и ответ «не могу достучаться до target через этот uplink» не должен молча включать обратно маршрут, который ранее был исключён — иначе мигающий peer мог бы циклично возвращать выкинутый маршрут в набор кандидатов. |

### RTT EWMA

`UpdateRTT(prev, sample)` сливает свежий round-trip в среднее:

```
new = prev × (1−α) + sample × α
где α = 0.3
```

Cold-start: при `prev = 0` функция возвращает `sample` как есть.
Outlier-риск acceptable, поскольку EWMA сходится к стабильному
baseline за ~5 samples в пределах 5%.

`sample ≤ 0` трактуется как «нет полезного измерения», `prev`
сохраняется. Senders вычисляют `sample` как `now() − probeSentAt`
(или `now() − relaySentAt` для hop-ack ingest в relay-пути).
**Senders НЕ должны доверять полю `rtt_ms` входящего
`route_probe_ack_v1` для собственных ranking-решений** — это поле
несёт local-оценку responder'а как informational, но zero-trust
budget требует sender-measured RTT для любого quality-решения.

### Composite scoring

`CompositeScore(hops, source, health)` производит float ranking-
score, выше — лучше:

```
score = 100 − hops × 10
      + rttBonus(health.RTT)
      + healthPenalty(health.Health)
      + sourceBonus(source)
```

- `rttBonus`: +30 при RTT ≤ 20ms, linear taper до 0 на 100ms, 0
  выше 100ms.
- `healthPenalty`: 0 для `Good`, −250 для `Questionable`, −500 для
  `Bad`, sentinel `−1` (исключено из selection) для `Dead`.
  Штрафы Questionable / Bad подобраны так, чтобы строго
  доминировать within-tier spread (≈190 unit'ов между hops ∈ [1,15],
  RTT, source). Получается strict-tier ordering: каждый Good
  обгоняет любой Questionable, каждый Questionable обгоняет любой
  Bad. Relay path в TableRouter опирается на этот инвариант —
  «selected Bad route» считается триггером recovery
  `route_query_v1`.
- `sourceBonus`: +20 для `Direct` или `Local`, +10 для `HopAck`, 0
  для `Announcement`.

`Lookup` фильтрует transit (`Source != RouteSourceDirect`) claims
с `Dead` health и ранжирует остальные по убыванию score. Direct
(own-origin) claims exempt от Dead-фильтра — lifecycle Direct'ов
session-bound, и Dead-метка на Direct означает «давно не было
organic hop_ack», а не «session ушла». Если Direct claim Dead,
ranking подставляет `Bad` penalty вместо excluded-sentinel (−1),
так что claim остаётся selectable как last resort, но проигрывает
любой подтверждённой альтернативе. Тот же Dead-filter
с Direct exemption применяется на announce-проекции
(`AnnounceableFor` / `AnnounceProjectionFor`), чтобы соседи и
локальный relay path соглашались о том, какие маршруты selectable.
Synthetic self-route (`Source=Local, Hops=0`) естественно идёт
первой — компоненты её score доминируют любую storage-claim.

Nil-health safe: когда health-state ещё не записан для пары,
формула пропускает RTT и health terms и возвращает
`base + sourceBonus`. Это воспроизводит pre-Phase-2
`isBetter`-style ordering modulo trust-tier ties.

### Wire format `route_probe_v1` / `route_probe_ack_v1`

#### Request (`route_probe_v1`)

```json
{
  "type":            "route_probe_v1",
  "probe_id":        12345678,
  "target_identity": "<X_fp>"
}
```

| Поле | Тип | Описание |
|---|---|---|
| `type` | string | Должно равняться `"route_probe_v1"`. |
| `probe_id` | uint64 | Sender-chosen opaque correlator. Эхом возвращается в ack. Non-zero по конвенции — JSON-default 0 однозначно отличим от minted ID. |
| `target_identity` | string | Ed25519 fingerprint destination'а, чью reachability probe проверяет. Required. |

#### Response (`route_probe_ack_v1`)

```json
{
  "type":       "route_probe_ack_v1",
  "probe_id":   12345678,
  "reachable":  true,
  "rtt_ms":     45
}
```

| Поле | Тип | Описание |
|---|---|---|
| `type` | string | Должно равняться `"route_probe_ack_v1"`. |
| `probe_id` | uint64 | Echo `probe_id` из request'а. |
| `reachable` | bool | True когда local `routing.Table.Lookup(target_identity)` responder'а вернул хотя бы один non-Dead маршрут. |
| `rtt_ms` | uint32 | Local RTT-оценка responder'а (best-effort, informational). Senders НЕ должны trust'ить этот field для своего ranking'а — measure локально как `now() − probeSentAt`. |

#### Протокол probe

- Probe'ы планируются sender'ом каждые `HealthProbeInterval`
  (15s) для каждой пары в `Questionable`. Outstanding-probe
  registry — in-process бухгалтерия, не больше одного outstanding
  probe на пару.
- Verify-before-trust для впервые наблюдаемых uplink'ов реализован
  через посев свежей `(Identity, Uplink)` пары как `Questionable`
  (см. `Table.UpdateRoute` в
  `internal/core/routing/table_mutation.go`). Probe-sender ticker
  подхватит её на следующем 15s tick'е и тогда отправит первую
  пробу — никакого immediate one-shot probe в момент admission
  нет. Пара ранжируется со штрафом `Questionable`
  (`scoreHealthQPenalty` в `CompositeScore`) до прихода первого
  probe ack — strict-tier ordering гарантирует, что непроверенный
  uplink не обгонит ни одну Good альтернативу независимо от hops /
  RTT / source.
- Send уважает Phase 0 overload gate. При overload пропускается
  только АКТИВНАЯ фаза отправки проб — passive `TickHealth()`
  sweep, который стареет каждую отслеживаемую пару через
  `applyIdleTick`, по-прежнему запускается в начале probe tick'а
  (см. `internal/core/node/routing_probe_loop.go::probeTick`), так
  что state machine продолжает продвигаться по timeline
  Good→Questionable→Bad→Dead. Пары, которые были бы пробированы
  во время overload'а, сходятся к `Bad` через более медленный
  passive 122s timeline вместо активного 45s probe-пути.
- Probe строго single-hop. Receivers НЕ должны forward'ить
  `route_probe_v1` другим peer'ам.
- Ack, который sender не может resolve'ить — либо `probe_id` не в
  outstanding registry (обычно stale ack после fire timeout watcher'а),
  либо пришёл от peer'а отличного от target uplink'а оригинальной
  пробы (potential spoof) — drop'ается без мутации state и
  логируется на debug level. Оба случая сливаются в одну log line
  by design: `probeRegistry.ResolveMatching` возвращает `ok=false`
  для обоих и намеренно скрывает который произошёл, потому что
  различение либо требует утечки внутренностей registry в handler,
  либо даёт side channel attacker'у пробующему monotonic
  `probe_id` space. Pending registry entry для реальной пробы
  остаётся intact в случае uplink mismatch, чтобы легитимный
  ack (или его timeout) всё ещё сработал unchanged. Это
  receive-side zero-trust guard против ack spoofing'а.

### Wire format `route_query_v1` / `route_query_response_v1`

#### Request (`route_query_v1`)

```json
{
  "type":            "route_query_v1",
  "query_id":        87654321,
  "target_identity": "<X_fp>",
  "max_hops":        8,
  "issued_at":       "2026-05-23T12:00:00Z"
}
```

| Поле | Тип | Описание |
|---|---|---|
| `type` | string | Должно равняться `"route_query_v1"`. |
| `query_id` | uint64 | Sender-chosen correlator. Echo в response. Non-zero по конвенции. |
| `target_identity` | string | Ed25519 fingerprint destination'а. Required. |
| `max_hops` | uint8 | Optional. Advisory cap на transit depth; responder делает single-hop Lookup в любом случае. |
| `issued_at` | string | Optional RFC 3339 timestamp для freshness'а. |

#### Response (`route_query_response_v1`)

```json
{
  "type":            "route_query_response_v1",
  "query_id":        87654321,
  "target_identity": "<X_fp>",
  "found":           true,
  "best_uplink":     "<peer_fp>",
  "best_hops":       2,
  "best_seq_no":     55,
  "issued_at":       "2026-05-23T12:00:01Z"
}
```

| Поле | Тип | Описание |
|---|---|---|
| `type` | string | Должно равняться `"route_query_response_v1"`. |
| `query_id` | uint64 | Echo `query_id`. |
| `target_identity` | string | Echo queried target'а. |
| `found` | bool | True когда у responder'а есть хотя бы один non-Dead маршрут к `target_identity`. |
| `best_uplink` | string | Informational. Лучший uplink responder'а. Receivers НЕ используют его для своего NextHop — там всегда sender. |
| `best_hops` | uint8 | Hop count responder'а до target. Receivers добавляют +1 на ingest. |
| `best_seq_no` | uint64 | Wire SeqNo responder'а для chosen claim. Стандартная SeqNo monotonicity в `UpdateRoute` решает accept'ить ли. |
| `issued_at` | string | Optional RFC 3339 timestamp. |

При `found=false`, `best_*` поля zero-valued / omitted на проводе
(schema использует `omitempty`).

#### Протокол query

- Query'и отправляются on-demand, не по periodic ticker. Intended
  caller — relay path чей `Lookup` вернул только `Bad`/`Dead`
  claims или пустой результат.
- Каждый `SendRouteQuery(target)` fan-out'ит к ≤3 directly
  connected peer'ам, рекламирующим ПОЛНЫЙ triplet
  `mesh_route_query_v1 + mesh_relay_v1 + mesh_routing_v1`. Triplet
  обязателен потому что принятый response всегда landed как
  transit next-hop (`Hops = BestHops + 1`): `mesh_relay_v1` нужен
  чтобы peer принимал `relay_message` фреймы, адресованные нам
  через этот next-hop, а `mesh_routing_v1` — чтобы он мог быть
  transit'ом вообще. Peer только с `mesh_route_query_v1`
  отфильтровывается через `peersWithRouteQueryCap` (см.
  `internal/core/node/routing_query_sender.go`). Outbound sessions
  preferred; inbound — fall-back.
- Per-target rate limit: ≤3 emissions per 30s sliding window.
  Window per-target — заполнение budget для target A не влияет на B.
- Протокол строго single-hop. Receivers НЕ должны forward'ить
  `route_query_v1` другим peer'ам.
- Responses ingest'ятся в receiver'а как `RouteSourceAnnouncement` —
  тот же trust class что обычный `announce_routes`. Стандартные
  admission rules (per-Origin high-water, SeqNo monotonicity, cap
  eviction, fast invalidation) применяются unchanged. Нет
  privileged query-response ingestion path'а.
- Defensive ingest guards: `found=true` с `best_hops=0` invalid
  (hops=0 — direct route, не transit-query answer) → drop.
  Response где `target_identity == senderIdentity` (pathological
  «я достигаю себя через кого-то ещё») → drop.

### Capability negotiation и mixed-version mesh

Обе пары wire-фреймов gated независимыми capability'ями:

- `mesh_route_probe_v1` — gates probe/ack пару.
- `mesh_route_query_v1` — gates query/response пару.

Нода может advertise'ить любую из них, обе или ни одной. Reference
implementation advertise'ит обе by default
(`internal/core/node/capabilities.go`). Peers без negotiated
capability никогда не получают frames gated ею —
`sendFrameToIdentity` enforces filter на send side, dispatcher
rejects inbound фреймы для connection без capability.

Mixed-version interop. Phase 2 capabilities гейтят WIRE-трафик
(probe sends, query fan-out), а НЕ локальное health-bookkeeping.
Каждый принятый claim `(Identity, Uplink)` — включая объявленные
peer'ом только с `mesh_routing_v1` — посевается как
`HealthQuestionable` в `RouteHealthState` и стареет по passive
idle timeline (`Table.UpdateRoute` в
`internal/core/routing/table_mutation.go`). Для peer'а без
Phase 2 cap'ов меняются только активные wire-пути: без
`mesh_route_probe_v1` мы никогда не шлём ему пробы, его пары
остаются `Questionable` (или эскалируются по passive timeline)
до прихода органического relay-трафика с `hop_ack`; без
`mesh_route_query_v1` он не подходит как fan-out target.
`Lookup` всё ещё ранжирует такие пары через `CompositeScore`, и
непроверенный uplink с `mesh_routing_v1` несёт стандартный
`Questionable` penalty (`scoreHealthQPenalty`) и под
strict-tier ordering ранжируется ниже любой Good альтернативы,
но остаётся selectable — `Lookup` строго исключает только Dead,
оставляя Questionable/Bad в результате.
