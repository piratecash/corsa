# Incremental Route Table Sync (Phase 3 PR 12.5)

## English

This document specifies the `route_sync_digest_v1` / `route_sync_summary_v1`
wire-frame pair introduced in Phase 3 PR 12.5 of the cluster-mesh plan
(`docs/cluster-mesh/phase-3-multipath-reputation.md` §4.5). The pair lets
two peers reconnecting within a short window agree, via a deterministic
digest exchange, that nothing has changed in their mutual routing view —
and skip the next forced full sync on a match.

### Scope

The digest exchange is an optimisation on top of the existing announce
plane. It does NOT replace `announce_routes` / `routes_update` / future
`route_announce_v3` frames; it does NOT carry routing decisions or trust
between peers. On a digest match the node suppresses the forced full
sync to the matching peer for a bounded window. The reconnect forced
full sync is the PRIMARY path this optimisation targets: a pair that
reconnects and agrees on its mutual view skips re-sending the whole
table. What keeps running unchanged is the delta cycle (real changes
always propagate), the first-ever initial sync (a peer with no prior
baseline, `LastSentSnapshot == nil`), and explicit `request_resync` /
consistency-loss resyncs (classified as a HARD resync — never
suppressible).

### Trust invariant

The digest is a LOCAL HINT, not authoritative routing state. A
malicious peer claiming `match=true` against a mismatched digest only
defers ONE forced-full-sync window; the next delta cycle restores
correctness and the announce plane remains the source of truth for
routing state. No trust is transferred through this exchange.

### Capability gate

Both frames are gated by `mesh_route_sync_v1`. Peers that did not
negotiate the capability never receive either frame, so a mixed-version
mesh degrades cleanly — non-capable peers continue receiving the full
announce stream as before. The capability is ORTHOGONAL to the
announce-plane capabilities (`mesh_routing_v1` / `mesh_routing_v2` /
`mesh_routing_v3` — all three shipped): the digest suppression applies
only between pairs that both advertised `mesh_route_sync_v1`,
regardless of which announce frame format they negotiated.

### Session lifecycle

The wire exchange is driven by the session lifecycle hooks in
`internal/core/node/routing_session.go`:

- **On session close** (`onPeerSessionClosed`, last-session transition,
  peer ever advertised `mesh_route_sync_v1`): the node computes
  `Table.AnnounceDigestFor(peer)` — the OUTBOUND announce projection to
  the peer (what we would announce to it, keyed by the per-peer wire
  SeqNos the peer stored) — and stashes the result via
  `Table.RecordPeerDigestSnapshot`. This is deliberately NOT
  `SyncDigestFor(peer)` (routes we learned THROUGH the peer): the
  receiver compares against its own via-(this node) view, and only the
  outbound projection lines up with that view, so only it can produce a
  match in a normal topology (A learns C via B ⇒ B must offer the digest
  of what B announces to A, which includes C). The cache entry survives
  `SessionDigestCacheTTL = 5 min` (Phase 3 §4.9 decision #5).
- **On session open** (`onPeerSessionEstablished`, first relay session,
  peer advertises `mesh_route_sync_v1`): the node calls
  `Table.ConsumePeerDigestSnapshot(peer, now)`. On a cache hit it arms a
  short PENDING suppression window via
  `AnnounceLoop.MarkPeerDigestPending` and then emits a
  `route_sync_digest_v1` frame through `sendFrameToIdentity`
  (capability-gated). Arming BEFORE the reconnect `TriggerUpdate` is
  what makes the optimisation work: the reconnect's own announce cycle
  sees the pending window and holds off the forced full sync until the
  summary can land, instead of racing ahead of the round trip. The
  pending window is `DigestRoundTripGrace` (clamped to the node's
  forced-full cadence) so a silent or slow peer defers the
  freshness-restoring full sync only briefly. The consume is
  single-shot — a future reconnect within the same TTL window does NOT
  re-emit a stale digest. When there is no cached digest the node arms
  nothing and the reconnect full sync proceeds normally.
- **TTL eviction** runs lazily inside `Table.TickTTL` via
  `pruneExpiredDigestSnapshotsLocked`, so long-disconnected peers do
  not bloat the cache.

The cache is NOT persisted across restart. Phase 3 §4.9 decision #6
keeps reputation and digest state in memory only; a fresh process
emits no digest on the first reconnect and the receiver simply runs
the normal forced-full-sync.

### Digest formula

The digest is the lowercase hex-encoded SHA-256 over the canonical
representation of a set of `(Identity, SeqNo)` pairs:

```
canonical(entries) =
    for each entry, sorted by Identity ascending:
        Identity bytes
        ':' (0x3a)
        SeqNo as decimal digits
        '\n' (0x0a)

digest = hex(sha256(canonical(entries)))
```

The exchange compares TWO digests computed over the SAME logical set
from opposite ends:

- **Sender (`Table.AnnounceDigestFor(peer)`)** — the outbound announce
  projection to the peer. For each Identity the sender would announce to
  the peer (a live, non-Dead, non-cooled winner via some uplink other
  than the peer exists), it emits `(Identity, wireSeqNo)` where
  `wireSeqNo = outboundPeerMax[(Identity, peer)]` — the per-peer
  synthesised SeqNo it last put on the wire to that peer. Reading the
  recorded wire SeqNo (rather than the local claim's native SeqNo, which
  differs at every hop) is what makes the value reproduce exactly what
  the peer stored.
- **Receiver (`Table.SyncDigestFor(peer)`)** — walks the per-identity
  buckets and selects, for each Identity, the SeqNo of the single live
  (`!IsWithdrawn() && !IsExpired(now)`) claim whose `Uplink == peer`.

When the two nodes are in sync these sets are identical: every Identity
the sender announced became a via-sender claim on the receiver, stored
at the same wire SeqNo. Both sides apply split horizon (exclude
`Identity == peer`). The pre-fix code used `SyncDigestFor` on BOTH ends,
comparing the sender's routes-via-peer against the receiver's
routes-via-sender — two disjoint sets that almost never matched.

SHA-256 is chosen for determinism and project-wide hash uniformity
(used elsewhere for message IDs). It carries no secret material;
faster non-cryptographic hashes (xxhash, blake3) were not adopted
because the per-call cost is sub-millisecond on the densely-connected
100-node mesh (~100 entries × ~50 bytes) and the simplicity of a
single hash family pays off in tests and operator tooling.

Empty input produces the stable SHA-256 of the empty string
(`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`),
so a node with no learned routes through the peer still emits a
well-defined digest.

### `route_sync_digest_v1` wire format

```json
{
  "type":                   "route_sync_digest_v1",
  "digest":                 "<hex-sha256>",
  "known_identities_count": <uint32>,
  "generated_at":           "<RFC3339 UTC>"
}
```

- `digest` — the hex SHA-256 from `ComputeSyncDigest`. Required and
  non-empty; `UnmarshalRouteSyncDigestFrame` rejects empty.
- `known_identities_count` — number of identities folded into the
  digest. Diagnostic hint; receivers compare against their own count
  to log "different content vs different shape" but do NOT use it as
  the match signal.
- `generated_at` — sender-local timestamp at digest computation. Used
  only as a freshness hint in structured logs; never consulted for
  the match decision.

### `route_sync_summary_v1` wire format

```json
{
  "type":             "route_sync_summary_v1",
  "digest":           "<hex-sha256>",
  "match":            <bool>,
  "expect_full_sync": <bool>
}
```

- `digest` — echo of the digest the sender provided. Lets the sender
  correlate the summary with its outstanding request. Empty is
  tolerated to leave room for future protocol revisions (the receive
  handler treats empty as "no correlation, drop the suppression
  update").
- `match` — `true` when the responder's locally computed digest over
  its current per-peer view equals the digest in the request.
- `expect_full_sync` — mirrors `!match` for now; reserved so a future
  revision can distinguish "mismatch — please resync" from "match
  but I would still like a full sync" without adding a third wire
  field.

### Receive-side semantics

`handleRouteSyncDigest` (in `internal/core/node/routing_sync.go`):

1. Capability gate (`CapMeshRouteSyncV1`) already enforced by the
   dispatch switch; the handler defensively no-ops on an empty
   `senderIdentity` (auth-race).
2. Compute `Table.SyncDigestFor(senderIdentity)`.
3. Build and emit `route_sync_summary_v1` with `match = local ==
   wire.digest` through `s.sendFrameViaNetwork`. Marshal failures are
   logged at warn and the reply is dropped; the sender times out
   waiting for the summary and proceeds with the normal full sync.

`handleRouteSyncSummary`:

1. Capability gate as above; defensive no-op on empty
   `senderIdentity` or unwired `announceLoop` (test fixtures).
2. Empty `digest` echo — dropped with no state change in either
   direction. `UnmarshalRouteSyncSummaryFrame` tolerates an empty echo
   for forward-compat, and documents that the receive handler must
   treat it as "no correlation, drop the suppression update": an empty
   echo cannot be tied to a digest we actually emitted.
3. `match=false` — the digests diverged, so the receiver needs a
   fresh full sync. The handler calls
   `AnnounceLoop.ClearPeerDigestSuppression(senderIdentity)` to drop
   any pending window armed on reconnect, so the next cycle full-syncs
   promptly instead of waiting out the grace; logged at debug.
4. `match=true` — call
   `AnnounceLoop.ConfirmPeerDigestMatch(senderIdentity, echo, now)`,
   which extends the suppression window ONLY when `echo` equals the
   digest THIS node emitted to the peer on reconnect (the correlation
   anchor stored by `MarkPeerDigestPending`). A stale, unsolicited, or
   spoofed summary echoing some other digest is ignored, so it cannot
   suppress a forced full sync it has no relation to. On a correlated
   match the window length is OWNED by the announce loop — derived as
   `EffectiveForcedFullSyncInterval(announceInterval) =
   min(2 × AnnounceInterval, DefaultTTL/2)` anchored at `now`, never a
   caller-supplied deadline — so it spans exactly one forced-full
   cadence and a single summary suppresses at most one forced-full.

### Suppression gate inside `announceToAllPeers`

The Phase 0 announce loop's per-peer goroutine checks the
suppression deadline AFTER the periodic forced-full-sync trigger
decision and BEFORE the wire send. The gate fires ONLY when ALL
of these conditions hold:

- `needsFull == true` (the deadline branch, or a session-boundary
  resync, decided a forced full sync is due).
- `view.LastSentSnapshot != nil` (the peer already has a baseline —
  the first-ever initial sync is exempt).
- `view.ResyncIsHard == false` — the resync is SOFT (a reconnect /
  session boundary), so the digest hint may suppress it. A HARD
  resync (explicit `request_resync` or a consistency-loss
  `MarkInvalid`) sets `ResyncIsHard == true` and is never suppressed;
  the peer is demanding a fresh table.
- `AnnounceLoop.isDigestSuppressionActive(peer, now) == true` (a
  pending or matched suppression window is still in effect).

If all hold, `needsFull` is reset to `false` and the cycle emits the
regular delta (or skips per the standard `isDeltaCycle && overloaded`
gate). Because the reconnect resync is soft, this is exactly the gate
that elides the reconnect forced full sync — the optimisation's whole
purpose. The expiry is checked on every poll, and stale entries are
evicted on the way out so the map does not grow as peers come and go.

### Wire-vs-RPC separation

Both frames are P2P wire commands; they live in
`dispatchNetworkFrame` and travel through the per-connection writer
goroutine via `sendFrameViaNetwork` / `sendFrameToIdentity`. Both are
ONE-WAY: a digest emit has no synchronous session reply (the summary
returns later as its own unsolicited inbound frame, routed to
`handleRouteSyncSummary` by the dispatcher), so both are classified
fire-and-forget (`isFireAndForgetFrame`) and the outbound session loop
must NOT route them through `peerSessionRequest` — doing so would
consume the next inbound frame (often the summary) as a bogus "reply"
and drop it. The session loop also dispatches an inbound route-sync
frame that lands mid-`peerSessionRequest` rather than returning it as a
reply. They do NOT have any presence on the `CommandTable` RPC surface.
There is no
RPC trigger to force a digest emit — the protocol is purely event-
driven from the session lifecycle hooks. Operator-facing
observability of digest activity, if needed in the future, would
live in a separate read-only RPC command (the same pattern Phase 3
PR 12.7 used for `fetchRouteReputation`), never aliased under the
`route_sync_digest_v1` / `route_sync_summary_v1` wire names.

---

## Русский

Документ специфицирует пару wire-фреймов `route_sync_digest_v1` /
`route_sync_summary_v1`, введённую в Phase 3 PR 12.5 cluster-mesh
плана (`docs/cluster-mesh/phase-3-multipath-reputation.md` §4.5).
Пара позволяет двум peer'ам, реконнектящимся в течение короткого
окна, договориться через детерминированный обмен digest'ами, что в
их взаимном routing-представлении ничего не изменилось — и
пропустить следующий forced full sync на match'е.

### Объём

Digest-обмен — это оптимизация поверх существующего announce-plane.
Он НЕ заменяет `announce_routes` / `routes_update` / будущий
`route_announce_v3`; он НЕ переносит routing decisions или trust
между peer'ами. На digest match узел подавляет forced full sync к
матчившему peer'у на bounded window. Forced full sync на реконнекте —
это ОСНОВНОЙ путь, ради которого эта оптимизация и существует: пара,
которая реконнектится и согласна по взаимному view, пропускает
повторную отправку всей таблицы. Без изменений продолжают работать:
delta-цикл (реальные изменения всегда пропагируются), самый первый
initial sync (peer без предыдущего baseline, `LastSentSnapshot == nil`)
и explicit `request_resync` / consistency-loss ресинки
(классифицируются как HARD resync — никогда не подавляются).

### Trust-инвариант

Digest — это ЛОКАЛЬНЫЙ HINT, не authoritative routing state.
Malicious peer, claim'ящий `match=true` против mismatched digest'а,
только отложит ОДНО окно forced-full-sync; следующий delta-cycle
восстановит корректность, и announce plane остаётся source of truth
для routing state. Никакого trust через этот обмен не переносится.

### Capability gate

Оба фрейма gated `mesh_route_sync_v1`. Peers, не negotiate'ившие
capability, никогда не получают ни одного из фреймов, так что
mixed-version mesh деградирует чисто — non-capable peers
продолжают получать full announce stream как раньше. Capability
ORTHOGONAL к announce-plane capabilities (`mesh_routing_v1` /
`mesh_routing_v2` / `mesh_routing_v3` — все три отгружены):
digest suppression применяется только между парами, которые обе
advertised `mesh_route_sync_v1`, независимо от того, какой
announce-формат они negotiate'или.

### Lifecycle сессии

Wire-обмен driver'ится session lifecycle hooks в
`internal/core/node/routing_session.go`:

- **На закрытие сессии** (`onPeerSessionClosed`, last-session
  transition, peer когда-либо advertised `mesh_route_sync_v1`):
  узел считает `Table.AnnounceDigestFor(peer)` — OUTBOUND announce
  projection к peer'у (что мы анонсировали бы ему, keyed по per-peer
  wire SeqNo'ам, которые peer сохранил) — и stash'ит результат через
  `Table.RecordPeerDigestSnapshot`. Это намеренно НЕ
  `SyncDigestFor(peer)` (routes, которые мы знаем ЧЕРЕЗ peer):
  receiver сравнивает со своим via-(этот узел) view, и только outbound
  projection совпадает с ним, поэтому только он даёт match в нормальной
  топологии (A знает C через B ⇒ B должен предложить digest того, что B
  анонсирует A, включающий C). Cache-entry переживает
  `SessionDigestCacheTTL = 5 мин` (Phase 3 §4.9 решение #5).
- **На открытие сессии** (`onPeerSessionEstablished`, первая
  relay-сессия, peer advertise'ит `mesh_route_sync_v1`): узел
  зовёт `Table.ConsumePeerDigestSnapshot(peer, now)`. На cache hit
  arm'ит короткое PENDING окно подавления через
  `AnnounceLoop.MarkPeerDigestPending`, и затем emit'ит
  `route_sync_digest_v1` фрейм через `sendFrameToIdentity`
  (capability-gated). Arming ДО реконнект-`TriggerUpdate` — это и есть
  то, что заставляет оптимизацию работать: реконнектный announce-цикл
  видит pending-окно и придерживает forced full sync, пока не придёт
  summary, вместо того чтобы обогнать round-trip. Pending-окно — это
  `DigestRoundTripGrace` (clamp'нутое к forced-full cadence узла), так
  что молчащий или медленный peer откладывает freshness-восстанавливающий
  full sync лишь ненадолго. Consume single-shot — будущий reconnect в
  том же TTL окне НЕ re-emit'ит stale digest. Если кэшированного digest
  нет — узел не arm'ит ничего, и реконнект-full-sync идёт как обычно.
- **TTL eviction** работает лениво внутри `Table.TickTTL` через
  `pruneExpiredDigestSnapshotsLocked`, так что долго-disconnected
  peers не bloat'ят cache.

Cache НЕ персистится через restart. Phase 3 §4.9 решение #6 держит
reputation и digest state только в памяти; свежий процесс не
emit'ит digest на первом реконнекте, и receiver просто делает
обычный forced-full-sync.

### Digest-формула

Digest — это lowercase hex-encoded SHA-256 над канонической
репрезентацией множества пар `(Identity, SeqNo)`:

```
canonical(entries) =
    for each entry, sorted by Identity ascending:
        Identity bytes
        ':' (0x3a)
        SeqNo as decimal digits
        '\n' (0x0a)

digest = hex(sha256(canonical(entries)))
```

Обмен сравнивает ДВА digest'а, посчитанных над ОДНИМ логическим
множеством с противоположных концов:

- **Sender (`Table.AnnounceDigestFor(peer)`)** — outbound announce
  projection к peer'у. Для каждой Identity, которую sender анонсировал
  бы peer'у (есть live, non-Dead, non-cooled winner через uplink,
  отличный от peer'а), эмитит `(Identity, wireSeqNo)`, где
  `wireSeqNo = outboundPeerMax[(Identity, peer)]` — per-peer
  синтезированный SeqNo, который он последним положил на wire этому
  peer'у. Чтение записанного wire SeqNo (а не native SeqNo локального
  claim'а, который отличается на каждом hop'е) — то, что делает
  значение точной репродукцией того, что peer сохранил.
- **Receiver (`Table.SyncDigestFor(peer)`)** — проходит по per-identity
  bucket'ам и для каждой Identity выбирает SeqNo единственного live
  (`!IsWithdrawn() && !IsExpired(now)`) claim'а с `Uplink == peer`.

Когда два узла in sync, эти множества идентичны: каждая Identity,
которую sender анонсировал, стала via-sender claim'ом на receiver'е,
с тем же wire SeqNo. Обе стороны применяют split horizon (исключают
`Identity == peer`). Pre-fix код использовал `SyncDigestFor` на ОБОИХ
концах, сравнивая sender'ские routes-via-peer с receiver'скими
routes-via-sender — два disjoint множества, которые почти никогда не
совпадали.

SHA-256 выбран ради детерминизма и project-wide hash uniformity
(используется elsewhere для message IDs). Не несёт secret
материала; более быстрые non-cryptographic hashes (xxhash, blake3)
не были adopt'нуты, потому что per-call cost sub-millisecond на
densely-connected 100-node mesh'е (~100 entries × ~50 bytes), а
простота единого hash-family окупается в тестах и operator-
tooling.

Пустой input даёт стабильный SHA-256 пустой строки
(`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`),
так что узел без learned routes через peer'а всё равно emit'ит
well-defined digest.

### Wire format `route_sync_digest_v1`

```json
{
  "type":                   "route_sync_digest_v1",
  "digest":                 "<hex-sha256>",
  "known_identities_count": <uint32>,
  "generated_at":           "<RFC3339 UTC>"
}
```

- `digest` — hex SHA-256 от `ComputeSyncDigest`. Required и
  non-empty; `UnmarshalRouteSyncDigestFrame` rejects empty.
- `known_identities_count` — число identities, fold'нутых в
  digest. Diagnostic hint; receivers сравнивают со своим
  собственным count для лога «different content vs different
  shape», но НЕ используют как match signal.
- `generated_at` — sender-local timestamp на момент computation.
  Используется только как freshness hint в structured logs;
  никогда не consult'ится для match decision.

### Wire format `route_sync_summary_v1`

```json
{
  "type":             "route_sync_summary_v1",
  "digest":           "<hex-sha256>",
  "match":            <bool>,
  "expect_full_sync": <bool>
}
```

- `digest` — echo digest'а, который дал sender. Позволяет
  sender'у correlate'ить summary с его outstanding request.
  Пустой tolerated, чтобы оставить место для будущих revisions
  (receive handler трактует пустой как «no correlation, drop the
  suppression update»).
- `match` — `true` когда responder'ский локально-computed digest
  над текущим per-peer view равен digest'у в request'е.
- `expect_full_sync` — mirror'ит `!match` пока что; reserved
  чтобы будущая revision могла различать «mismatch — please
  resync» и «match но мне всё равно нужен full sync» без
  добавления третьего wire-field'а.

### Receive-side семантика

`handleRouteSyncDigest` (в `internal/core/node/routing_sync.go`):

1. Capability gate (`CapMeshRouteSyncV1`) уже enforced'нут
   dispatch switch'ем; handler defensive no-op'ит на пустом
   `senderIdentity` (auth-race).
2. Считает `Table.SyncDigestFor(senderIdentity)`.
3. Строит и emit'ит `route_sync_summary_v1` с `match = local ==
   wire.digest` через `s.sendFrameViaNetwork`. Marshal-failure
   log'ируется на warn и reply drop'ается; sender timeout'нется
   ожидая summary и proceed'нет с normal full sync.

`handleRouteSyncSummary`:

1. Capability gate как выше; defensive no-op на пустом
   `senderIdentity` или unwired `announceLoop` (test fixtures).
2. Пустой `digest` echo — drop без изменения состояния в любую
   сторону. `UnmarshalRouteSyncSummaryFrame` допускает пустой echo
   ради forward-compat и документирует, что receive handler должен
   трактовать его как «no correlation, drop the suppression update»:
   пустой echo нельзя привязать к digest'у, который мы реально
   отправили.
3. `match=false` — digest'ы разошлись, поэтому receiver'у нужен
   свежий full sync. Handler зовёт
   `AnnounceLoop.ClearPeerDigestSuppression(senderIdentity)`, чтобы
   сбросить pending-окно, arm'нутое на реконнекте, и следующий цикл
   full-sync'нулся сразу, а не ждал grace; log на debug.
4. `match=true` — зовёт
   `AnnounceLoop.ConfirmPeerDigestMatch(senderIdentity, echo, now)`,
   который продлевает окно подавления ТОЛЬКО когда `echo` равен
   digest'у, который ЭТОТ узел отправил peer'у на реконнекте
   (correlation anchor, сохранённый `MarkPeerDigestPending`). Stale,
   unsolicited или spoofed summary с другим digest'ом игнорируется,
   так что не может подавить forced full sync, к которому не имеет
   отношения. На correlated match длину окна ВЛАДЕЕТ announce loop —
   `EffectiveForcedFullSyncInterval(announceInterval) =
   min(2 × AnnounceInterval, DefaultTTL/2)`, привязанная к `now`, а не
   caller-supplied deadline — поэтому окно ровно в одну forced-full
   cadence, и один summary подавляет максимум один forced-full.

### Suppression gate внутри `announceToAllPeers`

Per-peer goroutine Phase 0 announce loop'а проверяет suppression
deadline ПОСЛЕ periodic forced-full-sync trigger decision и ДО
wire-send'а. Gate срабатывает ТОЛЬКО когда ВСЕ условия выполнены:

- `needsFull == true` (deadline branch — или session-boundary
  resync — решили, что forced full sync due).
- `view.LastSentSnapshot != nil` (peer уже имеет baseline — самый
  первый initial sync exempt).
- `view.ResyncIsHard == false` — resync SOFT (реконнект /
  session boundary), поэтому digest-hint может его подавить. HARD
  resync (explicit `request_resync` или consistency-loss
  `MarkInvalid`) ставит `ResyncIsHard == true` и никогда не
  подавляется; peer требует свежую таблицу.
- `AnnounceLoop.isDigestSuppressionActive(peer, now) == true`
  (pending или matched окно подавления всё ещё в effect'е).

Если все hold'ятся, `needsFull` reset'ится в `false` и cycle emit'ит
regular delta (или skip'ает per standard `isDeltaCycle && overloaded`
gate). Поскольку реконнект-resync — soft, это ровно тот gate, что
elide'ит реконнектный forced full sync — вся цель оптимизации. Expiry
проверяется на каждом poll'е, и stale entries evict'ятся на выходе,
так что map не растёт по мере того как peers приходят и уходят.

### Wire-vs-RPC separation

Оба фрейма — P2P wire commands; они живут в
`dispatchNetworkFrame` и travel'ятся через per-connection writer
goroutine via `sendFrameViaNetwork` / `sendFrameToIdentity`. Оба
ONE-WAY: digest emit не имеет синхронного session-reply (summary
приходит позже как собственный unsolicited inbound фрейм, routed в
`handleRouteSyncSummary` dispatcher'ом), поэтому оба классифицированы
fire-and-forget (`isFireAndForgetFrame`), и outbound session loop НЕ
должен гонять их через `peerSessionRequest` — иначе следующий inbound
фрейм (часто сам summary) будет consumed как фиктивный «reply» и drop'нут.
Session loop также dispatch'ит inbound route-sync фрейм, прилетевший
mid-`peerSessionRequest`, вместо возврата его как reply. Они
НЕ имеют presence на `CommandTable` RPC surface. Нет RPC-trigger
для force'нуть digest emit — protocol чисто event-driven from
session lifecycle hooks. Operator-facing observability digest
activity, если понадобится в будущем, будет жить в отдельной
read-only RPC command (тот же pattern, что Phase 3 PR 12.7
использовал для `fetchRouteReputation`), никогда не alias'нутый
под `route_sync_digest_v1` / `route_sync_summary_v1` wire-имена.
