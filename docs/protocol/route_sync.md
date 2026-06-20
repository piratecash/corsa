# Incremental Route Table Sync (Phase 3 PR 12.5)

## English

This document specifies the `route_sync_digest_v1` / `route_sync_summary_v1`
wire-frame pair introduced in Phase 3 PR 12.5 of the cluster-mesh plan
(`docs/cluster-mesh/phase-3-multipath-reputation.md` §4.5). The pair lets
two peers reconnecting within a short window agree, via a deterministic
digest exchange, that nothing has changed in their mutual routing view —
and skip the next forced full sync on a match.

**Digest-as-heartbeat (periodic freshness).** The same exchange also runs
as the announce loop's periodic freshness heartbeat: at every refresh
deadline (`EffectiveForcedFullSyncInterval`, ≤ TTL/2) the sender emits a
digest instead of unconditionally rebuilding and resending the full table.
On a match the receiver renews the TTL of the routes learned through the
sender (`Table.RefreshRoutesVia`) — so an unchanged lineage stays alive
without the byte-heavy full sync — and a full rebuild fires only when the
previous heartbeat did not confirm a match (mismatch or silence). This is
what eliminates the periodic full sync from the stable path; the freshness
guarantee is unchanged because a lost/diverged digest escalates to a full at
the next deadline exactly as a dropped full did. See `docs/routing.md`
"Refresh interval invariant" / "Digest-as-heartbeat refresh".

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
  `Table.AnnounceDigestVectorFor(peer)` — the OUTBOUND announce projection
  to the peer (what we would announce to it, keyed by the per-peer wire
  SeqNos the peer stored), returning both the hash and the Phase-0 version
  vector — and stashes BOTH via `Table.RecordPeerDigestSnapshot`. The
  vector is captured here, not recomputed on reconnect, because the
  per-peer outbound SeqNo state (`outboundPeerMax`) is reclaimed when the
  session departs, so a post-teardown recompute would yield an empty
  vector. Carrying it lets the reconnect digest reconcile per identity
  (and so match under the K-cap) exactly as the periodic heartbeat does. This is deliberately NOT
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
  "generated_at":           "<RFC3339 UTC>",
  "entries":                [ {"i": "<identity-hex>", "s": <uint64>}, ... ]
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
- `entries` — OPTIONAL Phase-0 version vector: the `(Identity, SeqNo)`
  pairs the `digest` summarises, as compact `{"i": hex, "s": seqno}`
  objects (one per announced identity). Omitted (`omitempty`) by senders
  that predate it; when present it drives the per-identity reconcile in
  "Phase-0 version vector and safety reconcile" below instead of the
  whole-table hash compare. See `routing.AnnounceDigestVectorFor`.

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

### Phase-0 version vector and safety reconcile

The whole-table `digest` is all-or-nothing: any single divergent identity
flips the hash and there is no cheap way to find which one. Under the
receiver's per-origin K-cap this is fatal — the receiver drops the
sender's cap-rejected routes, so the sender's "what I announced" set and
the receiver's "what I kept" set are systematically different and the
hash can NEVER match in a dense mesh (observed as `summary_match = 0`).
The forced full then fires every cadence and the periodic-silence
optimisation above is dead.

The version vector (`entries`) localises the compare to the agreed
subset. The receiver runs `Table.ReconcileSyncVector(peer, entries, now)`
instead of the hash compare:

- For every live route it holds via the sender whose stored SeqNo equals
  the vector's entry for that identity, it renews the TTL
  (proven-current) — the per-entry equivalent of `RefreshRoutesVia`.
- It reports `match = true` only when it holds NO live via-sender route
  the vector left unconfirmed (absent, or a different SeqNo = stale).
- An identity the vector offers that the receiver does NOT hold via the
  sender (a cap-rejected offer) does NOT affect `match` — it is the
  sender's offer the receiver legitimately did not adopt. This is exactly
  what lets the digest match under the cap.

A stale route on the receiver (vector mismatch) flips `match = false`, so
the sender escalates a full to reconcile it — and the stale route, left
un-refreshed, ages out via `TickTTL` (it is never extended). A
peer that predates the vector sends none and the receiver falls back to
the hash compare unchanged.

**Safety reconcile cadence.** Once the digest can actually match, route
liveness on the stable path is maintained by the heartbeat's per-entry
TTL refresh (at the forced-full cadence, which still satisfies the
refresh-interval invariant) and the byte-heavy full stops firing. A stale
route is caught immediately by a vector mismatch, but a route the receiver
is MISSING because a best-effort delta was dropped would never reconcile
under continuous match. So the sender forces a full regardless of match
every `SafetyFullSyncMultiplier × EffectiveForcedFullSyncInterval`
(≈30 min at the 300 s default), anchored on the last successful full. This
bounds dropped-delta convergence while keeping the stable path otherwise
silent.

**Frame-size cap.** The vector is O(identities), so on a large table it can
exceed the single-frame `MaxFrameLine` (128 KiB ≈ 2000 entries — the
command-plane reader closes anything above it). Two guards keep the frame
legal: the announce loop skips the vector and forces a chunked full above
`maxRouteSyncVectorEntries` (1500), and `buildRouteSyncDigestWire` is an
exact-size backstop that drops the vector to a hash-only frame if a built
line would still exceed the limit (covering the reconnect-cache path, which
has no entry-count gate). So the per-identity reconcile applies up to
~1500 identities; beyond that the heartbeat falls back to the (chunked)
full sync, and the structural overlay (scaling roadmap Phase 2+) is what
scales reachability further.

### Receive-side semantics

`handleRouteSyncDigest` (in `internal/core/node/routing_sync.go`):

1. Capability gate (`CapMeshRouteSyncV1`) already enforced by the
   dispatch switch; the handler defensively no-ops on an empty
   `senderIdentity` (auth-race).
2. When the frame carries a version vector (`entries`), reconcile per
   identity via `Table.ReconcileSyncVector` (above); otherwise fall back
   to the whole-table `Table.SyncDigestFor(senderIdentity)` hash compare.
3. On a match, renew the TTL of the routes learned through the sender
   (`Table.RefreshRoutesVia(senderIdentity, now)`, or the per-entry
   refresh inside `ReconcileSyncVector`) — the receiver half of
   the digest-as-heartbeat freshness mechanism. A match proves those routes
   are exactly what the sender still announces, so they are renewed as if a
   full announce had re-confirmed them, WITHOUT a journal entry (TTL is not a
   wire-projected field, so the bump must not synthesise a delta). A mismatch
   refreshes nothing — the sender follows with a full.
4. Build and emit `route_sync_summary_v1` with `match = local ==
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
   min(10 × AnnounceInterval, DefaultTTL/2)` anchored at `now`, never a
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

**Digest-as-heartbeat (периодическая свежесть).** Тот же обмен работает
и как периодический freshness-heartbeat announce-loop'а: на каждом refresh-
дедлайне (`EffectiveForcedFullSyncInterval`, ≤ TTL/2) отправитель шлёт digest
вместо безусловной пересборки и пересылки всей таблицы. На match'е получатель
продлевает TTL маршрутов, изученных через отправителя (`Table.RefreshRoutesVia`),
— неизменная lineage остаётся живой без байт-тяжёлого full sync, — а full
rebuild срабатывает только если предыдущий heartbeat не подтвердил match
(mismatch или тишина). Именно это убирает периодический full sync со
стабильного пути; гарантия свежести не меняется, потому что потерянный/
разошедшийся digest эскалирует в full на следующем дедлайне ровно как раньше
эскалировал потерянный full. См. `docs/routing.md` «Инвариант интервала
refresh» / «Digest-as-heartbeat refresh».

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
  узел считает `Table.AnnounceDigestVectorFor(peer)` — OUTBOUND announce
  projection к peer'у (что мы анонсировали бы ему, keyed по per-peer
  wire SeqNo'ам, которые peer сохранил), возвращая И хеш, И Phase-0
  version-вектор — и stash'ит ОБА через `Table.RecordPeerDigestSnapshot`.
  Вектор захватывается здесь, а не пересчитывается на реконнекте, потому
  что per-peer outbound SeqNo state (`outboundPeerMax`) реклеймится при
  уходе сессии, так что пост-teardown пересчёт дал бы пустой вектор.
  Перенос его позволяет реконнект-digest сверять per-identity (и значит
  совпадать под K-cap) ровно как периодический heartbeat. Это намеренно НЕ
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
  "generated_at":           "<RFC3339 UTC>",
  "entries":                [ {"i": "<identity-hex>", "s": <uint64>}, ... ]
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
- `entries` — ОПЦИОНАЛЬНЫЙ Phase-0 version-вектор: пары
  `(Identity, SeqNo)`, которые суммирует `digest`, как компактные
  объекты `{"i": hex, "s": seqno}` (по одному на анонсируемую
  identity). Опускается (`omitempty`) отправителями, которые его не
  знают; при наличии — драйвит per-identity reconcile (см. «Phase-0
  version-вектор и safety reconcile» ниже) вместо whole-table hash
  compare. См. `routing.AnnounceDigestVectorFor`.

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

### Phase-0 version-вектор и safety reconcile

Whole-table `digest` — all-or-nothing: любая одна разошедшаяся identity
переворачивает хеш, и нет дешёвого способа найти какая. Под per-origin
K-cap'ом получателя это фатально — получатель отбрасывает cap-rejected
маршруты отправителя, поэтому множество отправителя «что я анонсировал» и
множество получателя «что я сохранил» систематически разные, и хеш НИКОГДА
не совпадёт в плотном меше (наблюдается как `summary_match = 0`). Тогда
forced full срабатывает каждую каденцию, и оптимизация периодической
тишины выше мертва.

Version-вектор (`entries`) локализует сравнение до согласованного
подмножества. Получатель вместо hash-сравнения зовёт
`Table.ReconcileSyncVector(peer, entries, now)`:

- Для каждого живого маршрута via отправитель, чей сохранённый SeqNo равен
  записи вектора по этой identity, продлевает TTL (доказанно-текущий) —
  per-entry эквивалент `RefreshRoutesVia`.
- Сообщает `match = true` только когда НЕ держит ни одного живого
  via-sender маршрута, не подтверждённого вектором (отсутствует или другой
  SeqNo = stale).
- Identity, которую вектор предлагает, но получатель НЕ держит via
  отправитель (cap-rejected offer), НЕ влияет на `match` — это предложение
  отправителя, которое получатель законно не принял. Ровно это и даёт
  совпадение digest'а под cap'ом.

Stale-маршрут у получателя (mismatch вектора) переворачивает
`match = false`, поэтому отправитель эскалирует full его сверить — а
stale-маршрут, не продлённый, уходит по `TickTTL` (никогда не
продлевается). Пир, не знающий вектора, его не шлёт, и получатель падает
на hash-сравнение без изменений.

**Каденция safety reconcile.** Раз digest реально может совпасть, liveness
маршрутов на стабильном пути держится per-entry TTL-рефрешем heartbeat'а
(на forced-full каденции, которая всё ещё удовлетворяет refresh-invariant),
и байт-тяжёлый full перестаёт срабатывать. Stale ловится сразу mismatch'ем
вектора, но маршрут, которого получатель НЕ ИМЕЕТ из-за дропнутой
best-effort дельты, при постоянном match не сверился бы никогда. Поэтому
отправитель форсит full независимо от match раз в
`SafetyFullSyncMultiplier × EffectiveForcedFullSyncInterval` (≈30 мин при
дефолте 300 с), привязанный к последнему успешному full. Это ограничивает
сходимость дропнутых дельт, оставляя стабильный путь в остальном молчащим.

**Лимит размера кадра.** Вектор O(identities), поэтому на большой таблице
он может превысить single-frame `MaxFrameLine` (128 KiB ≈ 2000 entries —
reader команд-плоскости закрывает кадр выше). Два guard'а держат кадр
легальным: announce-loop пропускает вектор и форсит chunked full выше
`maxRouteSyncVectorEntries` (1500), а `buildRouteSyncDigestWire` —
exact-size backstop, который сбрасывает вектор до hash-only кадра, если
построенная строка всё равно превышает лимит (покрывает reconnect-cache
путь, у которого нет entry-count gate). То есть per-identity reconcile
применяется примерно до ~1500 identities; дальше heartbeat падает на
(chunked) full, и структурный оверлей (scaling roadmap Phase 2+) — то, что
масштабирует достижимость дальше.

### Receive-side семантика

`handleRouteSyncDigest` (в `internal/core/node/routing_sync.go`):

1. Capability gate (`CapMeshRouteSyncV1`) уже enforced'нут
   dispatch switch'ем; handler defensive no-op'ит на пустом
   `senderIdentity` (auth-race).
2. Когда фрейм несёт version-вектор (`entries`), сверяет per-identity
   через `Table.ReconcileSyncVector` (выше); иначе падает на whole-table
   hash-сравнение `Table.SyncDigestFor(senderIdentity)`.
3. На match'е продлевает TTL маршрутов, изученных через отправителя
   (`Table.RefreshRoutesVia(senderIdentity, now)` или per-entry рефреш
   внутри `ReconcileSyncVector`) — receiver-половина
   digest-as-heartbeat механизма свежести. Match доказывает, что эти
   маршруты ровно те, что отправитель всё ещё анонсирует, поэтому они
   продлеваются как если бы full announce их переподтвердил, БЕЗ journal-
   записи (TTL не wire-проецируемое поле, bump не должен синтезировать
   дельту). Mismatch не продлевает ничего — отправитель следом шлёт full.
4. Строит и emit'ит `route_sync_summary_v1` с `match = local ==
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
   min(10 × AnnounceInterval, DefaultTTL/2)`, привязанная к `now`, а не
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
