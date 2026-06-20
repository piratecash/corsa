# Routing Commands

## English

### fetchRouteTable

Full routing table snapshot. Returns every entry (active, withdrawn, expired) with detailed metadata.

```bash
corsa-cli fetchRouteTable
```

Response:
```json
{
  "routes": [
    {
      "identity": "abc123...",
      "origin": "def456...",
      "next_hop": {
        "identity": "abc123...",
        "address": "65.108.204.190:64646",
        "network": "ipv4"
      },
      "hops": 1,
      "seq_no": 5,
      "source": "direct",
      "withdrawn": false,
      "expired": false,
      "ttl_seconds": 98.5
    }
  ],
  "total": 3,
  "active": 2,
  "snapshot_at": "2026-04-01T12:00:00Z"
}
```

| Field | Type | Description |
|---|---|---|
| `routes` | array | All route entries in the table |
| `routes[].identity` | string | Destination peer Ed25519 fingerprint |
| `routes[].origin` | string | Peer that originally advertised this route |
| `routes[].next_hop` | object | Direct neighbor peer to forward through |
| `routes[].next_hop.identity` | string | Ed25519 fingerprint of the next-hop peer |
| `routes[].next_hop.address` | string | Live transport address, best-effort (omitted if peer disconnected) |
| `routes[].next_hop.network` | string | Live network type: `"ipv4"`, `"ipv6"`, `"torv3"`, etc., best-effort (omitted if disconnected) |
| `routes[].hops` | int | Distance to destination (0=local/self, 1=direct, 16=withdrawn) |
| `routes[].seq_no` | int | Monotonic sequence number scoped to origin |
| `routes[].source` | string | How route was learned: `"local"`, `"direct"`, `"hop_ack"`, `"announcement"` |
| `routes[].withdrawn` | bool | True if hops >= 16 |
| `routes[].expired` | bool | True if TTL has elapsed (relative to `snapshot_at`) |
| `routes[].ttl_seconds` | float | Remaining time-to-live in seconds (relative to `snapshot_at`) |
| `total` | int | Total entry count (including withdrawn/expired) |
| `active` | int | Non-withdrawn, non-expired entry count |
| `snapshot_at` | string | ISO 8601 timestamp when snapshot was taken |

All time-dependent fields (`expired`, `ttl_seconds`, reachable counts) are computed relative to `snapshot_at`, not wall clock, ensuring self-consistent responses. Routes are sorted deterministically by (identity, origin, next_hop.identity, source).

**Note on `next_hop.address` / `next_hop.network`:** these fields reflect *live* peer transport state queried after the routing snapshot. They are best-effort metadata — a peer may disconnect between the snapshot and the transport lookup. Routing fields (`identity`, `hops`, `seq_no`, `source`, `withdrawn`, `expired`, `ttl_seconds`) are fully atomic within `snapshot_at`.

### fetchRouteSummary

Compact routing table overview for dashboards and monitoring.

```bash
corsa-cli fetchRouteSummary
```

Response:
```json
{
  "snapshot_at": "2026-04-01T12:00:00Z",
  "total_entries": 5,
  "active_entries": 3,
  "withdrawn_entries": 2,
  "reachable_identities": 3,
  "direct_peers": 2,
  "flap_state": [
    {
      "peer_identity": "abc123...",
      "recent_withdrawals": 4,
      "in_hold_down": true,
      "hold_down_until": "2026-04-01T12:01:30Z"
    }
  ],
  "cap_admission": {
    "accepted": 1247,
    "accepted_replaced": 18,
    "rejected_full": 4,
    "rejected_all_protected": 0,
    "seqno_flap_holdowns": 0,
    "fast_invalidations": 0,
    "bad_hops_holdowns": 0
  }
}
```

| Field | Type | Description |
|---|---|---|
| `snapshot_at` | string | RFC 3339 timestamp of the point-in-time snapshot |
| `total_entries` | int | Total route entries in table |
| `active_entries` | int | Non-withdrawn, non-expired entries |
| `withdrawn_entries` | int | Entries with hops=16 or expired |
| `reachable_identities` | int | Unique destinations with at least one *selectable* route, matching `Table.Lookup`: excludes withdrawn/expired, `HealthDead` transit pairs (Direct exempt), and black-hole cooled-down pairs (no Direct exemption) |
| `direct_peers` | int | Destinations reachable via direct route (hops=1, source=direct) |
| `flap_state` | array | Peers with active flap detection state |
| `flap_state[].peer_identity` | string | Peer Ed25519 fingerprint |
| `flap_state[].recent_withdrawals` | int | Disconnect count within flap window |
| `flap_state[].in_hold_down` | bool | True if peer is in hold-down (penalized TTL) |
| `flap_state[].hold_down_until` | string | ISO 8601 timestamp when hold-down expires (only when in_hold_down=true) |
| `cap_admission` | object | Cumulative counters for FOUR independent admission / invalidation policies that share this JSON envelope (the legacy key name is preserved for operator-facing stability — dashboards, env var `CORSA_MAX_NEXT_HOPS_PER_ORIGIN`): (1) the K-cap (`accepted` / `accepted_replaced` / `rejected_full` / `rejected_all_protected`) bounded by `MaxNextHopsPerOrigin`; (2) the SeqNo flap cap (`seqno_flap_holdowns`) bounded by `MaxSeqAdvancePerWindow` + `SeqAdvanceWindow`; (3) fast invalidation on count-to-infinity hops (`fast_invalidations`) bounded by `MaxSaneHops`; (4) bad-hops hysteresis (`bad_hops_holdowns`) — a recidivism hold-down layered on top of policy 3. Each policy has its OWN kill-switch knob — disabling the K-cap does NOT silence the P2/P3/hysteresis counters; see individual field rows below. Post-A1 storage refactor the K-cap semantically bounds K live `UplinkClaim` entries per Identity (Origin was dropped from the dedup key — see `docs/routing.md` core-types section). |
| `cap_admission.accepted` | int | Entries admitted into a per-Identity `UplinkClaim` bucket with room — no eviction. Sourced from announce / hop_ack ingestion only; direct registration via `AddDirectPeer` deliberately does NOT contribute here on a below-K admission, so the counter stays a clean signal for transit traffic. |
| `cap_admission.accepted_replaced` | int | Entries admitted by displacing the worst evictable candidate. Both announce / hop_ack ingestion AND direct registration on a saturated bucket (direct admission cannot be rejected, so the cap evicts to make room) contribute here. `accepted_replaced / (accepted + accepted_replaced)` is the cap eviction rate; a rising rate signals route churn or an undersized cap. The metric tracks cap pressure on announce / hop_ack primarily — direct reconnects against saturated buckets bump the numerator without the denominator, so on nodes with frequent direct churn the rate slightly over-reads. That over-read is itself a real form of cap pressure (a reconnect that would have appended a K+1 row without the cap). |
| `cap_admission.rejected_full` | int | Entries dropped because the per-Identity bucket was at the cap and the incoming entry was not strictly better than the worst evictable candidate. |
| `cap_admission.rejected_all_protected` | int | Entries dropped because every LIVE row in a specific Identity's `UplinkClaim` bucket was direct/local (cap cannot evict those — direct routes model live sessions). Withdrawn rows (tombstones) live outside the K-counted slots so they are never the "all-protected" reason. A direct claim has `Uplink == Identity` so each Identity's bucket holds at most one direct entry; combined with the synthetic self-route this means non-zero `rejected_all_protected` is essentially a "K=1 was too tight for this destination" marker. Non-zero with K≥2 usually points at synthetic test fixtures or direct-restore edge cases rather than legitimate fan-out sizing problems. |
| `cap_admission.seqno_flap_holdowns` | int | Phase 1 P2 SeqNo flap-cap engagement counter. Bumps once per arm: when a single Identity's outbound-SeqNo advance count crosses `MaxSeqAdvancePerWindow` (default 10) inside `SeqAdvanceWindow` (default 5 min), wire emit for that Identity is suppressed to every peer for `DefaultTTL/2` (300 s default — pinned to the forced-full cadence so a forced full sync cannot re-emit the flapping Identity mid-hold-down; stretched with the 120s→600s TTL change). A non-zero counter means at least one upstream lineage drove our outbound SeqNo namespace forward fast enough to trip the cap — usually a count-to-infinity loop, a hostile retransmission storm, or a buggy peer. Subsequent advances during an active hold-down do NOT re-arm (one engage = one bump). Stays at zero on tables built without `WithMaxSeqAdvancePerWindow` (default `0` for the bare Table; production wires `routing.DefaultMaxSeqAdvancePerWindow` via `CORSA_MAX_SEQNO_ADVANCE_PER_WINDOW`). |
| `cap_admission.fast_invalidations` | int | Phase 1 P3 fast-invalidation counter. Bumps once per accepted invalidation: an ingest with `Hops > MaxSaneHops` (default 8) is stored as a tombstone at the observed SeqNo so the SeqNo-resurrection guard for the `(Identity, Uplink)` pair remains intact, and the live row (if any) is replaced. Stale bad-hops replays (`incoming.SeqNo <= old.SeqNo`) are dropped and do NOT bump. The invalidation is strictly local — `AnnounceProjectionFor` only emits own-direct tombstones (`Source == RouteSourceDirect`), so a fast-invalidated transit claim never leaves the node. Stays at zero on tables built without `WithMaxSaneHops` (default `0` for the bare Table; production wires `routing.MaxSaneHops` via `CORSA_MAX_SANE_HOPS`). Phase 1 ships a single bucket; Phase 2 will split into `by_hop_limit` / `by_hop_ack_timeout` sub-counters when the hop_ack timeout path lands. NOTE: claims dropped by the bad-hops hysteresis hold-down (see `bad_hops_holdowns`) never reach this counter — under an active hold-down `fast_invalidations` for the affected Identity stops growing by design. |
| `cap_admission.bad_hops_holdowns` | int | Bad-hops hysteresis engagement counter (recidivism hold-down on top of fast invalidation). Bumps once per arm: when a single Identity accumulates more than `DefaultMaxBadHopsPerWindow` (3) ACCEPTED fast-invalidations within `DefaultBadHopsWindow` (1 min), the Identity enters hold-down — further `Hops > MaxSaneHops` claims for it are dropped before any storage mutation (no tombstone rewrite, no snapshot rebuild, no per-event log), while sane-hops claims (the loop-free recovery path) pass untouched. Re-arms within the 10-minute recidivism window double the hold-down (base 5 min, capped at 30 min); a quiet Identity resets to base. Paired 1:1 with the `routing_bad_hops_hold_down_engaged` warn log; drops during an active hold-down are silent by design. Kill-switch: stays at zero when `MaxSaneHops <= 0` (the hysteresis is only reachable from the fast-invalidation branch) or when the hysteresis budget/window is set non-positive (`WithMaxBadHopsPerWindow(0)` — test/embedding API; unlike the other policies the hysteresis is ENABLED by default and has no env var). |
| `overload` | object | Phase 0 announce-loop overload-gate counters. Stays at zero when the gate is not wired (`CORSA_OVERLOAD_GOROUTINE_THRESHOLD` unset) or has never engaged. |
| `overload.engaged_cycles` | int | Cumulative number of cycles where the gate ACTUALLY shed work — at least one delta-due peer was skipped specifically because of overload during this cycle. Cycles where the gate engaged but every peer happened to be forced-due (initial sync; periodic forced-full deadline; all peers due for full sync simultaneously) do NOT count, because no delta would have been sent anyway and overload didn't change behaviour. Neither do forced-only wakes where no delta cycle was due to begin with. Forced full sync continues to fire on its TTL/2 schedule under engagement (freshness invariant preserved). Monotonic; reset on process restart. A non-zero counter is the strict operator-visible signal that backpressure is actively shedding CPU — pair with goroutine-count metrics (or whatever proxy is wired) to correlate. |
| `digest` | object | route_sync digest-as-heartbeat counters (see `docs/protocol/route_sync.md`). Lets operators tell, without debug logging, whether the periodic heartbeat is suppressing full syncs or diverging. Monotonic; reset on process restart. |
| `digest.heartbeats_sent` | int | Cumulative `route_sync_digest_v1` frames this node emitted (periodic freshness heartbeats + reconnect digests). |
| `digest.summary_match` | int | Inbound `route_sync_summary_v1` verdicts of `match=true` for a digest WE sent — our announced view is confirmed, so the full sync is suppressed and the peer renewed those routes' TTL. |
| `digest.summary_mismatch` | int | Inbound summaries of `match=false` — the digests diverged, so the next deadline escalates to a full sync. **A high `summary_mismatch / (summary_match + summary_mismatch)` ratio means the heartbeat is inert** (every cycle still full-syncs); `summary_match ≫ summary_mismatch` means it is working. `summary_match + summary_mismatch < heartbeats_sent` is the silent / lost-reply share. |
| `digest.digests_compared` | int | Inbound digests this node compared as the RECEIVER (someone else's heartbeat against our via-peer view). |
| `digest.compare_match` | int | Of `digests_compared`, how many matched our view and therefore refreshed those routes' TTL (`Table.RefreshRoutesVia`). The mismatch share is `digests_compared - compare_match`. |
| `journal_churn` | object | Lifetime per-cause tally of change-journal appends (`docs/routing.md`), keyed by stable cause name. Attributes the steady-state announce-plane alloc: it answers "is the table actually changing, or is the control plane churning itself?" Monotonic; reset on process restart. |
| `journal_churn.announce_upsert` | int | A peer's inbound announce changed the wire projection (new claim, SeqNo bump, hops improvement, sig/Extra upgrade) — legitimate "told us something new". |
| `journal_churn.health_aging` | int | `TickHealth`'s passive Good→…→Dead idle timeline crossed the Dead/cooled projection boundary. **Timer-driven, fires with no inbound traffic.** A dominant share here means quiet routes flap Dead↔Good between forced-fulls — control-plane churn, not topology change — and is the prime suspect for steady-state announce alloc. |
| `journal_churn.health_evidence` | int | A hop-ack / probe result (positive or negative) flipped the Dead/cooled projection. Traffic-driven. |
| `journal_churn.ttl_expiry` | int | `TickTTL.CompactExpired` physically removed a backing claim. |
| `journal_churn.holddown_release` | int | A SeqNo flap-cap hold-down expired and the suppressed route re-appeared. |
| `journal_churn.cooldown_clear` | int | A black-hole cooldown was lifted or expired, un-filtering the pair. |
| `journal_churn.peer_remove` / `.transit_invalidate` / `.direct_admit` / `.withdrawal` / `.poison` / `.bulk_reset` | int | Peer-lifecycle and explicit-mutation causes (disconnect withdrawals, transit tombstones, direct-peer admit, inbound wire withdrawal, route_poison, journal bulk reset). Rare in a settled network; a non-trivial share indicates peer or admission churn. |

### fetchRouteLookup

Lookup routes for a specific destination identity. Returns routes ranked by the same Phase 2 `CompositeScore` that the relay path uses through `routing.Table.Lookup` (hops + source-trust bonus + RTT bonus + health penalty). The handler reads route entries from the cached `routing.Snapshot` (lock-free atomic-pointer load), but the per-(Identity, Uplink) `RouteHealthState` tiers come from `RoutingProvider.HealthSnapshot()` per request — a brief `routing.Table.t.mu.RLock` plus a copy, skipped on a lookup miss. The cached `Snapshot.Health` carries only the routing-relevant `{Dead ∪ cooled}` subset (see "Snapshot freshness" below), and CompositeScore needs the full `Good/Questionable/Bad` tiers, so this command is NOT a pure lock-free hot read on a hit. `HealthDead` claims are filtered (matching `Lookup`'s contract); `RouteSourceDirect` claims are exempt from the Dead filter and ranked with a `Bad`-equivalent penalty so they stay selectable as last resort, mirroring local relay-path behaviour. See `docs/protocol/route_health.md` for the full scoring formula.

```bash
corsa-cli fetchRouteLookup <identity>
```

Response:
```json
{
  "identity": "abc123...",
  "routes": [
    {
      "origin": "def456...",
      "next_hop": "abc123...",
      "hops": 1,
      "seq_no": 5,
      "source": "direct",
      "ttl_seconds": 98.5
    },
    {
      "origin": "ghi789...",
      "next_hop": "ghi789...",
      "hops": 2,
      "seq_no": 3,
      "source": "announcement",
      "ttl_seconds": 45.2
    }
  ],
  "count": 2,
  "snapshot_at": "2026-04-01T12:00:00Z"
}
```

| Field | Type | Description |
|---|---|---|
| `identity` | string | Queried destination identity |
| `routes` | array | Routes ranked descending by Phase 2 `CompositeScore` (the same ranking `routing.Table.Lookup` applies on the relay path). `HealthDead` transit claims are filtered out; `RouteSourceDirect` claims are exempt from that filter and ranked with a `Bad`-equivalent penalty. Black-hole cooled-down pairs (`cooldown_until` in the future) are also filtered out, with NO Direct exemption — matching `Table.Lookup`. See `docs/protocol/route_health.md`. Tie-break: `origin` asc, `next_hop` asc, `seq_no` desc — deterministic for diffing across requests. |
| `routes[].origin` | string | Peer that originated this route |
| `routes[].next_hop` | string | Peer to forward through |
| `routes[].hops` | int | Distance to destination |
| `routes[].seq_no` | int | Sequence number |
| `routes[].source` | string | `"local"`, `"direct"`, `"hop_ack"`, or `"announcement"` |
| `routes[].ttl_seconds` | float | Remaining TTL (relative to `snapshot_at`); 0 for local self-route |
| `count` | int | Number of routes found |
| `snapshot_at` | string | ISO 8601 timestamp when snapshot was taken |

When the queried identity matches the node's own identity, a synthetic local route (`source: "local"`, `hops: 0`) is always present. Returns `count: 0` and empty `routes` array if the identity is unknown or has no active routes. Returns 400 if `identity` argument is missing or malformed (must be 40-char lowercase hex).

### fetchRouteHealth

Returns the Phase 2 per-`(Identity, Uplink)` route health snapshot:
health state, RTT EWMA estimate, hop-ack/probe timestamps, and
the probe-failure counter. The handler is a pure read — it does
NOT trigger probe sends or any other side effect. Probes run on
their own ticker (`HealthProbeInterval`, see
`docs/protocol/route_health.md`).

```bash
corsa-cli fetchRouteHealth
```

Response:
```json
{
  "states": [
    {
      "identity":       "abc123...",
      "uplink":         "def456...",
      "health":         "good",
      "rtt_ms":         45,
      "last_hop_ack":   "2026-05-23T11:59:30Z",
      "last_probe":     "2026-05-23T11:59:00Z",
      "probe_failures": 0,
      "transition_at":  "2026-05-23T11:55:00Z"
    }
  ],
  "count":       1,
  "snapshot_at": "2026-05-23T12:00:00Z"
}
```

| Field | Type | Description |
|---|---|---|
| `states` | array | Health states keyed by `(identity, uplink)`. Sorted ascending by `(identity, uplink)`. |
| `states[].identity` | string | Destination identity of the tracked route. |
| `states[].uplink` | string | Direct-peer identity through which the route is tracked. |
| `states[].health` | string | One of `"good"`, `"questionable"`, `"bad"`, `"dead"`. See `docs/protocol/route_health.md` for the state machine. |
| `states[].rtt_ms` | int | EWMA RTT estimate in milliseconds. `0` when no sample has been taken yet. |
| `states[].last_hop_ack` | string | Optional. RFC 3339 timestamp of the last `relay_hop_ack` or `route_probe_ack_v1(reachable=true)` for this pair. Omitted when the pair has never been confirmed. |
| `states[].last_probe` | string | Optional. RFC 3339 timestamp of the last probe OBSERVATION for this pair — either an ack arrival (`applyProbeAck`) or a probe-failure tick (`applyProbeFailure`, fired by the timeout watcher when no ack arrived within `HealthProbeTimeout`). Omitted when no probe has been observed yet. NOTE: the send timestamp is NOT exposed here — it lives only in the in-process outstanding-probe registry (`probeRegistry.outstandingProbe.SentAt`) and never reaches `RouteHealthState`. A probe that is in flight (sent, no ack/timeout yet) therefore appears the same in `last_probe` as a pair that has not been probed at all; operators distinguishing "in-flight" from "cold" need to read `probe_failures` and `last_hop_ack` in conjunction. |
| `states[].probe_failures` | int | Consecutive probe-failure counter. Reset to zero on any successful confirmation; crossing `HealthProbeFailureThreshold` (3) forces `Bad`. |
| `states[].transition_at` | string | Optional RFC 3339 timestamp of the most recent state transition. Useful for "transitioned N seconds ago" dashboards. |
| `count` | int | Number of tracked pairs. |
| `snapshot_at` | string | RFC 3339 timestamp when the snapshot was assembled. |

Returns `count: 0` and an empty `states` array when no health
entries are tracked yet (cold-start, no traffic).

`fetchRouteHealth` is independent of the routing snapshot
discussed in **Snapshot freshness** below — health state has its
own data path through `routing.Table.HealthSnapshot()`, which
takes a read-lock on `t.mu` and returns a deep-copied slice. The
handler is invoked synchronously and the snapshot reflects the
live table state at the moment of the RPC call.

### fetchRouteReputation

```
corsa-cli fetchRouteReputation
```

Phase 3 PR 12.7 — per-`(Identity, Uplink)` hop-ack reputation
snapshot. Returns the same per-pair shape as `fetchRouteHealth`
(by design — a UI can join the two by `(identity, uplink)`
without reshaping), restricted to the reputation fields the
Phase 3 plan adds (`HopAckAttempts`, `HopAckSuccesses`,
`ReliabilityScore`, `ConsecutiveFailures`, `CooldownUntil`).
Wire-protocol contract — `docs/cluster-mesh/phase-3-multipath-reputation.md`
§4.7.

```json
{
  "states": [
    {
      "identity":             "<dest-fp>",
      "uplink":               "<peer-fp>",
      "hop_ack_attempts":     42,
      "hop_ack_successes":    37,
      "reliability_score":    0.88,
      "consecutive_failures": 1,
      "cooldown_until":       "2026-05-23T12:05:00Z"
    },
    ...
  ],
  "count":       N,
  "snapshot_at": "<RFC3339>"
}
```

Field semantics:

- `hop_ack_attempts` / `hop_ack_successes` — raw counters since
  the pair was first observed. Together they yield the long-run
  success ratio; the EWMA-smoothed instantaneous view that
  `CompositeScore` actually consumes is `reliability_score`.
- `reliability_score` — float in `[0, 1]`. Below
  `ReliabilityWarmupSamples=3` attempts the score is reported
  verbatim but `CompositeScore` ignores it.
- `consecutive_failures` — current streak; resets to zero on any
  organic success.
- `cooldown_until` — RFC3339 deadline of an armed black-hole
  cooldown. **Emitted ONLY when armed** (the zero-time
  placeholder is omitted so consumers can rely on "key present"
  ⇔ "cooldown armed"). The arm fires after
  `BlackHoleThreshold=5` consecutive failures; it auto-clears
  once the `BlackHoleCooldown=2min` window elapses via
  `Table.TickHealth.applyCooldownExpiryLocked`, or earlier on an
  organic hop-ack success.
- `count` — total number of tracked pairs (matches `len(states)`).
- `snapshot_at` — RFC3339 timestamp when the handler built the
  response. Same field shape as `fetchRouteHealth`.

States are sorted ascending by `(identity, uplink)` for
deterministic ordering — matches `fetchRouteHealth`'s sort so
joining the two surfaces is just a lockstep iteration.

Returns `count: 0` and an empty `states` array when no
reputation entries are tracked yet (cold-start, no traffic).

**Pure read invariant.** The handler MUST NOT trigger any side
effect — no probe send, no digest emit, no `MarkHopFailure`, no
reputation mutation. Reputation is local-only state (Phase 3
zero-trust budget): observers see a point-in-time view, never a
re-emission. The "force a probe" capability deliberately does
not exist on any transport path; UI aggregation (ranked top-N
reputable uplinks, success-rate histograms) lives on top of this
raw snapshot, never aliased under the `fetchRouteReputation` name.

`fetchRouteReputation` shares the same lock-and-copy contract as
`fetchRouteHealth` — `routing.Table.ReputationSnapshot()` takes
`t.mu.RLock` and returns a deep-copied slice. No
`atomic.Pointer` cache today; promotion is reserved for
telemetry-driven tuning if the read path ever becomes hot enough
to conflict with the writer workload.

### Snapshot freshness

`fetchRouteTable`, `fetchRouteSummary` and `fetchRouteLookup` all read the cached routing snapshot maintained by the node's hot-reads refresher (the same infrastructure that backs `fetch_network_stats`, `fetch_peer_health`, `get_peers` and the ConnectionManager slots view). The snapshot is rebuilt by a background goroutine when the routing table is dirty (i.e. some writer accepted a real mutation since the previous publish); idle ticks skip the rebuild.

One caveat specific to `fetchRouteLookup`: the cached snapshot's `Health` field was narrowed to the routing-relevant `{Dead ∪ cooled}` subset (to stop the publisher deep-copying the whole health set every rebuild), but `fetchRouteLookup`'s CompositeScore ranking needs every pair's `Good/Questionable/Bad` tier. It therefore reads the full health set via `RoutingProvider.HealthSnapshot()` per request — a brief `routing.Table.t.mu.RLock` plus a copy, skipped entirely on a lookup miss (target has no live route). So unlike the other two commands, `fetchRouteLookup` is not a pure lock-free read on a hit, and its route data and health tiers may be skewed by up to one snapshot interval (harmless for a reported ranking).

Two staleness bounds apply:

- **Structural changes** (route accepted, withdrawn, replaced; direct peer added/removed; flap burst arming hold-down; flap-state cleanup after a writer touched the table) are visible to RPC readers within `routingSnapshotMinInterval` (1 s — the coalescing floor that throttles the 500 ms refresher) plus the next refresh tick that crosses that floor (~500 ms) plus the time the publisher needs to acquire `routing.Table.t.mu` (an exclusive `Lock` for the `SnapshotIncremental` build) — on the order of 1–1.5 s. This is the bound that matters for UI dashboards reacting to topology changes.
- **Time-derived state** (`expired`, `ttl_seconds`, `flap_state[].in_hold_down`) is bounded loosely. The dirty-flag publisher only republishes when a writer touches the table, but the wall-clock timestamps that drive these fields advance without writer events:
  - A finite-TTL route quietly elapses — no writer until `TickTTL` (every 10 s) rewrites the entry. `expired` and `ttl_seconds` therefore stay at the snapshot's own `snapshot_at`-based view until `TickTTL` republishes; worst-case lag for "route aged out" equals `TickTTL` interval (≈10 s) plus the structural publish bound (`TickTTL`'s dirty mark still passes the `routingSnapshotMinInterval` floor + a refresh tick, ~1–1.5 s), i.e. ≈11–11.5 s.
  - `flap_state[].in_hold_down` flips from `true` to `false` when `holdDownUntil` elapses — also not a writer event. `TickTTL` clears the deadline and marks the table dirty so the next refresh publishes `in_hold_down=false`. Worst-case lag for hold-down expiry is therefore `TickTTL` interval (≈10 s) + the structural publish bound (~1–1.5 s), i.e. ≈11–11.5 s. Hold-down arming is a writer event (the disconnect that crossed the flap threshold) and is reflected within the structural bound (~1–1.5 s). The snapshot also normalizes `flap_state[].hold_down_until` to absent whenever `in_hold_down=false`, so consumers never observe a past-timestamp deadline paired with `in_hold_down=false`.

  Code that must observe finite-TTL expiry or hold-down expiry promptly should evaluate `ExpiresAt`/`HoldDownUntil` against `time.Now()` directly rather than rely on the cached fields, or call `routing.Table.Lookup(peer)` (per-destination, O(K)) / `routing.Table.Snapshot()` (full table) for a strictly fresh view.

Other consequences:

- The routing read path takes no routing-table mutex — with one exception: `fetchRouteLookup` additionally calls `RoutingProvider.HealthSnapshot()` per request (a brief `t.mu.RLock` + copy, gated on the target having a live route), because the cached `Snapshot.Health` was narrowed to the `{Dead ∪ cooled}` subset and its CompositeScore ranking needs the full tiers. For `fetchRouteTable`/`fetchRouteSummary` (and for `fetchRouteLookup` on a miss) a writer storm on the routing table (announce loop convergence, mass disconnect, hop_ack burst) does not block the RPC; a `fetchRouteLookup` hit can be briefly delayed by a routing-table writer.
- `snapshot_at` is the timestamp at which the cached snapshot was rebuilt, not the timestamp of the RPC call. Two RPCs issued within one refresh interval may report the same `snapshot_at`.
- Time-derived fields stay self-consistent within a single RPC response — every entry in one response is evaluated against the same `snapshot_at`.
- Code that needs strictly fresh state must read the routing table directly inside the node process; this path is not exposed over RPC. File-transfer reachability probes (`file_integration.isPeerReachable`), the locally-originated file send (`filerouter.Router.SendFileCommand` via `RouterConfig.RouteLookup`), and the diagnostic surface (`filerouter.Router.ExplainRoute`, also via `RouteLookup`) all call `routing.Table.Lookup(peer)` for an O(K) per-destination read so a route accepted moments before the call is visible immediately; integration tests that need the full picture call `routing.Table.Snapshot()` directly. The transit forwarding path inside `HandleInbound` keeps using the cached snapshot — an in-flight frame already carries its own metadata and a ~1–1.5 s delay there (the routing snapshot's coalescing floor plus a refresh tick) is harmless.

**Caveat — `fetchRouteTable` next-hop enrichment.** The `next_hop.address` and `next_hop.network` fields are not part of the routing snapshot. They are resolved per unique next-hop identity through `RoutingProvider.PeerTransport`, which currently takes `s.peerMu.RLock`. A peer-domain writer storm can therefore still delay `fetchRouteTable` even though its routing-table snapshot read is lock-free. This `PeerTransport` caveat does not apply to `fetchRouteSummary` or `fetchRouteLookup` — neither emits live transport metadata. (`fetchRouteLookup` does take a routing-table `t.mu.RLock` of its own, but for a different reason — the per-request `HealthSnapshot()` read described under "Snapshot freshness" above, not `PeerTransport`.) Migrating `PeerTransport` onto a cached peer-transport snapshot is tracked separately and is out of scope for the routing-snapshot phase.

### Availability

All routing commands require `RoutingProvider`. When the provider is nil (e.g., node without routing table), commands return 503 and are hidden from `help` output.

---

## Русский

### fetchRouteTable

Полный снапшот таблицы маршрутизации. Возвращает каждую запись (активную, отозванную, истёкшую) с подробными метаданными.

```bash
corsa-cli fetchRouteTable
```

Ответ:
```json
{
  "routes": [
    {
      "identity": "abc123...",
      "origin": "def456...",
      "next_hop": {
        "identity": "abc123...",
        "address": "65.108.204.190:64646",
        "network": "ipv4"
      },
      "hops": 1,
      "seq_no": 5,
      "source": "direct",
      "withdrawn": false,
      "expired": false,
      "ttl_seconds": 98.5
    }
  ],
  "total": 3,
  "active": 2,
  "snapshot_at": "2026-04-01T12:00:00Z"
}
```

| Поле | Тип | Описание |
|---|---|---|
| `routes` | array | Все записи маршрутов в таблице |
| `routes[].identity` | string | Ed25519 fingerprint целевого peer'a |
| `routes[].origin` | string | Peer, изначально объявивший этот маршрут |
| `routes[].next_hop` | object | Прямой сосед для пересылки пакетов |
| `routes[].next_hop.identity` | string | Ed25519 fingerprint next-hop peer'a |
| `routes[].next_hop.address` | string | Живой транспортный адрес, best-effort (отсутствует если peer отключён) |
| `routes[].next_hop.network` | string | Живой тип сети: `"ipv4"`, `"ipv6"`, `"torv3"` и т.д., best-effort (отсутствует если отключён) |
| `routes[].hops` | int | Расстояние до цели (1=прямой, 16=отозван) |
| `routes[].seq_no` | int | Монотонный sequence number, привязанный к origin |
| `routes[].source` | string | Как маршрут был получен: `"direct"`, `"hop_ack"`, `"announcement"` |
| `routes[].withdrawn` | bool | True если hops >= 16 |
| `routes[].expired` | bool | True если TTL истёк (относительно `snapshot_at`) |
| `routes[].ttl_seconds` | float | Оставшееся время жизни в секундах (относительно `snapshot_at`) |
| `total` | int | Общее количество записей (включая отозванные/истёкшие) |
| `active` | int | Количество не-отозванных, не-истёкших записей |
| `snapshot_at` | string | ISO 8601 timestamp момента снапшота |

Все зависящие от времени поля (`expired`, `ttl_seconds`, количество доступных) вычисляются относительно `snapshot_at`, а не текущего времени, обеспечивая внутренне согласованные ответы. Маршруты сортируются детерминированно по (identity, origin, next_hop.identity, source).

**Примечание о `next_hop.address` / `next_hop.network`:** эти поля отражают *живое* транспортное состояние peer'a, запрашиваемое после снапшота маршрутизации. Это best-effort метаданные — peer может отключиться между снапшотом и запросом транспорта. Поля маршрутизации (`identity`, `hops`, `seq_no`, `source`, `withdrawn`, `expired`, `ttl_seconds`) полностью атомарны в рамках `snapshot_at`.

### fetchRouteSummary

Компактный обзор таблицы маршрутизации для дашбордов и мониторинга.

```bash
corsa-cli fetchRouteSummary
```

Ответ:
```json
{
  "snapshot_at": "2026-04-01T12:00:00Z",
  "total_entries": 5,
  "active_entries": 3,
  "withdrawn_entries": 2,
  "reachable_identities": 3,
  "direct_peers": 2,
  "flap_state": [
    {
      "peer_identity": "abc123...",
      "recent_withdrawals": 4,
      "in_hold_down": true,
      "hold_down_until": "2026-04-01T12:01:30Z"
    }
  ],
  "cap_admission": {
    "accepted": 1247,
    "accepted_replaced": 18,
    "rejected_full": 4,
    "rejected_all_protected": 0,
    "seqno_flap_holdowns": 0,
    "fast_invalidations": 0,
    "bad_hops_holdowns": 0
  }
}
```

| Поле | Тип | Описание |
|---|---|---|
| `snapshot_at` | string | RFC 3339 временная метка point-in-time снапшота |
| `total_entries` | int | Всего записей маршрутов в таблице |
| `active_entries` | int | Не-отозванные, не-истёкшие записи |
| `withdrawn_entries` | int | Записи с hops=16 или истёкшие |
| `reachable_identities` | int | Уникальные destinations с хотя бы одним *selectable* маршрутом, как у `Table.Lookup`: исключает withdrawn/expired, `HealthDead` transit-пары (Direct exempt) и black-hole cooled-down пары (без Direct exemption) |
| `direct_peers` | int | Destinations, доступные через direct route (hops=1, source=direct) |
| `flap_state` | array | Peer'ы с активным состоянием flap detection |
| `flap_state[].peer_identity` | string | Ed25519 fingerprint peer'a |
| `flap_state[].recent_withdrawals` | int | Количество отключений в пределах flap window |
| `flap_state[].in_hold_down` | bool | True если peer в hold-down (укороченный TTL) |
| `flap_state[].hold_down_until` | string | ISO 8601 timestamp окончания hold-down (только при in_hold_down=true) |
| `cap_admission` | object | Кумулятивные счётчики ЧЕТЫРЁХ независимых admission / invalidation policies, объединённых в один JSON envelope (legacy ключ сохранён ради operator-facing stability — dashboards, env var `CORSA_MAX_NEXT_HOPS_PER_ORIGIN`): (1) K-cap (`accepted` / `accepted_replaced` / `rejected_full` / `rejected_all_protected`) bounded by `MaxNextHopsPerOrigin`; (2) SeqNo flap cap (`seqno_flap_holdowns`) bounded by `MaxSeqAdvancePerWindow` + `SeqAdvanceWindow`; (3) fast invalidation на count-to-infinity hops (`fast_invalidations`) bounded by `MaxSaneHops`; (4) bad-hops hysteresis (`bad_hops_holdowns`) — recidivism hold-down поверх policy 3. У каждой policy свой kill-switch knob — отключение K-cap НЕ замолкает P2/P3/hysteresis counters; см. описания индивидуальных полей ниже. Post-A1 storage refactor K-cap семантически bounds K live `UplinkClaim` записей per Identity (Origin был dropped из dedup key — см. core-types section в `docs/routing.md`). |
| `cap_admission.accepted` | int | Записи, admitted в per-Identity `UplinkClaim` bucket с местом — без eviction. Источник — только announce / hop_ack ingestion; direct registration через `AddDirectPeer` сознательно НЕ контрибутит сюда на below-K admission, чтобы counter оставался чистым сигналом transit-трафика. |
| `cap_admission.accepted_replaced` | int | Записи, admitted через displacement худшего evictable кандидата. Контрибутят и announce / hop_ack ingestion, и direct registration на saturated bucket'е (direct admission не может быть rejected, cap evict'ит чтобы освободить место). `accepted_replaced / (accepted + accepted_replaced)` — cap eviction rate; растущий rate сигналит route churn или undersized cap. Метрика отслеживает cap pressure на announce / hop_ack в первую очередь — direct reconnect'ы против saturated bucket'ов бампают числитель, но не знаменатель, так что на нодах с частым direct churn rate слегка завышен. Этот over-read — сам по себе вид cap pressure (reconnect, который без cap'а вставил бы K+1 строку). |
| `cap_admission.rejected_full` | int | Записи, отклонённые потому что per-Identity bucket был at-cap и incoming не строго лучше worst evictable кандидата. |
| `cap_admission.rejected_all_protected` | int | Записи, отклонённые потому что каждая LIVE строка в конкретном bucket'е Identity была direct/local (cap не может evict'нуть таких — direct routes модельны live session'ам). Withdrawn строки (tombstones) живут вне K-counted slots, поэтому они никогда не являются причиной "all-protected". Direct claim имеет `Uplink == Identity`, поэтому каждый Identity bucket содержит максимум одну direct entry; вместе с synthetic self-route это означает что ненулевой `rejected_all_protected` — по сути маркер "K=1 оказался слишком жёстким для этого destination". Ненулевое при K≥2 обычно указывает на synthetic test fixtures или direct-restore edge case, а не на legitimate fan-out sizing problem. |
| `cap_admission.seqno_flap_holdowns` | int | Phase 1 P2 SeqNo flap-cap engagement counter. Бампится один раз per arm: когда per-Identity outbound-SeqNo advance count пересекает `MaxSeqAdvancePerWindow` (default 10) внутри `SeqAdvanceWindow` (default 5 min), wire emit для этой Identity подавляется для всех peer'ов на `DefaultTTL/2` (300 s default — pinned к forced-full cadence, чтобы forced full sync не переотправил флапающую Identity посреди hold-down; растянулся вместе с TTL 120s→600s). Ненулевой counter означает что хотя бы один upstream lineage drove наш outbound SeqNo namespace forward достаточно быстро чтобы trip'нуть cap — обычно count-to-infinity loop, hostile retransmission storm или buggy peer. Последующие advances во время активного hold-down НЕ re-arm'ят (один engage = один bump). Остаётся на нуле на таблицах построенных без `WithMaxSeqAdvancePerWindow` (default `0` для bare Table; production wired'ит `routing.DefaultMaxSeqAdvancePerWindow` через `CORSA_MAX_SEQNO_ADVANCE_PER_WINDOW`). |
| `cap_admission.fast_invalidations` | int | Phase 1 P3 fast-invalidation counter. Бампится один раз per accepted invalidation: ingest с `Hops > MaxSaneHops` (default 8) сохраняется как tombstone на observed SeqNo (SeqNo-resurrection guard для пары `(Identity, Uplink)` остаётся intact), и live row (если был) замещается. Stale bad-hops replays (`incoming.SeqNo <= old.SeqNo`) drop'аются и НЕ бампят. Invalidation строго локальная — `AnnounceProjectionFor` emit'ит только own-direct tombstones (`Source == RouteSourceDirect`), поэтому fast-invalidated transit claim никогда не покидает узел. Остаётся на нуле на таблицах построенных без `WithMaxSaneHops` (default `0` для bare Table; production wired'ит `routing.MaxSaneHops` через `CORSA_MAX_SANE_HOPS`). Phase 1 ships один bucket; Phase 2 разделит на `by_hop_limit` / `by_hop_ack_timeout` sub-counters когда hop_ack timeout путь land'нется. ЗАМЕЧАНИЕ: claims, dropped гистерезисным hold-down'ом (см. `bad_hops_holdowns`), до этого counter'а не доходят — при активном hold-down `fast_invalidations` по затронутой Identity перестаёт расти by design. |
| `cap_admission.bad_hops_holdowns` | int | Engagement counter гистерезиса bad-hops (recidivism hold-down поверх fast invalidation). Бампится один раз per arm: когда одна Identity набирает больше `DefaultMaxBadHopsPerWindow` (3) ПРИНЯТЫХ fast-invalidations внутри `DefaultBadHopsWindow` (1 min), Identity входит в hold-down — дальнейшие claims с `Hops > MaxSaneHops` для неё drop'аются ДО любых мутаций storage (без перезаписи tombstone, без snapshot rebuild, без per-event лога), а sane-hops claims (loop-free recovery path) проходят нетронутыми. Re-arm внутри 10-минутного recidivism window удваивает hold-down (base 5 min, кап 30 min); тихая Identity сбрасывается на base. Paired 1:1 с warn-логом `routing_bad_hops_hold_down_engaged`; drops при активном hold-down тихие by design. Kill-switch: остаётся на нуле когда `MaxSaneHops <= 0` (гистерезис достижим только из fast-invalidation branch) или когда budget/window гистерезиса non-positive (`WithMaxBadHopsPerWindow(0)` — test/embedding API; в отличие от остальных policies гистерезис ENABLED by default и env var не имеет). |
| `overload` | object | Счётчики overload-gate announce-loop'а из Phase 0. Остаётся на нуле когда gate не wired (`CORSA_OVERLOAD_GOROUTINE_THRESHOLD` не установлен) или ни разу не engage'ил. |
| `overload.engaged_cycles` | int | Кумулятивное число cycle'ов, где gate ФАКТИЧЕСКИ shed'ит работу — хотя бы один delta-due peer был skip'нут именно из-за overload в этом cycle'е. Cycle'ы где gate engaged, но каждый peer оказался forced-due (initial sync; periodic forced-full deadline; все peer'ы due for full sync одновременно) НЕ считаются, потому что delta всё равно не отправлялась бы и overload не изменил поведение. Forced-only wakes (no delta due) тоже не считаются. Forced full sync продолжает срабатывать по TTL/2 расписанию даже при engagement (freshness invariant preserved). Monotonic, сбрасывается при рестарте процесса. Ненулевой counter — строгий operator-visible сигнал что backpressure активно shed'ит CPU; пара с goroutine-count метрикой (или другим wired'ным proxy) для корреляции. |
| `digest` | object | Счётчики route_sync digest-as-heartbeat (см. `docs/protocol/route_sync.md`). Позволяют без debug-логов понять, подавляет ли периодический heartbeat full sync'и или расходится. Monotonic, сбрасываются при рестарте процесса. |
| `digest.heartbeats_sent` | int | Кумулятивное число `route_sync_digest_v1`-фреймов, отправленных этой нодой (периодические freshness-heartbeat'ы + reconnect-digest'ы). |
| `digest.summary_match` | int | Входящие `route_sync_summary_v1`-вердикты `match=true` на digest, который МЫ отправили — наше анонсируемое представление подтверждено, full sync подавлён, peer продлил TTL этих маршрутов. |
| `digest.summary_mismatch` | int | Входящие summary с `match=false` — digest'ы разошлись, следующий дедлайн эскалирует в full sync. **Высокая доля `summary_mismatch / (summary_match + summary_mismatch)` означает, что heartbeat бесполезен** (каждый цикл всё равно делает full sync); `summary_match ≫ summary_mismatch` — работает. `summary_match + summary_mismatch < heartbeats_sent` — доля молчания / потерянных ответов. |
| `digest.digests_compared` | int | Входящие digest'ы, которые эта нода сравнила как ПОЛУЧАТЕЛЬ (чужой heartbeat против нашего via-peer представления). |
| `digest.compare_match` | int | Сколько из `digests_compared` совпало с нашим представлением и потому продлило TTL этих маршрутов (`Table.RefreshRoutesVia`). Доля mismatch — `digests_compared - compare_match`. |
| `journal_churn` | object | Кумулятивная разбивка append'ов change-журнала по причинам (`docs/routing.md`), ключ — стабильное имя причины. Атрибутирует steady-state alloc announce-плоскости: отвечает на вопрос «таблица реально меняется или control plane churn'ит сам себя?». Monotonic, сбрасывается при рестарте процесса. |
| `journal_churn.announce_upsert` | int | Входящий announce от peer'а изменил wire-проекцию (новый claim, SeqNo bump, улучшение hops, sig/Extra upgrade) — легитимное «нам сообщили новое». |
| `journal_churn.health_aging` | int | Пассивный idle-таймлайн `TickHealth` (Good→…→Dead) пересёк Dead/cooled-проекцию. **Timer-driven, срабатывает без входящего трафика.** Доминирующая доля = тихие маршруты флапают Dead↔Good между forced-full'ами (churn control-плоскости, не смена топологии) — главный подозреваемый по steady-state announce alloc. |
| `journal_churn.health_evidence` | int | hop-ack / probe результат (позитивный или негативный) флипнул Dead/cooled-проекцию. Traffic-driven. |
| `journal_churn.ttl_expiry` | int | `TickTTL.CompactExpired` физически удалил backing claim. |
| `journal_churn.holddown_release` | int | SeqNo flap-cap hold-down истёк, подавленный маршрут вернулся. |
| `journal_churn.cooldown_clear` | int | Black-hole cooldown снят или истёк, пара разфильтрована. |
| `journal_churn.peer_remove` / `.transit_invalidate` / `.direct_admit` / `.withdrawal` / `.poison` / `.bulk_reset` | int | Причины peer-lifecycle и явных мутаций (withdrawal'ы при disconnect, транзитные tombstone'ы, admit прямого peer'а, входящий wire-withdrawal, route_poison, bulk reset журнала). В устаканившейся сети редки; нетривиальная доля = churn peer'ов или admission'а. |

### fetchRouteLookup

Поиск маршрутов для конкретного destination identity. Маршруты ранжируются по Phase 2 `CompositeScore` — тот же ranking, который relay path применяет в `routing.Table.Lookup` (hops + source-trust bonus + RTT bonus + health penalty). Handler читает route entries из кешированного `routing.Snapshot` (lock-free atomic-pointer load), но per-(Identity, Uplink) `RouteHealthState`-тиры берёт из `RoutingProvider.HealthSnapshot()` на каждый запрос — короткий `routing.Table.t.mu.RLock` плюс копия, пропускается на lookup-miss. Кешированный `Snapshot.Health` несёт только routing-relevant подмножество `{Dead ∪ cooled}` (см. «Свежесть снапшота» ниже), а CompositeScore нужны полные тиры `Good/Questionable/Bad`, поэтому на hit'е эта команда — НЕ чистый lock-free hot read. `HealthDead` claims фильтруются (контракт совпадает с `Lookup`); `RouteSourceDirect` claims exempt от Dead-фильтра и ранжируются с `Bad`-equivalent penalty, оставаясь selectable как last resort — отражает локальное relay-path поведение. Black-hole cooled-down пары (`cooldown_until` в будущем) тоже фильтруются, БЕЗ Direct exemption — как в `Table.Lookup`. Полная формула — в `docs/protocol/route_health.md`.

```bash
corsa-cli fetchRouteLookup <identity>
```

Ответ:
```json
{
  "identity": "abc123...",
  "routes": [
    {
      "origin": "def456...",
      "next_hop": "abc123...",
      "hops": 1,
      "seq_no": 5,
      "source": "direct",
      "ttl_seconds": 98.5
    },
    {
      "origin": "ghi789...",
      "next_hop": "ghi789...",
      "hops": 2,
      "seq_no": 3,
      "source": "announcement",
      "ttl_seconds": 45.2
    }
  ],
  "count": 2,
  "snapshot_at": "2026-04-01T12:00:00Z"
}
```

| Поле | Тип | Описание |
|---|---|---|
| `identity` | string | Запрошенный destination identity |
| `routes` | array | Маршруты ранжируются по убыванию Phase 2 `CompositeScore` (тот же ranking, что `routing.Table.Lookup` применяет на relay path). `HealthDead` transit claims фильтруются; `RouteSourceDirect` claims exempt от этого фильтра и ранжируются с `Bad`-equivalent penalty. См. `docs/protocol/route_health.md`. Tie-break: `origin` asc, `next_hop` asc, `seq_no` desc — детерминизм для diff'ов между запросами. |
| `routes[].origin` | string | Peer, породивший этот маршрут |
| `routes[].next_hop` | string | Peer для пересылки |
| `routes[].hops` | int | Расстояние до цели |
| `routes[].seq_no` | int | Sequence number |
| `routes[].source` | string | `"local"`, `"direct"`, `"hop_ack"` или `"announcement"` |
| `routes[].ttl_seconds` | float | Оставшийся TTL (относительно `snapshot_at`); 0 для локального собственного маршрута |
| `count` | int | Количество найденных маршрутов |
| `snapshot_at` | string | ISO 8601 timestamp момента снапшота |

Когда запрашиваемый identity совпадает с собственным identity ноды, синтетический локальный маршрут (`source: "local"`, `hops: 0`) всегда присутствует. Возвращает `count: 0` и пустой массив `routes`, если identity неизвестен или не имеет активных маршрутов. Возвращает 400, если аргумент `identity` отсутствует или имеет неверный формат (должен быть 40-символьный hex в нижнем регистре).

### fetchRouteHealth

Возвращает Phase 2 snapshot per-`(Identity, Uplink)` health: состояние, RTT EWMA, timestamps hop-ack/probe и счётчик probe-failures. Handler — pure read и НЕ триггерит probe-send'ы или другие side effects. Probe'ы запускаются на собственном ticker'е (`HealthProbeInterval`, см. `docs/protocol/route_health.md`).

```bash
corsa-cli fetchRouteHealth
```

Ответ:
```json
{
  "states": [
    {
      "identity":       "abc123...",
      "uplink":         "def456...",
      "health":         "good",
      "rtt_ms":         45,
      "last_hop_ack":   "2026-05-23T11:59:30Z",
      "last_probe":     "2026-05-23T11:59:00Z",
      "probe_failures": 0,
      "transition_at":  "2026-05-23T11:55:00Z"
    }
  ],
  "count":       1,
  "snapshot_at": "2026-05-23T12:00:00Z"
}
```

| Поле | Тип | Описание |
|---|---|---|
| `states` | array | Health-state'ы по ключу `(identity, uplink)`. Отсортированы по возрастанию `(identity, uplink)`. |
| `states[].identity` | string | Destination identity отслеживаемого маршрута. |
| `states[].uplink` | string | Direct-peer identity, через который маршрут отслеживается. |
| `states[].health` | string | Одно из `"good"`, `"questionable"`, `"bad"`, `"dead"`. State machine описана в `docs/protocol/route_health.md`. |
| `states[].rtt_ms` | int | EWMA-оценка RTT в миллисекундах. `0` когда samples ещё не было. |
| `states[].last_hop_ack` | string | Optional. RFC 3339 timestamp последнего `relay_hop_ack` или `route_probe_ack_v1(reachable=true)`. Отсутствует когда пара ни разу не подтверждалась. |
| `states[].last_probe` | string | Optional. RFC 3339 timestamp последнего НАБЛЮДЕНИЯ probe для пары — либо приход ack'а (`applyProbeAck`), либо probe-failure tick (`applyProbeFailure`, срабатывает в timeout watcher'е когда ack не пришёл за `HealthProbeTimeout`). Отсутствует когда наблюдений ещё не было. NB: send timestamp здесь НЕ выставляется — он живёт только в in-process outstanding-probe registry (`probeRegistry.outstandingProbe.SentAt`) и в `RouteHealthState` не попадает. In-flight probe (отправлен, ack/timeout ещё не пришли) выглядит в `last_probe` так же как пара, которую ни разу не пробовали; операторы, отличающие "in-flight" от "cold", смотрят `probe_failures` и `last_hop_ack` в комбинации. |
| `states[].probe_failures` | int | Счётчик подряд probe-failures. Сбрасывается на любое успешное подтверждение; пересечение `HealthProbeFailureThreshold` (3) форсит `Bad`. |
| `states[].transition_at` | string | Optional RFC 3339 timestamp последнего state-transition'а. Используется для «transitioned N seconds ago» дашбордов. |
| `count` | int | Количество отслеживаемых пар. |
| `snapshot_at` | string | RFC 3339 timestamp когда snapshot собран. |

Возвращает `count: 0` и пустой `states`, когда health-entries ещё не отслеживаются (cold-start, нет трафика).

`fetchRouteHealth` независим от snapshot маршрутизации, обсуждаемого в **Свежесть снапшота** ниже — health state имеет собственный data path через `routing.Table.HealthSnapshot()`, который берёт read-lock на `t.mu` и возвращает deep-copied slice. Handler инвокается синхронно, snapshot отражает live состояние таблицы на момент RPC-вызова.

### fetchRouteReputation

```
corsa-cli fetchRouteReputation
```

Phase 3 PR 12.7 — per-`(Identity, Uplink)` snapshot hop-ack
репутации. Возвращает ту же per-pair форму, что и
`fetchRouteHealth` (намеренно — UI может join'ить два по
`(identity, uplink)` без reshape'а), ограниченную полями
репутации, которые добавил Phase 3 plan (`HopAckAttempts`,
`HopAckSuccesses`, `ReliabilityScore`, `ConsecutiveFailures`,
`CooldownUntil`). Wire-protocol контракт —
`docs/cluster-mesh/phase-3-multipath-reputation.md` §4.7.

```json
{
  "states": [
    {
      "identity":             "<dest-fp>",
      "uplink":               "<peer-fp>",
      "hop_ack_attempts":     42,
      "hop_ack_successes":    37,
      "reliability_score":    0.88,
      "consecutive_failures": 1,
      "cooldown_until":       "2026-05-23T12:05:00Z"
    },
    ...
  ],
  "count":       N,
  "snapshot_at": "<RFC3339>"
}
```

Семантика полей:

- `hop_ack_attempts` / `hop_ack_successes` — сырые счётчики с
  момента первого наблюдения пары. Вместе дают long-run success
  ratio; EWMA-smoothed instant view, который реально потребляет
  `CompositeScore`, — это `reliability_score`.
- `reliability_score` — float в `[0, 1]`. Ниже
  `ReliabilityWarmupSamples=3` attempts score выводится
  verbatim, но `CompositeScore` его игнорирует.
- `consecutive_failures` — текущий streak; сбрасывается на
  любой organic success.
- `cooldown_until` — RFC3339 deadline armed black-hole
  cooldown'а. **Emit'ится ТОЛЬКО при armed** (zero-time
  placeholder опускается, чтобы консумеры могли полагаться на
  «key present» ⇔ «cooldown armed»). Arm срабатывает после
  `BlackHoleThreshold=5` подряд failures; auto-clear'ится когда
  `BlackHoleCooldown=2мин` окно elaps'ится через
  `Table.TickHealth.applyCooldownExpiryLocked`, или раньше —
  на organic hop-ack success'е.
- `count` — общее число tracked pairs (matches `len(states)`).
- `snapshot_at` — RFC3339 timestamp когда handler собрал
  response. Та же field shape, что и у `fetchRouteHealth`.

States сортируются по возрастанию `(identity, uplink)` для
детерминированного ордера — matches sort `fetchRouteHealth`'а,
так что join двух surface'ов — это просто lockstep iteration.

Возвращает `count: 0` и пустой `states`, когда reputation-
entries ещё не отслеживаются (cold-start, нет трафика).

**Pure read invariant.** Handler НЕ ДОЛЖЕН triggers'ить ни
один side effect — никакого probe send, digest emit,
`MarkHopFailure`, reputation mutation. Reputation — local-only
state (Phase 3 zero-trust budget): observers видят point-in-time
view, никогда re-emission. «Force a probe» capability
deliberately не существует ни на одном transport path; UI
aggregation (ranked top-N reputable uplinks, success-rate
histograms) живёт поверх этого raw snapshot'а, никогда не
alias'нутый под именем `fetchRouteReputation`.

`fetchRouteReputation` разделяет тот же lock-and-copy контракт
что и `fetchRouteHealth` — `routing.Table.ReputationSnapshot()`
берёт `t.mu.RLock` и возвращает deep-copied slice. Никакого
`atomic.Pointer` cache на сегодня; promotion зарезервирован для
telemetry-driven tuning'а, если read path когда-нибудь станет
hot'ным enough для конфликта с writer workload.

### Свежесть снапшота

`fetchRouteTable`, `fetchRouteSummary` и `fetchRouteLookup` читают кэшированный снапшот маршрутизации, поддерживаемый фоновым refresher'ом ноды (та же инфраструктура, что обслуживает `fetch_network_stats`, `fetch_peer_health`, `get_peers` и view ConnectionManager). Снапшот перестраивается фоновой горутиной только когда таблица стала dirty (то есть какой-то writer принял реальную мутацию с момента предыдущего publish'а); idle-тики rebuild не делают.

Отдельная оговорка про `fetchRouteLookup`: поле `Health` кэшированного снапшота сужено до routing-relevant подмножества `{Dead ∪ cooled}` (чтобы publisher не копировал весь health-набор в каждый rebuild), но CompositeScore-ранжированию `fetchRouteLookup` нужны все тиры пар `Good/Questionable/Bad`. Поэтому он читает полный health-набор через `RoutingProvider.HealthSnapshot()` на каждый запрос — короткий `routing.Table.t.mu.RLock` плюс копия, полностью пропускается на lookup-miss (у target нет живого маршрута). Так что в отличие от двух других команд `fetchRouteLookup` на hit'е не является чистым lock-free чтением, и его route-данные и health-тиры могут разъехаться на величину до одного snapshot-интервала (безвредно для отчётного ранжирования).

Действуют две разные границы свежести:

- **Структурные изменения** (маршрут принят, отозван, заменён; добавлен/удалён direct peer; flap-burst, армирующий hold-down; flap-state cleanup после writer-touch'а) видны RPC-читателям не позже чем через `routingSnapshotMinInterval` (1 s — порог coalescing'а, троттлящий 500 ms refresher) плюс ближайший refresh-тик, пересекающий этот порог (~500 ms), плюс время, нужное publisher'у на захват `routing.Table.t.mu` (эксклюзивный `Lock` для сборки `SnapshotIncremental`) — порядка 1–1.5 s. Это та граница, что важна для UI-дашбордов, реагирующих на изменения топологии.
- **Time-производные поля** (`expired`, `ttl_seconds`, `flap_state[].in_hold_down`) ограничены мягче. Dirty-флаг publisher срабатывает только когда writer трогает таблицу, но wall-clock timestamps, которые двигают эти поля, идут вперёд без writer-событий:
  - Маршрут с конечным TTL тихо протухает — никакого writer'а до тех пор, пока `TickTTL` (каждые 10 s) не перепишет запись. `expired` и `ttl_seconds` поэтому остаются на представлении относительно собственного `snapshot_at` снапшота, пока `TickTTL` не сделает republish; worst-case lag «маршрут протух» равен интервалу `TickTTL` (≈10 s) плюс структурная граница публикации (dirty-пометка `TickTTL` всё равно проходит через порог `routingSnapshotMinInterval` + refresh-тик, ~1–1.5 s), т.е. ≈11–11.5 s.
  - `flap_state[].in_hold_down` переключается с `true` на `false` когда `holdDownUntil` истекает — тоже не writer-событие. `TickTTL` обнуляет deadline и помечает таблицу dirty, чтобы следующий refresh опубликовал `in_hold_down=false`. Worst-case lag для истечения hold-down поэтому равен `TickTTL` interval (≈10 s) + структурная граница публикации (~1–1.5 s), т.е. ≈11–11.5 s. Армирование hold-down — это writer-событие (disconnect, который пересёк flap threshold), и оно отражается в пределах структурной границы (~1–1.5 s). Снапшот также нормализует `flap_state[].hold_down_until` до отсутствующего, когда `in_hold_down=false`, поэтому консумеры никогда не наблюдают past-timestamp deadline в паре с `in_hold_down=false`.

  Код, который обязан промптно отрабатывать TTL-протухание или истечение hold-down, должен сравнивать `ExpiresAt`/`HoldDownUntil` с `time.Now()` напрямую, а не полагаться на кэшированные поля, либо звать `routing.Table.Lookup(peer)` (per-destination, O(K)) / `routing.Table.Snapshot()` (вся таблица) для строго свежего вида.

Прочие следствия:

- Routing-чтение RPC не берёт мьютекс таблицы маршрутизации — с одним исключением: `fetchRouteLookup` дополнительно зовёт `RoutingProvider.HealthSnapshot()` на каждый запрос (короткий `t.mu.RLock` плюс копия, гейтится наличием живого маршрута у target), потому что кешированный `Snapshot.Health` сужен до подмножества `{Dead ∪ cooled}`, а его CompositeScore-ранжированию нужны полные тиры. Для `fetchRouteTable`/`fetchRouteSummary` (и для `fetchRouteLookup` на miss) writer-шторм на таблице (конвергенция announce-цикла, массовый disconnect, всплеск hop_ack) RPC не блокирует; `fetchRouteLookup` на hit может быть кратко задержан writer'ом таблицы.
- `snapshot_at` — это момент, когда кэшированный снапшот был перестроен, а не момент RPC-вызова. Два RPC внутри одного refresh-интервала могут вернуть один и тот же `snapshot_at`.
- Time-производные поля остаются самосогласованными в пределах одного ответа — каждая запись вычисляется относительно одного и того же `snapshot_at`.
- Коду, которому нужно строго свежее состояние, следует читать таблицу напрямую внутри процесса ноды; этот путь не экспонируется по RPC. File-transfer probe-ы (`file_integration.isPeerReachable`), locally-originated отправка файлов (`filerouter.Router.SendFileCommand` через `RouterConfig.RouteLookup`) и диагностический surface (`filerouter.Router.ExplainRoute`, тоже через `RouteLookup`) — все зовут `routing.Table.Lookup(peer)` для O(K) per-destination чтения, чтобы маршрут, принятый за моменты до вызова, был виден немедленно; интеграционные тесты, которым нужна полная картина, зовут `routing.Table.Snapshot()` напрямую. Transit-форвард внутри `HandleInbound` продолжает использовать кэшированный snapshot — in-flight frame уже несёт собственные metadata и ~1–1.5 с задержка там (порог coalescing'а снапшота плюс refresh-тик) безвредна.

**Оговорка — обогащение `next_hop` в `fetchRouteTable`.** Поля `next_hop.address` и `next_hop.network` не входят в снапшот маршрутизации. Они резолвятся per уникальный next-hop identity через `RoutingProvider.PeerTransport`, который в текущей реализации берёт `s.peerMu.RLock`. Writer-шторм в peer-домене поэтому всё ещё может задержать `fetchRouteTable`, хотя его чтение routing-снапшота уже lock-free. Эта `PeerTransport`-оговорка не относится к `fetchRouteSummary` и `fetchRouteLookup` — ни один из них не отдаёт live transport metadata. (`fetchRouteLookup` всё же берёт собственный `t.mu.RLock` таблицы маршрутизации, но по другой причине — per-request `HealthSnapshot()`, описанный в «Свежесть снапшота» выше, а не `PeerTransport`.) Миграция `PeerTransport` на кэшированный peer-transport снапшот отслеживается отдельно и выходит за рамки этапа routing-снапшота.

### Доступность

Все routing-команды требуют `RoutingProvider`. Когда провайдер nil (например, нода без таблицы маршрутизации), команды возвращают 503 и скрыты из вывода `help`.
