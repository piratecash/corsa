package routing

import (
	"encoding/json"
	"errors"
	"time"
)

// UplinkClaim is the per-uplink storage shape introduced by
// cluster-mesh Phase 1 P0 Phase A. It replaces the per-
// (Identity, Origin, NextHop) RouteEntry triple as the unit of
// storage inside routeStore.
//
// The motivation is field-data analysis of routes.json on a
// 100-node test mesh: 6072 entries with 803 unique (Identity,
// NextHop) pairs and only 113 unique destinations — 12.7 origin
// lineages per identity on average. The Origin field carried
// protocol metadata (announce provenance) but contributed
// nothing to routing decisions; collapsing storage to per-
// (Identity, Uplink) removes the ~7-13× redundancy and the lock
// contention it caused.
//
// Phase 1 P0 Phase A is split into substeps inside this codebase:
//
//   - A0 (this file lands): introduce UplinkClaim and the
//     conversion helpers, with unit-tested invariants. Storage
//     stays on RouteEntry; no Table behaviour changes.
//   - A1: route_store_*.go internal storage swaps from
//     []RouteEntry to []UplinkClaim. ApplyUpdate consumes Origin
//     for anti-spoof on ingest and drops it; LookupActive /
//     AnnounceProjectionFor / AnnounceableFor synthesize boundary
//     RouteEntry values with Origin = localOrigin (with fallback
//     to Identity when localOrigin is empty — test fixtures
//     without WithLocalOrigin). The phase-1 doc originally
//     proposed Origin = Identity here, but that breaks pre-A1
//     receivers' withdrawal anti-spoof (Origin != senderIdentity
//     → reject) because live emit and withdrawal emit would land
//     on different (Identity, Origin, NextHop) triples on the
//     receiver. Choosing Origin = localOrigin makes live and
//     withdrawal share the same triple and keeps the legacy
//     anti-spoof working through the mixed rollout.
//   - A2: announce_builder.go stage 4 simplifies (storage already
//     yields per-Identity per-Uplink shape).
//   - A3 / A4: snapshot RPC schema rename (cap_admission →
//     uplink_admission) + MaxNextHopsPerOrigin deprecation are
//     both deferred to the Phase 6 cleanup window. Until then the
//     knob and JSON key stay stable for operator-facing tooling;
//     the substantive storage swap that A1/A2 deliver is what
//     callers see today.
//
// Wire format does NOT change in Phase A — emit side keeps the
// legacy AnnounceEntry shape with Origin = localOrigin
// ("sender originated route" semantic, matching what pre-A1
// own-direct emits had). Pre-A1 receivers key storage per-
// (Identity, Origin, NextHop) and find a consistent triple
// (Identity, sender, sender) for live AND withdrawal frames
// from the same sender — withdrawal anti-spoof (Origin ==
// sender) passes and the live entry is retired immediately.
// Post-A1 receivers drop Origin on ingest, so the value is
// opaque to them.
type UplinkClaim struct {
	// Uplink is the direct peer who told us about this claim — was
	// previously RouteEntry.NextHop. After the storage swap every
	// entry in routeStore is keyed by (destination Identity,
	// Uplink); Origin is consumed for anti-spoof on ingest and
	// then dropped because it carries no routing information
	// beyond "who originally created this announce".
	Uplink PeerIdentity

	// Hops is the distance to the destination after the relay's
	// own hop count has been incremented. 1 means the uplink IS
	// the destination (direct route via that uplink). Range is
	// 1..HopsInfinity-1 (15) for live claims; HopsInfinity (16)
	// is the wire sentinel for withdrawals but inside storage the
	// dedicated Withdrawn flag below is the canonical signal — a
	// withdrawn claim keeps its Hops value as observed at
	// withdrawal time, see IsWithdrawn.
	//
	// uint8 (vs RouteEntry.Hops int) reflects the actual value
	// range 0..16 and saves 7 bytes per claim — on a 100-node mesh
	// with ~5000 claims this is ~35 KB of heap. The conversion
	// across the boundary (RouteEntry int → UplinkClaim uint8) is
	// safe because RouteEntry.Validate() enforces the same 0..16
	// range upstream.
	Hops uint8

	// SeqNo is the origin's monotonically increasing sequence
	// number as relayed by the uplink. Per-Identity scope (the
	// swap dropped per-(Identity, Origin) scoping in favour of
	// pure per-Identity counters — see route_store.seqCounters).
	// Tombstones keep their SeqNo as the resurrection guard for
	// delayed lower-SeqNo announcements arriving on the same
	// uplink.
	SeqNo uint64

	// Source indicates how this claim was learned. The trust
	// hierarchy (direct > hop_ack > announcement) is preserved
	// from RouteEntry.Source and still governs cap admission and
	// Lookup ranking. RouteSourceLocal is synthetic and is NEVER
	// stored as an UplinkClaim — the self-route is injected by
	// Table.Lookup / Table.Snapshot at read time.
	Source RouteSource

	// UpdatedAt records the last wall-clock instant at which this
	// claim was mutated (admitted, refreshed, withdrawn). Used by
	// diagnostics and reserved for Phase 2 health scoring; A1
	// sets it to `now` on every mutation.
	UpdatedAt time.Time

	// ExpiresAt is the absolute time when this claim expires.
	// Derived from RemainingTTL at insertion. Zero for direct
	// routes (their lifetime is socket-bound, not TTL-bound) and
	// for the synthetic local route.
	ExpiresAt time.Time

	// Withdrawn marks the claim as a tombstone (origin explicitly
	// withdrew, or the uplink reported HopsInfinity on the wire,
	// or a direct uplink disconnected). Tombstones live alongside
	// the K-counted live claims in the same bucket and are
	// reclaimed by TickTTL after ExpiresAt elapses; they guard
	// against resurrection from delayed lower-SeqNo announcements.
	Withdrawn bool

	// AttestedSig is an Ed25519 signature attesting to this
	// particular (Identity, Hops, SeqNo) claim. Empty unless the
	// mesh_attested_links_v1 capability is negotiated; reserved
	// for Phase 4 (compact wire frames + signed announcements).
	// Allocated lazily — nil on every A0/A1-shipped claim.
	AttestedSig []byte

	// Extra carries opaque JSON fields from the wire that this node
	// does not understand. Preserved across storage operations so
	// that re-announced routes carry forward-compatible extensions
	// unchanged. RouteEntry.Extra documents the wire-protocol
	// contract (see types.go) — UplinkClaim mirrors the field
	// to keep that contract intact through the per-uplink storage
	// shape. Hop-promotion paths (route_store_mutation.go) preserve
	// Extra when in-place-replacing a claim of equal or lower
	// trust; relay-side tests pin this round-trip via
	// TestConfirmRouteViaHopAck_PreservesExtra and similar.
	//
	// Nil for locally originated direct routes and the synthetic
	// self-route; allocated only when the ingest layer received
	// non-nil Extra from the wire.
	Extra json.RawMessage

	// LastIngressOrigin records the wire-frame Origin we observed
	// on the most recent ingest for this (Identity, Uplink) claim.
	// Storage no longer KEYS on Origin (Phase A collapsed the dedup
	// to per-(Identity, Uplink) — see the type-level doc-comment),
	// but Origin is retained here as a sticky marker so ApplyUpdate
	// can detect cross-lineage shifts on the same uplink during
	// mixed-version interop with pre-A1 senders.
	//
	// Pre-A1 SeqNos were per-Origin (independent counters per
	// lineage), and pre-A1 BuildAnnounceSnapshot's Stage 4 picks one
	// per-Identity winner per cycle. A pre-A1 sender whose winner
	// switches from Origin=A (SeqNo=10, hops=3) to Origin=B
	// (SeqNo=5, hops=2) is a legitimate lineage shift, but to a
	// post-A1 receiver that compares SeqNo per-uplink the second
	// frame looks like a stale-SeqNo replay. ApplyUpdate's
	// stale-SeqNo branch uses LastIngressOrigin to detect this
	// case: `entry.Origin != old.LastIngressOrigin && incoming
	// strictly better` accepts the new lineage and resets the
	// per-uplink SeqNo namespace. Post-A1 senders always emit
	// Origin = localOrigin (constant across cycles) so this branch
	// never fires for A1-to-A1 traffic.
	//
	// Set on every ingest by toUplinkClaim. For own-direct claims
	// (AddDirectPeer → AdmitDirectPeer) it is set to localOrigin —
	// the value the sender-originated wire emit would carry, so
	// any future ApplyUpdate on the same uplink that arrives with
	// a foreign Origin can take the cross-Origin branch. Empty
	// string only on claims constructed by tests that did not pass
	// through toUplinkClaim / AdmitDirectPeer; treated by
	// ApplyUpdate as "no prior lineage observed" (cross-Origin
	// branch is gated on both old.LastIngressOrigin and
	// entry.Origin being non-empty).
	LastIngressOrigin PeerIdentity

	// SeenOriginSeqs records the high-water SeqNo we have observed
	// per wire-frame Origin on this (Identity, Uplink) claim.
	// Complements LastIngressOrigin: LastIngressOrigin names the
	// currently active lineage; SeenOriginSeqs remembers EVERY
	// lineage we have ever accepted, so a delayed in-flight frame
	// from an Origin whose active turn has passed cannot slip
	// through as a "fresh" cross-Origin update.
	//
	// Concretely, pre-A1 senders use independent per-Origin SeqNo
	// counters. A winner shift A10 → B5 is a legitimate lineage
	// transition (the round-5k cross-Origin lineage branch admits
	// it under "improves"). A subsequent delayed in-flight A9 from
	// BEFORE the shift has SeqNo > current B5 and would naively
	// fall into ApplyUpdate's `SeqNo > old.SeqNo` branch and
	// overwrite the legitimate B lineage with stale A state. The
	// cross-Origin stale-replay guard in ApplyUpdate rejects it
	// because SeenOriginSeqs[A] = 10 ≥ 9.
	//
	// Lazy-allocated: nil on synthetic test fixtures that build
	// UplinkClaim directly without going through ApplyUpdate /
	// AdmitDirectPeer / WithdrawTriple. Production paths populate
	// it via mergeOriginObservation on every accepted ingest —
	// toUplinkClaim itself leaves the field nil because the
	// rejected-path allocations would be wasted; the accept-path
	// helpers do the merge against the prior claim's map just
	// before storage commit. Post-A1 senders always emit
	// Origin = localOrigin (constant across cycles) so the map
	// stays single-entry in steady state and the allocation cost
	// is paid only during mixed-version interop.
	//
	// Invariant: SeenOriginSeqs[LastIngressOrigin] >= SeqNo
	// whenever both are non-empty. The active lineage's high-
	// water is always tracked.
	SeenOriginSeqs map[PeerIdentity]uint64
}

// mergeOriginObservation records that we observed `origin` at
// `seqNo` on the (Identity, Uplink) claim that owns `prior`. Returns
// the updated map (re-using the prior allocation when present,
// allocating only on first observation). No-op when origin is empty
// — defence-in-depth against synthetic test fixtures that build
// RouteEntry without Origin.
//
// Used by every accept path in route_store_mutation.go to keep
// UplinkClaim.SeenOriginSeqs consistent with the cross-Origin
// stale-replay guard's expectations. See SeenOriginSeqs field
// doc-comment for the mixed-version rationale.
func mergeOriginObservation(prior map[PeerIdentity]uint64, origin PeerIdentity, seqNo uint64) map[PeerIdentity]uint64 {
	if origin == "" {
		return prior
	}
	seen := prior
	if seen == nil {
		seen = make(map[PeerIdentity]uint64, 2)
	}
	if cur, ok := seen[origin]; !ok || seqNo > cur {
		seen[origin] = seqNo
	}
	return seen
}

// mergeOriginObservationCopy is the copy-on-write sibling of
// mergeOriginObservation. It produces an INDEPENDENT map: the
// returned map never aliases `prior`, so callers holding `prior`
// observe no mutation regardless of the merge outcome.
//
// Used by the tombstone-promotion paths in route_store_mutation.go
// (newer-SeqNo, equal-SeqNo cross-Origin, and stale-SeqNo cross-
// Origin) where the saved tombstone is restored on AdmitNew
// rejection. Without the copy, the merge into the candidate
// `incoming.SeenOriginSeqs` would aliasing-mutate the saved
// tombstone's map, leaving it remembering an Origin/SeqNo that
// was never actually accepted; a subsequent legitimate retry at
// the same SeqNo would then trip the per-Origin high-water guard
// in ApplyUpdate and reject incorrectly.
//
// The copy cost is one allocation per promotion attempt — small
// against the per-call AdmitNew bucket scan, and only paid on the
// cross-Origin/tombstone fast path that is itself rare in
// post-A1 steady state.
func mergeOriginObservationCopy(prior map[PeerIdentity]uint64, origin PeerIdentity, seqNo uint64) map[PeerIdentity]uint64 {
	// Allocate a fresh map sized for the prior + the new entry (if
	// origin is non-empty). When prior is nil the resulting map is
	// either empty (origin==empty) or single-entry.
	out := make(map[PeerIdentity]uint64, len(prior)+1)
	for k, v := range prior {
		out[k] = v
	}
	if origin == "" {
		return out
	}
	if cur, ok := out[origin]; !ok || seqNo > cur {
		out[origin] = seqNo
	}
	return out
}

var (
	// ErrEmptyUplink is returned by UplinkClaim.Validate when
	// Uplink is the zero value. An uplink is the direct peer who
	// told us this claim; storing a claim with empty uplink would
	// make split-horizon and CompactExpired-driven exposure
	// detection unreliable.
	ErrEmptyUplink = errors.New("routing: UplinkClaim.Uplink must not be empty")

	// ErrInvalidClaimHops is returned by UplinkClaim.Validate when
	// Hops is outside the live range (1..HopsInfinity for non-
	// withdrawn, 0..HopsInfinity for withdrawn — withdrawn
	// tombstones keep the observed-at-withdrawal-time Hops, which
	// can be 0 for direct routes whose uplink disconnected).
	ErrInvalidClaimHops = errors.New("routing: UplinkClaim.Hops must be between 0 and HopsInfinity (16)")
)

// IsWithdrawn reports whether the claim is a tombstone. After the
// storage swap this is a dedicated bool rather than a Hops-sentinel
// check — see the Withdrawn field doc-comment for why.
func (c UplinkClaim) IsWithdrawn() bool {
	return c.Withdrawn
}

// IsExpired reports whether the claim's TTL has elapsed against
// `now`. Inclusive: a claim whose ExpiresAt equals now is
// considered expired (keeps ttl_seconds=0 and expired=true
// consistent in RPC output, matching RouteEntry.IsExpired).
//
// Zero ExpiresAt is treated as "never expires" — direct routes
// and the synthetic local route both use that convention.
func (c UplinkClaim) IsExpired(now time.Time) bool {
	return !c.ExpiresAt.IsZero() && !now.Before(c.ExpiresAt)
}

// Validate checks structural invariants of the claim. Returns an
// error when the claim is malformed and must not be inserted into
// the store.
//
// Source-specific rules:
//
//   - RouteSourceDirect: Hops must be 1, Uplink IS the destination
//     identity (the caller must use the bucket key as Uplink for
//     direct routes — admission helpers do not re-check this
//     because the bucket structure makes the violation impossible
//     by construction).
//   - RouteSourceLocal: not allowed in storage at all. The
//     synthetic self-route is injected at read time by Table.Lookup
//     and Table.Snapshot, never persisted as an UplinkClaim. The
//     check is structural (Hops=0) so the rule is enforced even
//     for callers that bypass the public Table API.
//   - Withdrawn: Hops can be 0..HopsInfinity. Tombstones generated
//     by InvalidateAllVia keep the just-withdrawn entry's Hops; a
//     tombstone created by WithdrawTriple with no prior entry uses
//     HopsInfinity.
//   - Live (not Withdrawn): Hops must be in 1..HopsInfinity-1
//     (1..15). HopsInfinity for a non-Withdrawn claim would be
//     ambiguous against the wire sentinel and is rejected.
func (c UplinkClaim) Validate() error {
	if string(c.Uplink) == "" {
		return ErrEmptyUplink
	}
	if c.Source == RouteSourceLocal {
		return ErrLocalSourceReserved
	}
	if c.Withdrawn {
		if c.Hops > HopsInfinity {
			return ErrInvalidClaimHops
		}
		return nil
	}
	if c.Source == RouteSourceDirect {
		if c.Hops != 1 {
			return ErrDirectHopsMust1
		}
		return nil
	}
	if c.Hops < 1 || c.Hops >= HopsInfinity {
		return ErrInvalidClaimHops
	}
	return nil
}

// toUplinkClaim converts an ingest-side RouteEntry into the
// storage shape. The caller must have already performed the
// anti-spoof check on entry.Origin (Table.UpdateRoute rejects
// RouteSourceDirect with foreign Origin upstream); Origin is
// not preserved as a separate field — its semantic content (who
// originally announced this lineage) is dropped because the
// per-(Identity, Uplink) dedup key no longer needs it.
// Wire-emit code re-emits Origin = localOrigin (legacy sender-
// originated semantic, which keeps pre-A1 receiver withdrawal
// anti-spoof working — see route_store_lookup.go's
// AnnounceProjectionFor for the contract).
//
// Extra (forward-compatible wire fields carried via
// RouteEntry.Extra) IS preserved through storage so that
// transit re-announce paths and hop_ack promotions can carry
// unknown wire extensions unchanged. This honours the
// documented public RouteEntry.Extra contract and the
// frame.go-level wire-protocol forward-compat promise.
//
// `now` provides the UpdatedAt timestamp; ExpiresAt is taken
// from the incoming RouteEntry (the caller defaults zero via
// defaultTTL before reaching this function).
func toUplinkClaim(entry RouteEntry, now time.Time) UplinkClaim {
	return UplinkClaim{
		Uplink:            entry.NextHop,
		Hops:              uint8(entry.Hops),
		SeqNo:             entry.SeqNo,
		Source:            entry.Source,
		UpdatedAt:         now,
		ExpiresAt:         entry.ExpiresAt,
		Withdrawn:         entry.Hops >= HopsInfinity,
		Extra:             entry.Extra,
		LastIngressOrigin: entry.Origin,
	}
}

// toRouteEntry synthesises a boundary RouteEntry from a stored
// UplinkClaim, its destination identity, and the owning
// routeStore's localOrigin. The Origin field on the result is
// localOrigin (sender-originated semantic). Pre-A1 receivers
// expect Origin == sender for the wire-frame anti-spoof, and
// post-A1 receivers drop Origin on ingest anyway, so this is
// the only value that survives both rollouts cleanly.
//
// When localOrigin is empty (e.g. a unit-test Table created
// without WithLocalOrigin), the synthesised Origin falls back to
// identity. This keeps test fixtures that build tables without
// configuring localOrigin from producing structurally-invalid
// Origin="" entries that would later fail RouteEntry.Validate.
//
// Used by routeStore lookup helpers (LookupActive,
// AnnounceableFor, AnnounceProjectionFor) and by SnapshotRoutes
// to keep the Table boundary API working with RouteEntry while
// the inner storage is per-uplink.
//
// Withdrawn tombstones carry Hops=HopsInfinity on the synthesised
// RouteEntry regardless of the claim's stored Hops, so callers
// reading the boundary value cannot tell the difference from a
// legacy tombstone — RouteEntry.IsWithdrawn keeps working.
//
// Extra is carried back so that round-tripping a claim through
// storage (UpdateRoute → Lookup) preserves the wire's
// forward-compatible fields.
func toRouteEntry(identity, localOrigin PeerIdentity, claim UplinkClaim) RouteEntry {
	hops := int(claim.Hops)
	if claim.Withdrawn {
		hops = HopsInfinity
	}
	origin := localOrigin
	if origin == "" {
		origin = identity
	}
	return RouteEntry{
		Identity:  identity,
		Origin:    origin,
		NextHop:   claim.Uplink,
		Hops:      hops,
		SeqNo:     claim.SeqNo,
		Source:    claim.Source,
		ExpiresAt: claim.ExpiresAt,
		Extra:     claim.Extra,
	}
}
