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
// Storage keeps no Origin. Every supported sender emits
// Origin == itself, and the node receive path refuses any entry whose
// Origin is not the sending peer (route_announce_v3 and route_query
// ingest substitute the sender), so by the time an announcement
// reaches storage its Origin is the uplink and carries nothing the
// (Identity, Uplink) key does not; Table.UpdateRoute additionally
// refuses a Direct entry with a foreign Origin. The Origin is then
// dropped, and SeqNo inside a slot is ordered strictly, with no
// Origin in the comparison.
//
// The emit side synthesises Origin = localOrigin (with fallback to
// Identity when localOrigin is empty — test fixtures without
// WithLocalOrigin) for live and withdrawal frames alike. That keeps
// the wire contract "Origin == sender" that receivers enforce: an
// entry, live or withdrawal, is accepted only when its Origin is the
// sending peer.
//
// The snapshot RPC schema rename (cap_admission → uplink_admission)
// and the MaxNextHopsPerOrigin deprecation are deferred; the knob
// and JSON key stay stable for operator-facing tooling.
type UplinkClaim struct {
	// Uplink is the direct peer who told us about this claim — was
	// previously RouteEntry.NextHop. Every entry in routeStore is
	// keyed by (destination Identity, Uplink).
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

	// AttestedSig is an Ed25519 signature by the destination identity
	// attesting to its (Identity, Extra) claim — produced via
	// protocol.RouteAnnounceV3Entry.CanonicalSigningBytes. Per-emitter
	// wire fields (hops, epoch, seq_no) are deliberately excluded from
	// the signed payload so transit re-emit does not invalidate the
	// signature (see CanonicalSigningBytes doc for the full rationale).
	// Shipped on the wire via route_announce_v3.entry.sig under
	// mesh_attested_links_v1; receivers verify when the cap is
	// negotiated and the destination pubkey is known. Allocated lazily
	// — nil for legacy v1/v2 ingest, for unsigned v3 ingest, and for
	// own-direct routes whose ingest path does not carry a signature.
	AttestedSig []byte

	// AttestedSigVerified records whether the receiver successfully
	// ed25519.Verify'd AttestedSig against the destination identity's
	// public key at ingest time (Phase 4 13.2-C). Three states are
	// collapsed onto two booleans paired with AttestedSig:
	//
	//   - AttestedSig != nil && AttestedSigVerified == true:
	//     signature present AND cryptographically verified by us.
	//     Highest trust tier — CompositeScore applies the signed
	//     bonus.
	//   - AttestedSig != nil && AttestedSigVerified == false:
	//     signature present BUT we could not verify (the destination
	//     identity's pubkey was not in our knowledge store at
	//     ingest). The bytes are still forwarded verbatim so a peer
	//     with the pubkey CAN verify; locally treated as unsigned
	//     for ranking.
	//   - AttestedSig == nil: no signature. Tier-2 unsigned path.
	//     Same ranking as the unverified case.
	//
	// Present-but-invalid signatures never reach storage — the
	// receive-side verifier drops them in verifyRouteAnnounceV3Sigs
	// before applyAnnounceEntries runs.
	AttestedSigVerified bool

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
//     tombstone created by WithdrawUplinkClaim with no prior entry uses
//     HopsInfinity.
//   - Live (not Withdrawn): Hops must be in 1..HopsInfinity-1
//     (1..15). HopsInfinity for a non-Withdrawn claim would be
//     ambiguous against the wire sentinel and is rejected.
func (c UplinkClaim) Validate() error {
	if c.Uplink.IsZero() {
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
// dropped here because the per-(Identity, Uplink) key already
// names the only sender whose Origin a valid frame can carry.
// Wire-emit code synthesises Origin = localOrigin (see
// route_store_lookup.go's AnnounceProjectionFor for the
// contract).
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
		Uplink:    entry.NextHop,
		Hops:      uint8(entry.Hops),
		SeqNo:     entry.SeqNo,
		Source:    entry.Source,
		UpdatedAt: now,
		ExpiresAt: entry.ExpiresAt,
		Withdrawn: entry.Hops >= HopsInfinity,
		Extra:     entry.Extra,
		// Phase 4 13.2-A: thread the attested-links signature opaquely
		// through ingest into storage. Sign-at-emit (own-origin) and
		// verify-on-receive land in 13.2-B; today the bytes are
		// stashed verbatim so a re-announce can forward them
		// unchanged. Nil for legacy v1/v2 ingest (no sig on the wire)
		// and for own-direct routes whose ingest path does not carry
		// a signature today.
		AttestedSig:         entry.AttestedSig,
		AttestedSigVerified: entry.AttestedSigVerified,
	}
}

// toRouteEntry synthesises a boundary RouteEntry from a stored
// UplinkClaim, its destination identity, and the owning
// routeStore's localOrigin. The Origin field on the result is
// localOrigin (sender-originated semantic): when the value goes
// out on the wire, receivers accept the entry only if its Origin
// is the sending peer.
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
	if origin.IsZero() {
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
		// Phase 4 13.2-A: return the stored attested-links signature on
		// the boundary so the projection layer (BuildAnnounceSnapshot /
		// AnnounceProjectionFor) and the wire emit (buildRouteAnnounceV3Frame)
		// can forward it unchanged.
		AttestedSig:         claim.AttestedSig,
		AttestedSigVerified: claim.AttestedSigVerified,
	}
}
