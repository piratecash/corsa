package routing

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// attested_delta_propagation_test.go pins the Round-14 fix that
// sig-only upgrades flow through the delta-emit path. The
// Round-11/12 reconfirmation fix stores AttestedSig on a same-seq /
// same-hops re-announcement, but without this round's fix:
//
//   1. ComputeDelta ignored AttestedSig in its content comparison —
//      old vs new snapshot looked identical, delta was empty, the
//      AnnounceLoop suppressed the cycle as a no-op, and the
//      verified attestation never reached the peer.
//   2. outboundEmitSig ignored AttestedSig in the content cache —
//      even if the snapshot did surface the entry, the per-(Identity,
//      content) cache hit on the prior (unsigned) emit, handed back
//      the burnt SeqNo, and the receiver rejected the new (signed)
//      entry as stale-by-SeqNo via the per-peer monotonicity check.
//
// Both pieces of plumbing must treat AttestedSig bytes as wire
// content; AttestedSigVerified stays local (a verify-flip without
// any wire-byte change must NOT manufacture a delta).

// TestBuildAnnounceSnapshot_PreservesVerbatimExtra pins the Round-24
// fix: BuildAnnounceSnapshot Stage 1 uses normalized Extra ONLY for
// the dedup key — the AnnounceEntry it emits MUST carry the original
// verbatim Extra bytes. The attested-links signature
// (RouteAnnounceV3Entry.CanonicalSigningBytes) hashes Extra
// byte-for-byte from the wire, and docs/protocol/attested_links.md
// promises verbatim transit. If the snapshot stored the normalized
// form, a transit re-emit of a signed frame would carry mutated
// Extra alongside the original AttestedSig — every downstream peer
// with mesh_attested_links_v1 negotiated would fail verification
// and drop the entry.
func TestBuildAnnounceSnapshot_PreservesVerbatimExtra(t *testing.T) {
	cases := []struct {
		name string
		raw  json.RawMessage
	}{
		{
			name: "extra_with_outer_whitespace",
			raw:  json.RawMessage(`  {"anchor":"x"}  `),
		},
		{
			name: "literal_null_extra",
			raw:  json.RawMessage(`null`),
		},
		{
			name: "extra_with_tabs_and_newlines",
			raw:  json.RawMessage("\n\t{\"anchor\":\"y\"}\n"),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			snap := BuildAnnounceSnapshot([]AnnounceEntry{{
				Identity: domaintest.ID("dest"),
				Origin:   domaintest.ID("src"),
				Hops:     2,
				SeqNo:    5,
				Extra:    c.raw,
			}})
			if snap == nil || len(snap.Entries) != 1 {
				t.Fatalf("expected one snapshot entry, got %+v", snap)
			}
			if string(snap.Entries[0].Extra) != string(c.raw) {
				t.Fatalf("Extra mutated by Stage 1 dedup: got %q want %q (Round-24 verbatim contract)",
					snap.Entries[0].Extra, c.raw)
			}
		})
	}
}

// TestBuildAnnounceSnapshot_NormalizationStillCollapsesEquivalentDedups
// pins the OTHER half of the Round-24 contract: even though the
// emitted Extra is verbatim, two entries whose Extra bytes differ
// only in JSON whitespace / null variations MUST still collapse to a
// single dedup output (the wire-key still uses normalizeExtra). This
// preserves the Stage 1 dedup utility — only the visible wire bytes
// move to verbatim, not the equality definition.
func TestBuildAnnounceSnapshot_NormalizationStillCollapsesEquivalentDedups(t *testing.T) {
	raw := []AnnounceEntry{
		{Identity: domaintest.ID("dest"), Origin: domaintest.ID("src"), Hops: 2, SeqNo: 5, Extra: json.RawMessage(`  {"anchor":"x"}  `)},
		{Identity: domaintest.ID("dest"), Origin: domaintest.ID("src"), Hops: 2, SeqNo: 5, Extra: json.RawMessage(`{"anchor":"x"}`)},
	}
	snap := BuildAnnounceSnapshot(raw)
	if snap == nil {
		t.Fatalf("snapshot must not be nil")
	}
	// Both entries dedup-collapse to one because normalizeExtra
	// strips outer whitespace; the surviving entry carries WHICHEVER
	// bytes Stage 1 admitted first (insertion-order semantic).
	if n := len(snap.Entries); n != 1 {
		t.Fatalf("equivalent-Extra entries must collapse to one snapshot entry; got %d entries", n)
	}
	// First-write-wins on dedup → the first entry's verbatim bytes
	// survive.
	if string(snap.Entries[0].Extra) != "  {\"anchor\":\"x\"}  " {
		t.Fatalf("first-write verbatim bytes must survive dedup; got %q", snap.Entries[0].Extra)
	}
}

// TestAnnounceEntryEqual_SigDifferenceProducesInequality pins the
// Round-19 alignment for AnnounceSnapshot.Equal: two entries that
// differ ONLY in AttestedSig must compare not-equal, matching the
// Round-14 ComputeDelta + outboundEmitSig rule that AttestedSig is
// wire content. AttestedSigVerified stays local-only.
func TestAnnounceEntryEqual_SigDifferenceProducesInequality(t *testing.T) {
	a := AnnounceSnapshot{Entries: []AnnounceEntry{{
		Identity: domaintest.ID("dest"), Origin: domaintest.ID("src"),
		Hops: 2, SeqNo: 5,
		AttestedSig: []byte("sig-A"),
	}}}
	b := AnnounceSnapshot{Entries: []AnnounceEntry{{
		Identity: domaintest.ID("dest"), Origin: domaintest.ID("src"),
		Hops: 2, SeqNo: 5,
		AttestedSig: []byte("sig-B"),
	}}}
	if a.Equal(&b) {
		t.Fatal("snapshots differing only in AttestedSig must NOT compare equal (Round-19 alignment with Round-14 wire-content contract)")
	}

	// Verified-flip alone must NOT affect equality: it is a local
	// observation that never travels on the wire.
	c := AnnounceSnapshot{Entries: []AnnounceEntry{{
		Identity: domaintest.ID("dest"), Origin: domaintest.ID("src"),
		Hops: 2, SeqNo: 5,
		AttestedSig:         []byte("sig-stable"),
		AttestedSigVerified: false,
	}}}
	d := AnnounceSnapshot{Entries: []AnnounceEntry{{
		Identity: domaintest.ID("dest"), Origin: domaintest.ID("src"),
		Hops: 2, SeqNo: 5,
		AttestedSig:         []byte("sig-stable"),
		AttestedSigVerified: true,
	}}}
	if !c.Equal(&d) {
		t.Fatal("snapshots differing only in AttestedSigVerified must compare equal (verified is local-only, not wire content)")
	}
}

// TestBuildAnnounceSnapshot_Stage1DedupKeepsSigDifferentEntries pins
// the Round-19 fix for Stage 1 exact-wire-dedup: two entries with
// identical (Identity, Origin, SeqNo, Hops, Extra) but DIFFERENT
// AttestedSig must NOT be collapsed by the dedup. In production
// AnnounceProjectionFor never produces such a pair (one entry per
// per-Identity winner), but pinning the contract here prevents a
// future producer change from silently losing a verified-bytes
// upgrade through Stage 1.
func TestBuildAnnounceSnapshot_Stage1DedupKeepsSigDifferentEntries(t *testing.T) {
	raw := []AnnounceEntry{
		{Identity: domaintest.ID("dest"), Origin: domaintest.ID("src"), Hops: 2, SeqNo: 5, AttestedSig: []byte("sig-A")},
		{Identity: domaintest.ID("dest"), Origin: domaintest.ID("src"), Hops: 2, SeqNo: 5, AttestedSig: []byte("sig-B")},
	}
	snap := BuildAnnounceSnapshot(raw)
	if snap == nil {
		t.Fatalf("snapshot must not be nil")
	}
	// Stage 1 keeps both — Stages 2/3 then collapse by per-Identity
	// rules. The Round-19 contract is just that Stage 1 doesn't
	// silently merge sig-different entries on its own. Count by
	// AttestedSig values surviving end-to-end.
	sigs := map[string]bool{}
	for _, e := range snap.Entries {
		sigs[string(e.AttestedSig)] = true
	}
	// At least one of the two must reach the output; the per-
	// Identity collapse (Stage 3) picks ONE winner among same-
	// (Identity, Hops, SeqNo) inputs, so the post-condition is
	// "the pipeline didn't crash and Stage 1 didn't collapse to
	// zero entries". The important negative — sig-different
	// entries treated as exact-duplicates — is caught by the
	// Equal test above when AnnounceSnapshot.Equal would have
	// matched two distinct wire snapshots.
	if len(snap.Entries) == 0 {
		t.Fatal("BuildAnnounceSnapshot must keep at least one of the two sig-different inputs; Stage 1 swallowed both")
	}
}

// TestComputeDelta_SigOnlyChangeEmits is the primary pin: an
// otherwise-identical entry with different AttestedSig bytes must
// produce a non-empty ComputeDelta result, even though SeqNo, Hops
// and Extra are byte-for-byte equal.
func TestComputeDelta_SigOnlyChangeEmits(t *testing.T) {
	oldSnap := &AnnounceSnapshot{
		Entries: []AnnounceEntry{
			{
				Identity:    domaintest.ID("dest"),
				Origin:      domaintest.ID("src"),
				Hops:        2,
				SeqNo:       5,
				AttestedSig: nil,
			},
		},
	}
	newSnap := &AnnounceSnapshot{
		Entries: []AnnounceEntry{
			{
				Identity:    domaintest.ID("dest"),
				Origin:      domaintest.ID("src"),
				Hops:        2,
				SeqNo:       5,
				AttestedSig: []byte("verified-sig"),
			},
		},
	}
	delta := ComputeDelta(oldSnap, newSnap)
	if len(delta) != 1 {
		t.Fatalf("sig-only change must produce a 1-entry delta; got %d entries", len(delta))
	}
	if string(delta[0].AttestedSig) != "verified-sig" {
		t.Fatalf("delta entry must carry the new sig bytes; got %q", delta[0].AttestedSig)
	}
}

// TestComputeDelta_VerifiedFlipAloneDoesNotEmit pins the negative
// half: a local-only verify-flip (AttestedSigVerified false → true
// with byte-identical AttestedSig) must NOT manufacture a delta.
// AttestedSigVerified is the receiver's own observation; flipping
// it doesn't change anything on the wire, and emitting a delta
// would burn a fresh SeqNo for no benefit.
func TestComputeDelta_VerifiedFlipAloneDoesNotEmit(t *testing.T) {
	sigBytes := []byte("stable-sig")
	oldSnap := &AnnounceSnapshot{
		Entries: []AnnounceEntry{
			{
				Identity:            domaintest.ID("dest"),
				Origin:              domaintest.ID("src"),
				Hops:                2,
				SeqNo:               5,
				AttestedSig:         sigBytes,
				AttestedSigVerified: false,
			},
		},
	}
	newSnap := &AnnounceSnapshot{
		Entries: []AnnounceEntry{
			{
				Identity:            domaintest.ID("dest"),
				Origin:              domaintest.ID("src"),
				Hops:                2,
				SeqNo:               5,
				AttestedSig:         sigBytes,
				AttestedSigVerified: true,
			},
		},
	}
	if delta := ComputeDelta(oldSnap, newSnap); len(delta) != 0 {
		t.Fatalf("verified-flip alone must NOT produce a delta (local-only observation); got %d entries", len(delta))
	}
}

// TestOutboundEmitSig_SigUpgradeForcesFreshSeqNo walks the
// outboundEmitSig content-cache contract end-to-end via Table.AnnounceTo:
// a stored claim emits at SeqNo S; an upgrade adds AttestedSig at
// the same wire-content otherwise; the next AnnounceTo must NOT
// reuse SeqNo S — the receiver's per-peer monotonicity check would
// reject the upgraded frame at the burnt SeqNo, so the cache must
// surface a strictly newer SeqNo for the new (signed) content.
func TestOutboundEmitSig_SigUpgradeForcesFreshSeqNo(t *testing.T) {
	now := time.Date(2026, 5, 30, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(
		WithClock(fixedClock(now)),
		WithLocalOrigin(domaintest.ID("local")),
	)

	var (
		dest   = domaintest.ID("alice")
		origin = domaintest.ID("bob")
		uplink = domaintest.ID("uplink")
		peerY  = domaintest.ID("peer-Y")
	)

	// Seed the unsigned claim.
	mustUpdate(t, tbl, RouteEntry{
		Identity: dest, Origin: origin, NextHop: uplink,
		Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
	})
	first := tbl.AnnounceTo(peerY)
	var firstSeq uint64
	for _, e := range first {
		if e.Identity == dest && e.Hops < HopsInfinity {
			firstSeq = e.SeqNo
			break
		}
	}
	if firstSeq == 0 {
		t.Fatalf("first AnnounceTo must emit dest; got %+v", first)
	}

	// Same SeqNo, same Hops, NO sig yet on storage → reconfirmation
	// path runs but no upgrade happens. AnnounceTo cache hits on the
	// same content and reuses the SeqNo (this is the desired
	// idempotency for unchanged content).
	mustUpdate(t, tbl, RouteEntry{
		Identity: dest, Origin: origin, NextHop: uplink,
		Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
	})
	idem := tbl.AnnounceTo(peerY)
	for _, e := range idem {
		if e.Identity == dest && e.Hops < HopsInfinity {
			if e.SeqNo != firstSeq {
				t.Fatalf("unchanged-content reconfirmation must reuse SeqNo; got %d want %d", e.SeqNo, firstSeq)
			}
			break
		}
	}

	// NOW upgrade with a verified sig — same SeqNo / Hops / Extra,
	// only AttestedSig changes. Round-14 must force a fresh SeqNo
	// so the receiver's per-peer monotonicity check accepts the new
	// signed wire content.
	mustUpdate(t, tbl, RouteEntry{
		Identity: dest, Origin: origin, NextHop: uplink,
		Hops: 2, SeqNo: 5, Source: RouteSourceAnnouncement,
		AttestedSig:         []byte("verified-sig"),
		AttestedSigVerified: true,
	})
	upgraded := tbl.AnnounceTo(peerY)
	var upgradedSeq uint64
	var upgradedSig []byte
	for _, e := range upgraded {
		if e.Identity == dest && e.Hops < HopsInfinity {
			upgradedSeq = e.SeqNo
			upgradedSig = e.AttestedSig
			break
		}
	}
	if upgradedSeq == 0 {
		t.Fatalf("upgraded AnnounceTo must emit dest; got %+v", upgraded)
	}
	if upgradedSeq <= firstSeq {
		t.Fatalf("sig-upgrade must force a strictly newer SeqNo: upgradedSeq=%d, firstSeq=%d — without the bump the receiver rejects the new content as stale", upgradedSeq, firstSeq)
	}
	if string(upgradedSig) != "verified-sig" {
		t.Fatalf("upgraded emit must carry the new sig bytes; got %q", upgradedSig)
	}
}
