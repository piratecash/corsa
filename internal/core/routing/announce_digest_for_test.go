package routing

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// announce_digest_for_test.go covers the Phase 3 review P1 fix: the
// reconnect digest must be the sender's OUTBOUND announce projection to
// the peer (Table.AnnounceDigestFor), not the routes the sender learned
// THROUGH the peer (SyncDigestFor). Only the outbound projection lines up
// with what the peer stored as its via-(this node) view, so only then can
// a reconnect digest actually match.
//
// Fixed-clock convention (2026-05-23) matches the rest of the routing
// suite; upsertClaim/UpdateRoute stamp ExpiresAt at real-now + 1h, which
// is in the future relative to the fixed clock, so claims stay live.

// TestAnnounceDigestFor_MatchesReceiverViaSet_ACviaB is the headline e2e:
// A learns C through B. On reconnect B emits AnnounceDigestFor(A); A
// compares it against SyncDigestFor(B). The two MUST be equal, otherwise
// the reconnect optimisation can never fire in this (entirely ordinary)
// topology.
func TestAnnounceDigestFor_MatchesReceiverViaSet_ACviaB(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)

	// Node B is directly connected to C.
	tblB := NewTable(WithLocalOrigin(domaintest.ID("id-B")), WithClock(fixedClock(now)))
	if _, err := tblB.AddDirectPeer(domaintest.ID("id-C")); err != nil {
		t.Fatalf("B.AddDirectPeer(C): %v", err)
	}

	// B announces to A. AnnounceTo records B's per-peer wire SeqNos in
	// outboundPeerMax — the basis AnnounceDigestFor reproduces.
	entries := tblB.AnnounceTo(domaintest.ID("id-A"))
	if len(entries) == 0 {
		t.Fatal("B.AnnounceTo(A) produced no entries; expected a route to C")
	}

	// Node A ingests B's announce: each entry becomes a claim via B with
	// the exact wire SeqNo B sent (receiver adds +1 hop, mirroring the
	// real receive path).
	tblA := NewTable(WithLocalOrigin(domaintest.ID("id-A")), WithClock(fixedClock(now)))
	for _, e := range entries {
		if _, err := tblA.UpdateRoute(RouteEntry{
			Identity:  e.Identity,
			Origin:    e.Origin,
			NextHop:   domaintest.ID("id-B"),
			Hops:      e.Hops + 1,
			SeqNo:     e.SeqNo,
			Source:    RouteSourceAnnouncement,
			ExpiresAt: time.Now().Add(time.Hour),
		}); err != nil {
			t.Fatalf("A.UpdateRoute(%q via B): %v", e.Identity, err)
		}
	}

	bOutboundToA, bCount := tblB.AnnounceDigestFor(domaintest.ID("id-A"))
	aViaB, aCount := tblA.SyncDigestFor(domaintest.ID("id-B"))

	if bCount == 0 {
		t.Fatal("B.AnnounceDigestFor(A) is empty; expected the route to C")
	}
	if bOutboundToA != aViaB {
		t.Fatalf("digest mismatch: B.AnnounceDigestFor(A)=%s (count %d) != A.SyncDigestFor(B)=%s (count %d); reconnect optimisation cannot fire",
			bOutboundToA, bCount, aViaB, aCount)
	}

	// Regression guard: the PRE-FIX sender computation (SyncDigestFor on
	// the sender = routes B knows THROUGH A) does NOT equal A's via-B set.
	// B learned nothing through A, so its via-A digest is the empty-input
	// digest while A's via-B digest covers C — demonstrating exactly why
	// the old comparison almost never matched.
	bViaA, _ := tblB.SyncDigestFor(domaintest.ID("id-A"))
	if bViaA == aViaB {
		t.Fatal("pre-fix via-peer digest unexpectedly matched; the regression guard is ineffective")
	}
}

// TestAnnounceDigestFor_EmptyWhenNothingAnnounced — a node that has never
// announced to a peer produces the stable empty-input digest, so a
// reconnect with no prior outbound view degrades to a full sync rather
// than a spurious match.
func TestAnnounceDigestFor_EmptyWhenNothingAnnounced(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tblB := NewTable(WithLocalOrigin(domaintest.ID("id-B")), WithClock(fixedClock(now)))
	// A route exists but was never announced to A (no AnnounceTo call), so
	// outboundPeerMax has no (id-C, id-A) entry.
	if _, err := tblB.AddDirectPeer(domaintest.ID("id-C")); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}

	digest, count := tblB.AnnounceDigestFor(domaintest.ID("id-A"))
	if count != 0 {
		t.Fatalf("AnnounceDigestFor count = %d, want 0 (never announced to A)", count)
	}
	if want := ComputeSyncDigest(nil); digest != want {
		t.Fatalf("AnnounceDigestFor digest = %s, want empty-input digest %s", digest, want)
	}
}

// TestAnnounceDigestFor_DropsDeadTransitIdentity — once a previously
// announced transit route goes Dead (no live announceable winner remains),
// it must drop out of the outbound digest so the digest mismatches the
// peer's stale view and the reconnect falls back to a full sync.
func TestAnnounceDigestFor_DropsDeadTransitIdentity(t *testing.T) {
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	tblB := NewTable(WithLocalOrigin(domaintest.ID("id-B")), WithClock(fixedClock(now)))

	// Transit route to C via uplink U, announced to A.
	upsertClaim(t, tblB, domaintest.ID("id-C"), domaintest.ID("id-U"), 2, RouteSourceAnnouncement)
	if len(tblB.AnnounceTo(domaintest.ID("id-A"))) == 0 {
		t.Fatal("expected an announce entry for C")
	}
	withDigest, withCount := tblB.AnnounceDigestFor(domaintest.ID("id-A"))
	if withCount == 0 {
		t.Fatal("expected C in the outbound digest before it went Dead")
	}

	// Mark the (C, U) transit pair Dead — a Dead transit claim is excluded
	// from the announceable-winner check (Direct exemption does not apply
	// to a transit claim), so C drops out of the outbound digest.
	setNonShapingTierPair(t, tblB, domaintest.ID("id-C"), domaintest.ID("id-U"), HealthDead)

	afterDigest, afterCount := tblB.AnnounceDigestFor(domaintest.ID("id-A"))
	if afterCount != 0 {
		t.Fatalf("Dead transit identity still in digest: count = %d, want 0", afterCount)
	}
	if afterDigest == withDigest {
		t.Fatal("digest unchanged after the route went Dead; identity not dropped")
	}
}
