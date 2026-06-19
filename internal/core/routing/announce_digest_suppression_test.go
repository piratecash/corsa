package routing

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// announce_digest_suppression_test.go covers Phase 3 PR 12.5 —
// the per-peer digest suppression that elides the reconnect / periodic
// forced-full-sync. Tests here are unit-level over the AnnounceLoop
// helpers; the loop-cycle integration (suppression actually skipping a
// forced full) lives in announce_resync_suppression_test.go.
//
// Post-review contract:
//   - MarkPeerDigestPending arms a SHORT window and stores the digest we
//     emitted as the correlation anchor.
//   - ConfirmPeerDigestMatch extends the window to one forced-full
//     cadence ONLY when an inbound summary echoes exactly that digest.
//   - Window length is derived from EffectiveForcedFullSyncInterval(a.interval),
//     never a caller-supplied deadline.

// TestMarkPeerDigestPending_ArmsShortWindow — the reconnect pending
// window must be DigestRoundTripGrace long when the cadence is larger.
func TestMarkPeerDigestPending_ArmsShortWindow(t *testing.T) {
	// interval=10s → cadence=100s > DigestRoundTripGrace (5s) → grace unclamped.
	a := &AnnounceLoop{interval: 10 * time.Second}
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	a.MarkPeerDigestPending(domaintest.ID("id-peer"), now, "dig")

	if !a.isDigestSuppressionActive(domaintest.ID("id-peer"), now) {
		t.Fatal("pending suppression not active at arm time")
	}
	if !a.isDigestSuppressionActive(domaintest.ID("id-peer"), now.Add(DigestRoundTripGrace-time.Millisecond)) {
		t.Fatal("pending suppression not active just before grace deadline")
	}
	if a.isDigestSuppressionActive(domaintest.ID("id-peer"), now.Add(DigestRoundTripGrace)) {
		t.Fatal("pending suppression still active at the grace deadline; gate must be exclusive")
	}
}

// TestMarkPeerDigestPending_ClampedToCadence — a node with a very short
// announce interval has a cadence below DigestRoundTripGrace, so the
// pending window must clamp to the cadence (freshness invariant).
func TestMarkPeerDigestPending_ClampedToCadence(t *testing.T) {
	// interval=200ms → cadence=multiplier*200ms=2s < DigestRoundTripGrace (5s) → clamp to 2s.
	a := &AnnounceLoop{interval: 200 * time.Millisecond}
	cadence := EffectiveForcedFullSyncInterval(a.interval)
	if cadence >= DigestRoundTripGrace {
		t.Fatalf("precondition: cadence %v must be below grace %v", cadence, DigestRoundTripGrace)
	}
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	a.MarkPeerDigestPending(domaintest.ID("id-peer"), now, "dig")

	if !a.isDigestSuppressionActive(domaintest.ID("id-peer"), now.Add(cadence-time.Millisecond)) {
		t.Fatal("pending suppression not active just before clamped cadence deadline")
	}
	if a.isDigestSuppressionActive(domaintest.ID("id-peer"), now.Add(cadence)) {
		t.Fatal("pending suppression active at/after clamped cadence; clamp not applied")
	}
}

// TestMarkPeerDigestPending_ZeroIntervalNoSuppression — with no
// configured interval the cadence is zero, so the clamp drives the
// window to zero and no pending suppression is armed.
func TestMarkPeerDigestPending_ZeroIntervalNoSuppression(t *testing.T) {
	a := &AnnounceLoop{} // interval == 0 → cadence == 0 → window == 0
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	a.MarkPeerDigestPending(domaintest.ID("id-peer"), now, "dig")
	if a.isDigestSuppressionActive(domaintest.ID("id-peer"), now) {
		t.Fatal("pending suppression active despite zero cadence")
	}
}

// TestMarkPeerDigestPending_EmptyPeerIsNoop — empty identity allocates
// nothing.
func TestMarkPeerDigestPending_EmptyPeerIsNoop(t *testing.T) {
	a := &AnnounceLoop{interval: 10 * time.Second}
	a.MarkPeerDigestPending(domain.PeerIdentity{}, time.Now(), "dig")
	if a.digestSuppression != nil {
		t.Fatal("MarkPeerDigestPending allocated map for empty peer")
	}
}

// TestConfirmPeerDigestMatch_ExtendsWindowOnCorrelatedEcho — a summary
// that echoes the digest we sent extends the window from the short grace
// to one full forced-full cadence.
func TestConfirmPeerDigestMatch_ExtendsWindowOnCorrelatedEcho(t *testing.T) {
	a := &AnnounceLoop{interval: 10 * time.Second} // cadence 20s, grace 5s
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	a.MarkPeerDigestPending(domaintest.ID("id-peer"), now, "dig")

	if !a.ConfirmPeerDigestMatch(domaintest.ID("id-peer"), "dig", now) {
		t.Fatal("correlated echo was not confirmed")
	}
	cadence := EffectiveForcedFullSyncInterval(a.interval)
	// Active well past the original short grace — proves the extension.
	if !a.isDigestSuppressionActive(domaintest.ID("id-peer"), now.Add(DigestRoundTripGrace+time.Second)) {
		t.Fatal("window not extended past the pending grace after a correlated match")
	}
	if !a.isDigestSuppressionActive(domaintest.ID("id-peer"), now.Add(cadence-time.Millisecond)) {
		t.Fatal("window not active just before the cadence deadline")
	}
	if a.isDigestSuppressionActive(domaintest.ID("id-peer"), now.Add(cadence)) {
		t.Fatal("window still active at the cadence deadline")
	}
}

// TestConfirmPeerDigestMatch_SingleShot — a second correlated summary
// echoing the SAME digest within the window must not re-extend it.
// One correlated reply defers at most one forced-full cadence; a
// duplicate or replayed summary cannot keep the window alive forever.
func TestConfirmPeerDigestMatch_SingleShot(t *testing.T) {
	a := &AnnounceLoop{interval: 10 * time.Second} // cadence 20s
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	cadence := EffectiveForcedFullSyncInterval(a.interval)
	a.MarkPeerDigestPending(domaintest.ID("id-peer"), now, "dig")

	if !a.ConfirmPeerDigestMatch(domaintest.ID("id-peer"), "dig", now) {
		t.Fatal("first correlated echo was not confirmed")
	}
	// First confirm extended the window to now+cadence. A second
	// confirm later in the window must be rejected (already consumed)
	// and must NOT push the deadline to later.Add(cadence).
	later := now.Add(cadence / 2)
	if a.ConfirmPeerDigestMatch(domaintest.ID("id-peer"), "dig", later) {
		t.Fatal("second confirm with same digest re-armed suppression; not single-shot")
	}
	// The window still ends at the ORIGINAL now+cadence, not extended.
	if a.isDigestSuppressionActive(domaintest.ID("id-peer"), now.Add(cadence)) {
		t.Fatal("window extended past the first cadence by a replayed summary")
	}
}

// TestConfirmPeerDigestMatch_IgnoresWrongEcho — a summary echoing some
// OTHER digest must not confirm; the short pending window stands but is
// not extended, so it elapses at the grace deadline.
func TestConfirmPeerDigestMatch_IgnoresWrongEcho(t *testing.T) {
	a := &AnnounceLoop{interval: 10 * time.Second}
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	a.MarkPeerDigestPending(domaintest.ID("id-peer"), now, "dig")

	if a.ConfirmPeerDigestMatch(domaintest.ID("id-peer"), "other-digest", now) {
		t.Fatal("uncorrelated echo was confirmed; correlation broken")
	}
	// The pending grace is untouched, so it still elapses on schedule.
	if a.isDigestSuppressionActive(domaintest.ID("id-peer"), now.Add(DigestRoundTripGrace)) {
		t.Fatal("window unexpectedly active past the pending grace")
	}
}

// TestConfirmPeerDigestMatch_IgnoresEmptyEcho — an empty echo carries no
// correlation and must never confirm.
func TestConfirmPeerDigestMatch_IgnoresEmptyEcho(t *testing.T) {
	a := &AnnounceLoop{interval: 10 * time.Second}
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	a.MarkPeerDigestPending(domaintest.ID("id-peer"), now, "dig")
	if a.ConfirmPeerDigestMatch(domaintest.ID("id-peer"), "", now) {
		t.Fatal("empty echo was confirmed")
	}
}

// TestConfirmPeerDigestMatch_IgnoresUnsolicited — a summary for a peer
// we never armed (no digest emitted) must not suppress anything.
func TestConfirmPeerDigestMatch_IgnoresUnsolicited(t *testing.T) {
	a := &AnnounceLoop{interval: 10 * time.Second}
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	if a.ConfirmPeerDigestMatch(domaintest.ID("id-peer"), "dig", now) {
		t.Fatal("unsolicited summary (no pending) was confirmed")
	}
	if a.isDigestSuppressionActive(domaintest.ID("id-peer"), now) {
		t.Fatal("suppression armed for an unsolicited summary")
	}
}

// TestConfirmPeerDigestMatch_IgnoresExpiredPending — a correct echo that
// arrives after the pending grace elapsed must not resurrect
// suppression; the safe path has already (or will) full-sync.
func TestConfirmPeerDigestMatch_IgnoresExpiredPending(t *testing.T) {
	a := &AnnounceLoop{interval: 10 * time.Second}
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	a.MarkPeerDigestPending(domaintest.ID("id-peer"), now, "dig")
	late := now.Add(DigestRoundTripGrace) // exactly at/after the grace
	if a.ConfirmPeerDigestMatch(domaintest.ID("id-peer"), "dig", late) {
		t.Fatal("echo confirmed against an elapsed pending window")
	}
}

// TestConfirmPeerDigestMatch_EmptyPeerIsNoop — defensive guard.
func TestConfirmPeerDigestMatch_EmptyPeerIsNoop(t *testing.T) {
	a := &AnnounceLoop{interval: 10 * time.Second}
	if a.ConfirmPeerDigestMatch(domain.PeerIdentity{}, "dig", time.Now()) {
		t.Fatal("empty peer was confirmed")
	}
}

// TestIsDigestSuppressionActive_PerPeerScoping — suppression for peer-A
// must not affect peer-B.
func TestIsDigestSuppressionActive_PerPeerScoping(t *testing.T) {
	a := &AnnounceLoop{interval: 10 * time.Second}
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	a.MarkPeerDigestPending(domaintest.ID("id-peer-A"), now, "dig")

	if !a.isDigestSuppressionActive(domaintest.ID("id-peer-A"), now) {
		t.Fatal("peer-A suppression not registered")
	}
	if a.isDigestSuppressionActive(domaintest.ID("id-peer-B"), now) {
		t.Fatal("peer-B suppression leaked from peer-A")
	}
}

// TestIsDigestSuppressionActive_AbsentPeerReportsFalse — cold-start.
func TestIsDigestSuppressionActive_AbsentPeerReportsFalse(t *testing.T) {
	a := &AnnounceLoop{interval: 10 * time.Second}
	if a.isDigestSuppressionActive(domaintest.ID("id-anyone"), time.Now()) {
		t.Fatal("suppression active for never-marked peer")
	}
}

// TestClearPeerDigestSuppression_RemovesActiveWindow — a mismatch
// summary clears a pending window so the next cycle full-syncs.
func TestClearPeerDigestSuppression_RemovesActiveWindow(t *testing.T) {
	a := &AnnounceLoop{interval: 10 * time.Second}
	now := time.Date(2026, 5, 23, 12, 0, 0, 0, time.UTC)
	a.MarkPeerDigestPending(domaintest.ID("id-peer"), now, "dig")
	if !a.isDigestSuppressionActive(domaintest.ID("id-peer"), now) {
		t.Fatal("precondition: pending suppression must be active")
	}
	a.ClearPeerDigestSuppression(domaintest.ID("id-peer"))
	if a.isDigestSuppressionActive(domaintest.ID("id-peer"), now) {
		t.Fatal("suppression still active after ClearPeerDigestSuppression")
	}
}

// TestClearPeerDigestSuppression_AbsentPeerIsNoop — clearing a peer that
// was never armed must not panic or allocate.
func TestClearPeerDigestSuppression_AbsentPeerIsNoop(t *testing.T) {
	a := &AnnounceLoop{interval: 10 * time.Second}
	a.ClearPeerDigestSuppression(domaintest.ID("id-never-armed")) // must not panic
	a.ClearPeerDigestSuppression(domain.PeerIdentity{})           // empty guard
}
