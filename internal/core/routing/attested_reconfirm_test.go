package routing

import (
	"encoding/json"
	"testing"
	"time"
)

// attested_reconfirm_test.go pins the Round-11 contract on the
// same-SeqNo / same-Hops reconfirmation branch of route_store_mutation.go
// (the `sameHopsReconfirmation` block). Before the fix that branch only
// refreshed ExpiresAt / UpdatedAt and dropped any incoming
// AttestedSig / AttestedSigVerified, so a route first learned as
// legacy/unsigned could never be upgraded by a subsequent signed
// re-announcement from the SAME peer at the SAME SeqNo + same hops —
// it had to wait for a seq bump or TTL cleanup. CompositeScore reads
// AttestedSigVerified for the scoreSignedBonus tie-break, so the
// dropped upgrade meant trust never converged once
// mesh_attested_links_v1 went live on the rolling re-enable.
//
// Contract pinned here:
//   1. Verified incoming UPGRADES an existing unsigned/unverified
//      claim (verified bit + fresh sig bytes).
//   2. Unsigned incoming MUST NOT downgrade an already-verified
//      claim — the local verify was a real observation we still
//      trust; the peer may simply have stopped sending the sig.
//   3. Both unverified, incoming has bytes / stored does not: bytes
//      are forwarded through storage so a downstream peer that DOES
//      have the cap can verify on its side.

const (
	reconfirmIdent  PeerIdentity = "alice"
	reconfirmOrigin PeerIdentity = "bob"
	reconfirmUplink PeerIdentity = "charlie"
)

func reconfirmKey() RouteTriple {
	return RouteTriple{
		Identity: reconfirmIdent,
		Origin:   reconfirmOrigin,
		NextHop:  reconfirmUplink,
	}
}

// TestReconfirmation_VerifiedIncomingUpgradesUnsignedStored is the
// primary fix pin: unsigned legacy first, verified signed second,
// stored claim must end up verified with the new sig bytes.
func TestReconfirmation_VerifiedIncomingUpgradesUnsignedStored(t *testing.T) {
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	mustUpdate(t, tbl, RouteEntry{
		Identity: reconfirmIdent, Origin: reconfirmOrigin, NextHop: reconfirmUplink,
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
	})
	pre := tbl.InspectTriple(reconfirmKey())
	if pre == nil {
		t.Fatalf("precondition: stored claim must exist after initial admission")
	}
	if pre.AttestedSigVerified {
		t.Fatalf("precondition: legacy/unsigned claim must start AttestedSigVerified=false")
	}
	if len(pre.AttestedSig) != 0 {
		t.Fatalf("precondition: legacy claim must start with empty AttestedSig; got %q", pre.AttestedSig)
	}

	// Reconfirmation at same SeqNo + same Hops, now carrying a
	// verified signed v3 entry.
	sigBytes := []byte("verified-sig-bytes")
	status, err := tbl.UpdateRoute(RouteEntry{
		Identity: reconfirmIdent, Origin: reconfirmOrigin, NextHop: reconfirmUplink,
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
		AttestedSig:         sigBytes,
		AttestedSigVerified: true,
	})
	if err != nil {
		t.Fatalf("UpdateRoute: %v", err)
	}
	if status != RouteUnchanged {
		t.Fatalf("status: got %v want RouteUnchanged (same SeqNo + same Hops reconfirmation)", status)
	}

	post := tbl.InspectTriple(reconfirmKey())
	if post == nil {
		t.Fatalf("post: stored claim must still exist")
	}
	if !post.AttestedSigVerified {
		t.Fatal("reconfirmation with verified sig must upgrade AttestedSigVerified=true")
	}
	if string(post.AttestedSig) != string(sigBytes) {
		t.Fatalf("AttestedSig bytes not upgraded: got %q want %q", post.AttestedSig, sigBytes)
	}
}

// TestReconfirmation_UnsignedIncomingDoesNotDowngradeVerified pins the
// second rule: an unsigned reconfirmation against a verified stored
// claim must keep the verified state (local observation still trusted).
func TestReconfirmation_UnsignedIncomingDoesNotDowngradeVerified(t *testing.T) {
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	sigBytes := []byte("verified-sig-bytes")
	mustUpdate(t, tbl, RouteEntry{
		Identity: reconfirmIdent, Origin: reconfirmOrigin, NextHop: reconfirmUplink,
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
		AttestedSig:         sigBytes,
		AttestedSigVerified: true,
	})
	pre := tbl.InspectTriple(reconfirmKey())
	if pre == nil || !pre.AttestedSigVerified {
		t.Fatalf("precondition: stored claim must start verified")
	}

	// Reconfirmation at same SeqNo + same Hops, no sig (peer dropped
	// the cap, or this cycle's frame stripped sig for some reason).
	status, err := tbl.UpdateRoute(RouteEntry{
		Identity: reconfirmIdent, Origin: reconfirmOrigin, NextHop: reconfirmUplink,
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
		// AttestedSig and AttestedSigVerified deliberately zero.
	})
	if err != nil {
		t.Fatalf("UpdateRoute: %v", err)
	}
	if status != RouteUnchanged {
		t.Fatalf("status: got %v want RouteUnchanged", status)
	}

	post := tbl.InspectTriple(reconfirmKey())
	if post == nil {
		t.Fatalf("post: stored claim must still exist")
	}
	if !post.AttestedSigVerified {
		t.Fatal("unsigned reconfirmation must NOT downgrade verified state")
	}
	if string(post.AttestedSig) != string(sigBytes) {
		t.Fatalf("verified sig bytes must survive an unsigned reconfirmation: got %q want %q", post.AttestedSig, sigBytes)
	}
}

// TestReconfirmation_UnverifiedIncomingForwardsBytesToUnverifiedStored
// pins the third rule: both sides unverified — receiver did not
// negotiate mesh_attested_links_v1 — but the incoming carries sig
// bytes that should pass through storage so a downstream peer with
// the cap can verify on its side. Without this rule the bytes were
// dropped on reconfirmation and a peer at hop 2 would never see them.
func TestReconfirmation_UnverifiedIncomingForwardsBytesToUnverifiedStored(t *testing.T) {
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	mustUpdate(t, tbl, RouteEntry{
		Identity: reconfirmIdent, Origin: reconfirmOrigin, NextHop: reconfirmUplink,
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
	})
	pre := tbl.InspectTriple(reconfirmKey())
	if pre == nil || pre.AttestedSigVerified || len(pre.AttestedSig) != 0 {
		t.Fatalf("precondition: stored claim must be unsigned + unverified")
	}

	// Reconfirmation at same SeqNo + same Hops, sig bytes present but
	// verified=false (no cap negotiation, but bytes flow through).
	sigBytes := []byte("forwarded-sig-bytes")
	status, err := tbl.UpdateRoute(RouteEntry{
		Identity: reconfirmIdent, Origin: reconfirmOrigin, NextHop: reconfirmUplink,
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
		AttestedSig:         sigBytes,
		AttestedSigVerified: false,
	})
	if err != nil {
		t.Fatalf("UpdateRoute: %v", err)
	}
	if status != RouteUnchanged {
		t.Fatalf("status: got %v want RouteUnchanged", status)
	}

	post := tbl.InspectTriple(reconfirmKey())
	if post == nil {
		t.Fatalf("post: stored claim must still exist")
	}
	if post.AttestedSigVerified {
		t.Fatal("forwarded bytes must NOT flip verified bit — local never verified")
	}
	if string(post.AttestedSig) != string(sigBytes) {
		t.Fatalf("forwarded sig bytes not stored: got %q want %q", post.AttestedSig, sigBytes)
	}
}

// TestReconfirmation_VerifiedUpgradeCopiesIncomingExtra pins the
// Round-12 fix: the attested-links signature covers
// lenpfx(identity) || lenpfx(extra), so storing a new
// AttestedSig WITHOUT the matching Extra would mark the OLD Extra as
// "verified by a signature computed over different bytes" — a bogus
// trust bonus locally AND a later re-emit of (old Extra, new sig)
// that downstream peers cannot verify. The fix treats Extra as part
// of the cryptographic pair: an upgrade copies both together.
func TestReconfirmation_VerifiedUpgradeCopiesIncomingExtra(t *testing.T) {
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	oldExtra := json.RawMessage(`{"v":1,"anchor":"old"}`)
	mustUpdate(t, tbl, RouteEntry{
		Identity: reconfirmIdent, Origin: reconfirmOrigin, NextHop: reconfirmUplink,
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
		Extra: oldExtra,
	})
	pre := tbl.InspectTriple(reconfirmKey())
	if pre == nil || string(pre.Extra) != string(oldExtra) {
		t.Fatalf("precondition: stored Extra must equal initial; got %q", pre.Extra)
	}
	if pre.AttestedSigVerified {
		t.Fatalf("precondition: unsigned claim must start unverified")
	}

	// Same SeqNo + same hops, but a verified signed reconfirmation
	// arrives with DIFFERENT Extra. The signature was computed over
	// the NEW Extra (canonical bytes are identity || extra), so the
	// stored Extra MUST be updated alongside the sig — otherwise the
	// stored state would have (old Extra) marked verified by a sig
	// over (new Extra).
	newExtra := json.RawMessage(`{"v":1,"anchor":"new"}`)
	newSig := []byte("sig-over-new-extra")
	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: reconfirmIdent, Origin: reconfirmOrigin, NextHop: reconfirmUplink,
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
		Extra:               newExtra,
		AttestedSig:         newSig,
		AttestedSigVerified: true,
	})
	if err != nil {
		t.Fatalf("UpdateRoute: %v", err)
	}

	post := tbl.InspectTriple(reconfirmKey())
	if post == nil {
		t.Fatalf("post: stored claim must still exist")
	}
	if !post.AttestedSigVerified {
		t.Fatal("verified sig must flip AttestedSigVerified=true")
	}
	if string(post.AttestedSig) != string(newSig) {
		t.Fatalf("AttestedSig bytes: got %q want %q", post.AttestedSig, newSig)
	}
	if string(post.Extra) != string(newExtra) {
		t.Fatalf("Extra MUST be copied alongside the sig — got %q want %q (Round-12 pair-consistency)", post.Extra, newExtra)
	}
}

// TestReconfirmation_ForwardThroughBytesCopyIncomingExtra is the
// "both unverified" analogue of the verified-upgrade test: forwarded
// sig bytes still need to be stored alongside the Extra they cover,
// otherwise a downstream peer with the cap would compute canonical
// bytes from (stored Extra) and fail to verify the (incoming sig)
// that was actually signed over (incoming Extra).
func TestReconfirmation_ForwardThroughBytesCopyIncomingExtra(t *testing.T) {
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	oldExtra := json.RawMessage(`{"v":1,"anchor":"old"}`)
	mustUpdate(t, tbl, RouteEntry{
		Identity: reconfirmIdent, Origin: reconfirmOrigin, NextHop: reconfirmUplink,
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
		Extra: oldExtra,
	})

	newExtra := json.RawMessage(`{"v":1,"anchor":"new"}`)
	newSig := []byte("sig-over-new-extra")
	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: reconfirmIdent, Origin: reconfirmOrigin, NextHop: reconfirmUplink,
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
		Extra:               newExtra,
		AttestedSig:         newSig,
		AttestedSigVerified: false, // local did not verify (no cap negotiated)
	})
	if err != nil {
		t.Fatalf("UpdateRoute: %v", err)
	}

	post := tbl.InspectTriple(reconfirmKey())
	if post == nil {
		t.Fatalf("post: stored claim must still exist")
	}
	if post.AttestedSigVerified {
		t.Fatal("forward-through must NOT flip verified bit — local never verified")
	}
	if string(post.AttestedSig) != string(newSig) {
		t.Fatalf("AttestedSig bytes: got %q want %q", post.AttestedSig, newSig)
	}
	if string(post.Extra) != string(newExtra) {
		t.Fatalf("Extra MUST be copied alongside forward-through sig bytes — got %q want %q (Round-12 pair-consistency)", post.Extra, newExtra)
	}
}

// TestReconfirmation_UnsignedDoesNotChangeExtra pins the narrow
// scope of the Round-12 fix: Extra is only touched when sig metadata
// is also being touched. An unsigned reconfirmation with different
// Extra leaves the stored Extra alone — same-SeqNo unsigned content
// changes are still treated as no-ops (the prior reconfirmation
// behaviour), avoiding scope creep into "what does same-SeqNo
// different-Extra mean for unsigned wire frames" (answer: protocol
// oddity — origin should bump SeqNo on content change, and we keep
// the existing first-write-wins semantic for that case).
func TestReconfirmation_UnsignedDoesNotChangeExtra(t *testing.T) {
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	oldExtra := json.RawMessage(`{"v":1,"anchor":"old"}`)
	mustUpdate(t, tbl, RouteEntry{
		Identity: reconfirmIdent, Origin: reconfirmOrigin, NextHop: reconfirmUplink,
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
		Extra: oldExtra,
	})

	newExtra := json.RawMessage(`{"v":1,"anchor":"new"}`)
	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: reconfirmIdent, Origin: reconfirmOrigin, NextHop: reconfirmUplink,
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
		Extra: newExtra,
		// No sig — no upgrade trigger.
	})
	if err != nil {
		t.Fatalf("UpdateRoute: %v", err)
	}

	post := tbl.InspectTriple(reconfirmKey())
	if post == nil {
		t.Fatalf("post: stored claim must still exist")
	}
	if string(post.Extra) != string(oldExtra) {
		t.Fatalf("unsigned reconfirmation with different Extra must NOT touch stored Extra — got %q want %q", post.Extra, oldExtra)
	}
}

// TestReconfirmation_UnsignedDowngradeAttemptKeepsStoredExtra
// covers the anti-downgrade branch with a different-Extra payload:
// when stored is verified and incoming is unsigned with different
// Extra, the fix must preserve BOTH the verified sig AND its Extra
// (otherwise the stored sig would end up paired with a different
// Extra than the one it was signed over).
func TestReconfirmation_UnsignedDowngradeAttemptKeepsStoredExtra(t *testing.T) {
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	signedExtra := json.RawMessage(`{"v":1,"anchor":"signed"}`)
	sigBytes := []byte("sig-over-signed-extra")
	mustUpdate(t, tbl, RouteEntry{
		Identity: reconfirmIdent, Origin: reconfirmOrigin, NextHop: reconfirmUplink,
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
		Extra:               signedExtra,
		AttestedSig:         sigBytes,
		AttestedSigVerified: true,
	})

	// Incoming: unsigned + different Extra. The downgrade-skip branch
	// fires (incoming has no sig, stored is verified) so neither sig
	// nor extra change — the cryptographic pair stays intact.
	driftExtra := json.RawMessage(`{"v":1,"anchor":"drift"}`)
	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: reconfirmIdent, Origin: reconfirmOrigin, NextHop: reconfirmUplink,
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
		Extra: driftExtra,
	})
	if err != nil {
		t.Fatalf("UpdateRoute: %v", err)
	}

	post := tbl.InspectTriple(reconfirmKey())
	if post == nil {
		t.Fatalf("post: stored claim must still exist")
	}
	if !post.AttestedSigVerified {
		t.Fatal("anti-downgrade: verified bit must survive unsigned reconfirmation")
	}
	if string(post.AttestedSig) != string(sigBytes) {
		t.Fatalf("anti-downgrade: stored sig bytes must survive; got %q want %q", post.AttestedSig, sigBytes)
	}
	if string(post.Extra) != string(signedExtra) {
		t.Fatalf("anti-downgrade: stored Extra must survive (cryptographic pair); got %q want %q", post.Extra, signedExtra)
	}
}

// TestReconfirmation_UnverifiedRotationIsIgnored pins the Round-15
// churn-prevention contract: once storage has any AttestedSig bytes,
// subsequent unverified incoming with DIFFERENT bytes must be
// IGNORED. Round-14 made AttestedSig part of ComputeDelta and the
// outboundEmitSig content cache, so without this guard an attacker
// peer could rotate fake sig bytes at same-seq/same-hops to force
// fresh wire SeqNos and emit cycles with no verifiable benefit.
// First observation still stores (so a downstream peer with the cap
// can verify); only the REWRITE path on the unverified branch is
// closed.
func TestReconfirmation_UnverifiedRotationIsIgnored(t *testing.T) {
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	mustUpdate(t, tbl, RouteEntry{
		Identity: reconfirmIdent, Origin: reconfirmOrigin, NextHop: reconfirmUplink,
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
	})

	// First observation of unverified bytes — must store.
	firstBytes := []byte("first-unverified-sig")
	if _, err := tbl.UpdateRoute(RouteEntry{
		Identity: reconfirmIdent, Origin: reconfirmOrigin, NextHop: reconfirmUplink,
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
		AttestedSig:         firstBytes,
		AttestedSigVerified: false,
	}); err != nil {
		t.Fatalf("UpdateRoute first observation: %v", err)
	}
	after1 := tbl.InspectTriple(reconfirmKey())
	if after1 == nil || string(after1.AttestedSig) != string(firstBytes) {
		t.Fatalf("first observation must store unverified bytes; got %q", after1.AttestedSig)
	}

	// Attempted rotation — same identity, different unverified bytes.
	// Must be IGNORED: stored bytes stay at the first observation.
	rotatedBytes := []byte("rotated-fake-sig")
	if _, err := tbl.UpdateRoute(RouteEntry{
		Identity: reconfirmIdent, Origin: reconfirmOrigin, NextHop: reconfirmUplink,
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
		AttestedSig:         rotatedBytes,
		AttestedSigVerified: false,
	}); err != nil {
		t.Fatalf("UpdateRoute rotation attempt: %v", err)
	}
	after2 := tbl.InspectTriple(reconfirmKey())
	if after2 == nil {
		t.Fatalf("post: stored claim must still exist")
	}
	if string(after2.AttestedSig) != string(firstBytes) {
		t.Fatalf("unverified rotation must be ignored; bytes drifted from %q to %q (Round-15 churn-prevention broken)", firstBytes, after2.AttestedSig)
	}
}

// TestReconfirmation_VerifiedStillUpgradesOverStoredUnverified pins
// the upgrade-vs-churn-prevention distinction. Round-15 closes
// unverified rewrites, but a Verified incoming MUST still upgrade
// over a stored unverified state — that is the entire point of
// rolling enable for mesh_attested_links_v1 / Phase 5 anchor
// publication. Without this assertion the churn-prevention guard
// could regress to "no rewrites of any kind" and the upgrade path
// would be silently broken.
func TestReconfirmation_VerifiedStillUpgradesOverStoredUnverified(t *testing.T) {
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	mustUpdate(t, tbl, RouteEntry{
		Identity: reconfirmIdent, Origin: reconfirmOrigin, NextHop: reconfirmUplink,
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
		AttestedSig:         []byte("stored-unverified"),
		AttestedSigVerified: false,
	})

	// Verified incoming — must overwrite even though stored already
	// has bytes.
	verifiedBytes := []byte("verified-upgrade-sig")
	if _, err := tbl.UpdateRoute(RouteEntry{
		Identity: reconfirmIdent, Origin: reconfirmOrigin, NextHop: reconfirmUplink,
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
		AttestedSig:         verifiedBytes,
		AttestedSigVerified: true,
	}); err != nil {
		t.Fatalf("UpdateRoute verified upgrade: %v", err)
	}
	post := tbl.InspectTriple(reconfirmKey())
	if post == nil {
		t.Fatalf("post: stored claim must still exist")
	}
	if !post.AttestedSigVerified {
		t.Fatal("verified upgrade must flip the verified bit even when stored already had unverified bytes")
	}
	if string(post.AttestedSig) != string(verifiedBytes) {
		t.Fatalf("verified upgrade must overwrite stored bytes; got %q want %q", post.AttestedSig, verifiedBytes)
	}
}

// TestReconfirmation_MarksDirtyOnSigUpgrade pins that the
// upgrade-to-verified path also flips dirty — CompositeScore's
// scoreSignedBonus reads from the snapshot, so without a republish the
// receiver's Lookup would not see the new bonus until the next
// dirty-triggering event.
func TestReconfirmation_MarksDirtyOnSigUpgrade(t *testing.T) {
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	tbl := NewTable(WithClock(fixedClock(now)))

	mustUpdate(t, tbl, RouteEntry{
		Identity: reconfirmIdent, Origin: reconfirmOrigin, NextHop: reconfirmUplink,
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
	})
	tbl.ConsumeDirty()

	_, err := tbl.UpdateRoute(RouteEntry{
		Identity: reconfirmIdent, Origin: reconfirmOrigin, NextHop: reconfirmUplink,
		Hops: 3, SeqNo: 5, Source: RouteSourceAnnouncement,
		AttestedSig:         []byte("sig"),
		AttestedSigVerified: true,
	})
	if err != nil {
		t.Fatalf("UpdateRoute: %v", err)
	}
	if !tbl.ConsumeDirty() {
		t.Fatal("sig upgrade must mark dirty so the published snapshot carries the new AttestedSigVerified for CompositeScore")
	}
}
