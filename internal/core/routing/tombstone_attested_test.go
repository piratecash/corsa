package routing

import (
	"testing"
)

// tombstone_attested_test.go pins the Round-13 fix on
// AnnounceProjectionFor's tombstone branch: stored AttestedSig /
// AttestedSigVerified must be forwarded on the per-peer tombstone
// retry (and the full-sync withdrawal redelivery this branch feeds),
// mirroring the liveWinner branch. Before the fix the tombstone
// AnnounceEntry was built without sig fields, so a route emitted as
// signed during the live phase silently downgraded to unsigned the
// moment its uplink disconnected and the receiver saw the
// withdrawal via a tombstone retry — breaking the attested-links
// forwarding contract for the live→tombstone transition.

// TestTombstoneProjection_ForwardsAttestedSig walks the production
// path end-to-end at the table API: a direct claim is signed
// (simulating the Phase 5 self-attestation path that puts a sig on
// the local direct claim before withdrawal), the peer disconnects
// (RemoveDirectPeer → InvalidateAllVia preserves the sig on the
// stored tombstone), and a subsequent AnnounceTo to a DIFFERENT
// peer (the per-peer tombstone retry path) must emit the tombstone
// with the sig still attached.
func TestTombstoneProjection_ForwardsAttestedSig(t *testing.T) {
	const (
		local  PeerIdentity = "node-A"
		direct PeerIdentity = "peer-X"
		other  PeerIdentity = "peer-Y"
	)

	tbl := NewTable(WithLocalOrigin(local))
	if _, err := tbl.AddDirectPeer(direct); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}

	// Splice an attested-links signature into the stored direct claim.
	// AddDirectPeer doesn't take a sig today (the production sig path is
	// Phase 5 self-attestation), so the test reaches into the bucket
	// directly. This is the same package, so unexported access is
	// allowed; production code never mutates a claim this way — it
	// flows through UpdateRoute / ApplyUpdate.
	sig := []byte("attested-sig-bytes")
	tbl.mu.Lock()
	bucket := tbl.store.buckets[direct]
	found := false
	for i := range bucket {
		if bucket[i].Uplink == direct {
			bucket[i].AttestedSig = sig
			bucket[i].AttestedSigVerified = true
			found = true
			break
		}
	}
	tbl.mu.Unlock()
	if !found {
		t.Fatalf("test setup: direct claim for %q missing from bucket", direct)
	}

	// Disconnect → withdrawal. RemoveDirectPeer's InvalidateAllVia
	// already preserves the sig on the stored tombstone (Round-7),
	// so the storage state at this point carries the sig. The
	// returned Withdrawals slice (broadcast to all currently-known
	// peers) ALSO carries the sig — what Round-13 fixes is the
	// LATER per-peer projection path below, not the immediate
	// broadcast.
	if _, err := tbl.RemoveDirectPeer(direct); err != nil {
		t.Fatalf("RemoveDirectPeer: %v", err)
	}

	// Per-peer tombstone retry: AnnounceTo for a peer the
	// RemoveDirectPeer broadcast did not reach (or that needs a
	// full-sync redelivery). This is the AnnounceProjectionFor
	// tombstone-branch the Round-13 fix touches.
	entries := tbl.AnnounceTo(other)
	var tomb *AnnounceEntry
	for i := range entries {
		if entries[i].Identity == direct {
			tomb = &entries[i]
			break
		}
	}
	if tomb == nil {
		t.Fatalf("tombstone for %q must be in AnnounceTo projection; got %+v", direct, entries)
	}
	if tomb.Hops != HopsInfinity {
		t.Fatalf("expected tombstone (Hops=HopsInfinity), got Hops=%d", tomb.Hops)
	}
	if !tomb.AttestedSigVerified {
		t.Fatal("tombstone projection dropped AttestedSigVerified — Round-13 contract broken")
	}
	if string(tomb.AttestedSig) != string(sig) {
		t.Fatalf("tombstone projection AttestedSig: got %q want %q", tomb.AttestedSig, sig)
	}
}

// TestTombstoneProjection_UnsignedClaimStaysUnsigned is the negative
// pin: a tombstone whose stored claim was unsigned must NOT acquire
// a sig out of thin air through the projection. Without this
// assertion the Round-13 fix could regress to "always forward
// whatever bytes are on the claim" without surfacing as a test
// failure when those bytes happen to be empty.
func TestTombstoneProjection_UnsignedClaimStaysUnsigned(t *testing.T) {
	const (
		local  PeerIdentity = "node-A"
		direct PeerIdentity = "peer-X"
		other  PeerIdentity = "peer-Y"
	)

	tbl := NewTable(WithLocalOrigin(local))
	if _, err := tbl.AddDirectPeer(direct); err != nil {
		t.Fatalf("AddDirectPeer: %v", err)
	}
	if _, err := tbl.RemoveDirectPeer(direct); err != nil {
		t.Fatalf("RemoveDirectPeer: %v", err)
	}

	entries := tbl.AnnounceTo(other)
	var tomb *AnnounceEntry
	for i := range entries {
		if entries[i].Identity == direct {
			tomb = &entries[i]
			break
		}
	}
	if tomb == nil {
		t.Fatalf("tombstone for %q must be in AnnounceTo projection", direct)
	}
	if tomb.AttestedSigVerified {
		t.Fatal("tombstone for an unsigned claim must NOT carry verified=true")
	}
	if len(tomb.AttestedSig) != 0 {
		t.Fatalf("tombstone for an unsigned claim must NOT acquire sig bytes; got %q", tomb.AttestedSig)
	}
}
