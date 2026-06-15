package node

import (
	"crypto/ed25519"
	"encoding/base64"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// routing_announce_v3_attest_test.go covers the Phase 4 13.2-B sign /
// verify wire round-trip for mesh_attested_links_v1: own-origin
// announcements get an Ed25519 attestation produced by the local
// identity key before emit; the receiver verifies via the destination
// identity's public key looked up from the knowledge store. Present-
// but-invalid sigs drop the offending entry; absent or pubkey-unknown
// sigs pass through unchanged (Tier-2 lenient — rank penalty for
// unsigned lands in 13.2-C).

// newTestServiceWithIdentity is the sign-test analogue of
// newTestServiceWithRouting: same routing fixture, but the Service
// carries a real Ed25519 identity (not just an Address string) so the
// signer has a private key to sign with and the verifier has a public
// key to register in the knowledge store.
func newTestServiceWithIdentity(t *testing.T) (*Service, *identity.Identity) {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	svc := newTestServiceWithRouting(t, domain.PeerIdentityFromWire(id.Address))
	svc.identity = id
	// publicKeyForIdentity reads s.pubKeys under knowledgeMu; the fixture
	// leaves the map nil by default so add a defensive initialiser.
	if svc.pubKeys == nil {
		svc.pubKeys = make(map[string]string)
	}
	return svc, id
}

// registerKnownPubKey teaches the test Service that `addr` has pubkey
// `pub`, mirroring what addKnownPubKey does in production.
func registerKnownPubKey(t *testing.T, svc *Service, addr string, pub ed25519.PublicKey) {
	t.Helper()
	svc.knowledgeMu.Lock()
	if svc.pubKeys == nil {
		svc.pubKeys = make(map[string]string)
	}
	svc.pubKeys[addr] = identity.PublicKeyBase64(pub)
	svc.knowledgeMu.Unlock()
}

func TestSignOwnOriginV3Entries_SignsOnlyOwnIdentityWithEmptySig(t *testing.T) {
	svc, id := newTestServiceWithIdentity(t)

	foreignSig := []byte("forwarded-from-elsewhere")
	entries := []routing.AnnounceEntry{
		// Own-origin entry without sig → must be signed.
		{Identity: domain.PeerIdentityFromWire(id.Address), SeqNo: 7, Extra: nil},
		// Own-origin entry with an existing sig → must NOT be overwritten.
		{Identity: domain.PeerIdentityFromWire(id.Address), SeqNo: 8, AttestedSig: foreignSig},
		// Foreign identity → must NOT be signed by us (we have no
		// authority over another identity's announcements).
		{Identity: idPeerB, SeqNo: 9},
	}
	out := svc.signOwnOriginV3Entries(entries, 42)
	if len(out[0].AttestedSig) != ed25519.SignatureSize {
		t.Fatalf("own-origin empty-sig entry not signed; got len=%d", len(out[0].AttestedSig))
	}
	if string(out[1].AttestedSig) != string(foreignSig) {
		t.Fatalf("own-origin pre-signed entry was overwritten; want %q got %q", foreignSig, out[1].AttestedSig)
	}
	if len(out[2].AttestedSig) != 0 {
		t.Fatalf("foreign identity entry was signed; sig=%x", out[2].AttestedSig)
	}

	// The produced signature must verify against our own public key over
	// CanonicalSigningBytes() — sanity check that signOwn / verify
	// agree on the canonical-bytes contract.
	v3e := protocol.RouteAnnounceV3Entry{Identity: id.Address, SeqNo: 7}
	if !ed25519.Verify(id.PublicKey, v3e.CanonicalSigningBytes(), out[0].AttestedSig) {
		t.Fatal("produced signature does not verify against own public key")
	}
}

func TestSignOwnOriginV3Entries_NoPrivateKeyShortCircuits(t *testing.T) {
	// Test fixtures that build Service with only an Address (no
	// PrivateKey) must not panic in the signer. The short-circuit
	// returns the slice untouched.
	svc := newTestServiceWithRouting(t, idNodeA)
	entries := []routing.AnnounceEntry{
		{Identity: idNodeA, SeqNo: 1},
	}
	out := svc.signOwnOriginV3Entries(entries, 1)
	if len(out[0].AttestedSig) != 0 {
		t.Fatalf("signer must short-circuit without a private key; got sig=%x", out[0].AttestedSig)
	}
}

func TestVerifyRouteAnnounceV3Sigs_ValidPassesInvalidDropped(t *testing.T) {
	svc, id := newTestServiceWithIdentity(t)
	registerKnownPubKey(t, svc, id.Address, id.PublicKey)

	// Build two entries: one with a valid signature, one with a
	// corrupt signature. The verifier must drop the invalid entry and
	// keep the valid one.
	epoch := uint64(11)
	v3eGood := protocol.RouteAnnounceV3Entry{Identity: id.Address, SeqNo: 1}
	goodSig := ed25519.Sign(id.PrivateKey, v3eGood.CanonicalSigningBytes())
	v3eBad := protocol.RouteAnnounceV3Entry{Identity: id.Address, SeqNo: 2}
	badSig := ed25519.Sign(id.PrivateKey, v3eBad.CanonicalSigningBytes())
	// Corrupt the bad sig by flipping a byte.
	badSig[0] ^= 0xff

	frames := []protocol.AnnounceRouteFrame{
		{Identity: id.Address, Origin: id.Address, Hops: 1, SeqNo: 1},
		{Identity: id.Address, Origin: id.Address, Hops: 1, SeqNo: 2},
	}
	sigs := [][]byte{goodSig, badSig}

	outFrames, outSigs, verified := svc.verifyRouteAnnounceV3Sigs(idPeerB, frames, sigs, epoch)
	if len(outFrames) != 1 || outFrames[0].SeqNo != 1 {
		t.Fatalf("verifier filter wrong: outFrames=%v want only SeqNo=1", outFrames)
	}
	if len(outSigs) != 1 || string(outSigs[0]) != string(goodSig) {
		t.Fatalf("verifier filter sig parallel slice wrong: outSigs=%v", outSigs)
	}
	if len(verified) != 1 || !verified[0] {
		t.Fatalf("surviving entry must be marked verified; got %v", verified)
	}
}

func TestVerifyRouteAnnounceV3Sigs_UnsignedPassthrough(t *testing.T) {
	svc, _ := newTestServiceWithIdentity(t)

	frames := []protocol.AnnounceRouteFrame{
		{Identity: idTargetX.String(), Origin: idOriginC.String(), Hops: 1, SeqNo: 1},
		{Identity: idPeerB.String(), Origin: idOriginC.String(), Hops: 1, SeqNo: 1},
	}
	sigs := [][]byte{nil, nil}

	outFrames, outSigs, verified := svc.verifyRouteAnnounceV3Sigs(idOriginC, frames, sigs, 1)
	if len(outFrames) != 2 || len(outSigs) != 2 || len(verified) != 2 {
		t.Fatalf("unsigned entries must pass through (got frames=%d sigs=%d verified=%d)", len(outFrames), len(outSigs), len(verified))
	}
	for i, v := range verified {
		if v {
			t.Fatalf("unsigned entry %d must have verified=false; got true", i)
		}
	}
}

func TestVerifyRouteAnnounceV3Sigs_SigWithUnknownPubkeyPassthrough(t *testing.T) {
	// Sig present but the destination identity's pubkey is NOT in our
	// knowledge store. Tier-2 lenient: the verifier passes through with
	// verified=false (rank penalty for unverifiable lands in 13.2-C
	// CompositeScore).
	svc, _ := newTestServiceWithIdentity(t)

	frames := []protocol.AnnounceRouteFrame{
		{Identity: idTargetX.String(), Origin: idOriginC.String(), Hops: 1, SeqNo: 1},
	}
	sigs := [][]byte{[]byte("garbage-but-pubkey-unknown")}

	outFrames, outSigs, verified := svc.verifyRouteAnnounceV3Sigs(idOriginC, frames, sigs, 1)
	if len(outFrames) != 1 {
		t.Fatalf("sig with unknown pubkey must pass through; got %d", len(outFrames))
	}
	if string(outSigs[0]) != "garbage-but-pubkey-unknown" {
		t.Fatalf("pass-through must preserve original sig; got %x", outSigs[0])
	}
	if verified[0] {
		t.Fatalf("unverified-pubkey entry must have verified=false; got true")
	}
}

func TestHandleRouteAnnounceV3_SignedEntryStoresVerifiedSig(t *testing.T) {
	// End-to-end: peer (own identity) sends a v3 frame with a valid sig
	// of its own announcement. We register the peer's pubkey so the
	// verifier can resolve it; expect the entry to land in storage with
	// the signature preserved verbatim.
	svc, _ := newTestServiceWithIdentity(t)
	// Use a separate identity for the peer (its private key signs).
	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	registerKnownPubKey(t, svc, peer.Address, peer.PublicKey)
	svc.announceLoop.StateRegistry().MarkReconnected(domain.PeerIdentityFromWire(peer.Address),
		[]routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3, domain.CapMeshAttestedLinksV1})
	// Phase 4 P2: verifier is gated on per-session mesh_attested_links_v1
	// negotiation. Wire a session at the sender address with the cap so
	// peerSupportsAttestedLinks returns true and the verifier runs.
	senderAddr := domain.PeerAddress("addr-peer")
	svc.peerMu.Lock()
	svc.sessions[senderAddr] = &peerSession{
		address:      senderAddr,
		peerIdentity: domain.PeerIdentityFromWire(peer.Address),
		capabilities: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3, domain.CapMeshAttestedLinksV1, domain.CapMeshRelayV1},
	}
	svc.peerMu.Unlock()
	svc.eventBus = newStormBus(t)

	epoch := uint64(3)
	v3e := protocol.RouteAnnounceV3Entry{Identity: peer.Address, SeqNo: 1}
	sigBytes := ed25519.Sign(peer.PrivateKey, v3e.CanonicalSigningBytes())
	frame := protocol.RouteAnnounceV3Frame{
		Kind:  protocol.RouteAnnounceV3KindFull,
		Epoch: epoch,
		Entries: []protocol.RouteAnnounceV3Entry{
			{Identity: peer.Address, Hops: 1, SeqNo: 1, Sig: base64.StdEncoding.EncodeToString(sigBytes)},
		},
	}
	svc.handleRouteAnnounceV3(domain.PeerIdentityFromWire(peer.Address), senderAddr, frame)

	got := svc.routingTable.Lookup(domain.PeerIdentityFromWire(peer.Address))
	if len(got) == 0 {
		t.Fatalf("verified entry was not applied to the table")
	}
	if string(got[0].AttestedSig) != string(sigBytes) {
		t.Fatalf("verified sig not preserved in storage; want %x got %x", sigBytes, got[0].AttestedSig)
	}
	// Phase 4 13.2-C: the verifier marked the entry verified=true; the
	// boundary must surface that flag so CompositeScore applies the
	// trust-score bonus at rank time.
	if !got[0].AttestedSigVerified {
		t.Fatalf("verified entry must have AttestedSigVerified=true on the boundary")
	}
}

func TestHandleRouteAnnounceV3_InvalidSignatureDropsEntry(t *testing.T) {
	// End-to-end: a signed entry whose signature does NOT verify must
	// be dropped before reaching storage. The peer's pubkey is known,
	// so the verifier can definitively reject.
	svc, _ := newTestServiceWithIdentity(t)
	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	registerKnownPubKey(t, svc, peer.Address, peer.PublicKey)
	svc.announceLoop.StateRegistry().MarkReconnected(domain.PeerIdentityFromWire(peer.Address),
		[]routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3, domain.CapMeshAttestedLinksV1})
	// Phase 4 P2: verifier is gated on per-session
	// mesh_attested_links_v1 negotiation — wire a session so the
	// verifier actually runs and the corrupt sig produces a drop.
	senderAddr := domain.PeerAddress("addr-peer")
	svc.peerMu.Lock()
	svc.sessions[senderAddr] = &peerSession{
		address:      senderAddr,
		peerIdentity: domain.PeerIdentityFromWire(peer.Address),
		capabilities: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3, domain.CapMeshAttestedLinksV1, domain.CapMeshRelayV1},
	}
	svc.peerMu.Unlock()
	svc.eventBus = newStormBus(t)

	epoch := uint64(3)
	v3e := protocol.RouteAnnounceV3Entry{Identity: peer.Address, SeqNo: 1}
	sigBytes := ed25519.Sign(peer.PrivateKey, v3e.CanonicalSigningBytes())
	sigBytes[0] ^= 0xff // corrupt
	frame := protocol.RouteAnnounceV3Frame{
		Kind:  protocol.RouteAnnounceV3KindFull,
		Epoch: epoch,
		Entries: []protocol.RouteAnnounceV3Entry{
			{Identity: peer.Address, Hops: 1, SeqNo: 1, Sig: base64.StdEncoding.EncodeToString(sigBytes)},
		},
	}
	svc.handleRouteAnnounceV3(domain.PeerIdentityFromWire(peer.Address), senderAddr, frame)

	if got := svc.routingTable.Lookup(domain.PeerIdentityFromWire(peer.Address)); len(got) > 0 {
		t.Fatalf("invalid-signature entry must NOT reach storage; got %d entries", len(got))
	}
}

// TestHandleRouteAnnounceV3_NoAttestedCapPreservesInvalidSigInformational
// pins the Phase 4 P2 contract for the "v3 cap negotiated but
// mesh_attested_links_v1 NOT negotiated" branch: incoming sig bytes
// MUST be preserved through storage (so downstream peers with the cap
// can still verify), the entry MUST NOT be dropped on
// ed25519-invalid (we never ran the verifier locally — the bytes are
// informational to us), and AttestedSigVerified MUST stay false (so
// CompositeScore does not award the signed-route bonus based on a
// locally-unverified sig). Without this gate the receiver would
// either drop legitimate-but-locally-unverifiable entries or
// silently award trust bonus without negotiation, both of which
// violate docs/protocol/attested_links.md "Capability negotiation".
func TestHandleRouteAnnounceV3_NoAttestedCapPreservesInvalidSigInformational(t *testing.T) {
	svc, _ := newTestServiceWithIdentity(t)
	peer, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	registerKnownPubKey(t, svc, peer.Address, peer.PublicKey)
	svc.announceLoop.StateRegistry().MarkReconnected(domain.PeerIdentityFromWire(peer.Address),
		[]routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3})
	// Wire a session WITHOUT mesh_attested_links_v1 — only v1+v3+relay.
	// peerSupportsAttestedLinks must return false → verifier skipped.
	senderAddr := domain.PeerAddress("addr-peer-no-attested")
	svc.peerMu.Lock()
	svc.sessions[senderAddr] = &peerSession{
		address:      senderAddr,
		peerIdentity: domain.PeerIdentityFromWire(peer.Address),
		capabilities: []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3, domain.CapMeshRelayV1},
	}
	svc.peerMu.Unlock()
	svc.eventBus = newStormBus(t)

	// Construct an entry with a CORRUPT signature. If the verifier ran,
	// it would reject and drop the entry. With the cap not negotiated,
	// the verifier is skipped and the entry must reach storage with the
	// (locally unverified) sig bytes preserved.
	epoch := uint64(3)
	v3e := protocol.RouteAnnounceV3Entry{Identity: peer.Address, SeqNo: 1}
	sigBytes := ed25519.Sign(peer.PrivateKey, v3e.CanonicalSigningBytes())
	sigBytes[0] ^= 0xff // corrupt
	frame := protocol.RouteAnnounceV3Frame{
		Kind:  protocol.RouteAnnounceV3KindFull,
		Epoch: epoch,
		Entries: []protocol.RouteAnnounceV3Entry{
			{Identity: peer.Address, Hops: 1, SeqNo: 1, Sig: base64.StdEncoding.EncodeToString(sigBytes)},
		},
	}
	svc.handleRouteAnnounceV3(domain.PeerIdentityFromWire(peer.Address), senderAddr, frame)

	got := svc.routingTable.Lookup(domain.PeerIdentityFromWire(peer.Address))
	if len(got) == 0 {
		t.Fatalf("entry must reach storage when attested-links cap is not negotiated (verifier is skipped)")
	}
	if string(got[0].AttestedSig) != string(sigBytes) {
		t.Fatalf("sig bytes must be preserved verbatim for downstream verification; got %x want %x", got[0].AttestedSig, sigBytes)
	}
	if got[0].AttestedSigVerified {
		t.Fatalf("AttestedSigVerified must stay false without local verification — CompositeScore would otherwise award trust bonus on an unverified sig")
	}
}
