package routing

import (
	"bytes"
	"testing"
	"time"
)

// attested_sig_roundtrip_test.go pins the Phase 4 13.2-A storage
// round-trip for the attested-links signature: the bytes survive the
// RouteEntry → UplinkClaim → RouteEntry → AnnounceEntry conversion
// chain unchanged so a v3 re-emit can forward the original signer's
// payload verbatim. Signing and verification land in 13.2-B; this file
// only proves the wiring.

func TestToUplinkClaim_PreservesAttestedSig(t *testing.T) {
	sig := []byte{0x01, 0x02, 0x03, 0x04}
	entry := RouteEntry{
		Identity:    PeerIdentity("aa00000000000000000000000000000000000001"),
		Origin:      PeerIdentity("bb00000000000000000000000000000000000002"),
		NextHop:     PeerIdentity("bb00000000000000000000000000000000000002"),
		Hops:        2,
		SeqNo:       7,
		Source:      RouteSourceAnnouncement,
		AttestedSig: sig,
	}
	claim := toUplinkClaim(entry, time.Unix(0, 0))
	if !bytes.Equal(claim.AttestedSig, sig) {
		t.Fatalf("toUplinkClaim dropped AttestedSig: got %x want %x", claim.AttestedSig, sig)
	}
}

func TestToRouteEntry_PreservesAttestedSig(t *testing.T) {
	sig := []byte{0x05, 0x06, 0x07}
	claim := UplinkClaim{
		Uplink:      PeerIdentity("bb00000000000000000000000000000000000002"),
		Hops:        2,
		SeqNo:       7,
		Source:      RouteSourceAnnouncement,
		AttestedSig: sig,
	}
	got := toRouteEntry(
		PeerIdentity("aa00000000000000000000000000000000000001"),
		PeerIdentity("cc00000000000000000000000000000000000003"),
		claim,
	)
	if !bytes.Equal(got.AttestedSig, sig) {
		t.Fatalf("toRouteEntry dropped AttestedSig: got %x want %x", got.AttestedSig, sig)
	}
}

func TestRouteEntry_ToAnnounceEntry_PreservesAttestedSig(t *testing.T) {
	sig := []byte{0xde, 0xad, 0xbe, 0xef}
	e := RouteEntry{
		Identity:    PeerIdentity("aa00000000000000000000000000000000000001"),
		Origin:      PeerIdentity("bb00000000000000000000000000000000000002"),
		NextHop:     PeerIdentity("bb00000000000000000000000000000000000002"),
		Hops:        1,
		SeqNo:       3,
		Source:      RouteSourceAnnouncement,
		AttestedSig: sig,
	}
	ae := e.ToAnnounceEntry()
	if !bytes.Equal(ae.AttestedSig, sig) {
		t.Fatalf("ToAnnounceEntry dropped AttestedSig: got %x want %x", ae.AttestedSig, sig)
	}
}

func TestToUplinkClaim_NilAttestedSigStaysNil(t *testing.T) {
	// Legacy v1/v2 ingest paths produce RouteEntry with no AttestedSig.
	// Storage must not synthesise a non-nil slice (allocation waste).
	entry := RouteEntry{
		Identity: PeerIdentity("aa00000000000000000000000000000000000001"),
		Origin:   PeerIdentity("bb00000000000000000000000000000000000002"),
		NextHop:  PeerIdentity("bb00000000000000000000000000000000000002"),
		Hops:     1,
		SeqNo:    1,
		Source:   RouteSourceAnnouncement,
	}
	claim := toUplinkClaim(entry, time.Unix(0, 0))
	if claim.AttestedSig != nil {
		t.Fatalf("nil AttestedSig must round-trip as nil; got %x", claim.AttestedSig)
	}
}
