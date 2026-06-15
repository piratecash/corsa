package node

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// routing_announce_v3_sig_test.go pins the Phase 4 13.2-A wire round-
// trip for the attested-links signature field on route_announce_v3:
// build emits sig as base64; receive decodes base64 back into the
// stored claim. Verification lands in 13.2-B; this file only proves
// the opaque bytes survive the wire boundary.

func TestBuildRouteAnnounceV3Frame_EncodesAttestedSigAsBase64(t *testing.T) {
	sig := []byte{0xde, 0xad, 0xbe, 0xef, 0x00, 0x7f}
	entries := []routing.AnnounceEntry{
		{
			Identity:    idTargetX,
			Origin:      idOriginC,
			Hops:        1,
			SeqNo:       1,
			AttestedSig: sig,
		},
	}
	frame, err := buildRouteAnnounceV3Frame(protocol.RouteAnnounceV3KindFull, 7, entries)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	got, err := protocol.UnmarshalRouteAnnounceV3Frame([]byte(strings.TrimRight(frame.RawLine, "\n")))
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got.Entries) != 1 {
		t.Fatalf("entries: got %d want 1", len(got.Entries))
	}
	wantSig := base64.StdEncoding.EncodeToString(sig)
	if got.Entries[0].Sig != wantSig {
		t.Fatalf("wire Sig: got %q want %q (base64 of %x)", got.Entries[0].Sig, wantSig, sig)
	}
}

func TestBuildRouteAnnounceV3Frame_EmptyAttestedSigOmitsWireField(t *testing.T) {
	// omitempty on RouteAnnounceV3Entry.Sig must keep the Tier-2
	// unsigned-entry path off the wire so peers without
	// mesh_attested_links_v1 see no superfluous JSON key.
	entries := []routing.AnnounceEntry{
		{Identity: idTargetX, Origin: idOriginC, Hops: 1, SeqNo: 1},
	}
	frame, err := buildRouteAnnounceV3Frame(protocol.RouteAnnounceV3KindFull, 1, entries)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if strings.Contains(frame.RawLine, "\"sig\"") {
		t.Fatalf("empty AttestedSig must omit the sig field; got %s", frame.RawLine)
	}
}

func TestRouteAnnounceV3EntriesToWire_DecodesSigIntoParallelSlice(t *testing.T) {
	sig := []byte{0x01, 0x02, 0x03}
	v3Entries := []protocol.RouteAnnounceV3Entry{
		{Identity: idTargetX.String(), Hops: 1, SeqNo: 1, Sig: base64.StdEncoding.EncodeToString(sig)},
		{Identity: idPeerB.String(), Hops: 1, SeqNo: 1}, // unsigned
	}
	frames, sigs := routeAnnounceV3EntriesToWire(idOriginC, v3Entries)
	if len(frames) != 2 || len(sigs) != 2 {
		t.Fatalf("parallel slices: got frames=%d sigs=%d want both 2", len(frames), len(sigs))
	}
	if !bytes.Equal(sigs[0], sig) {
		t.Fatalf("sigs[0]: got %x want %x", sigs[0], sig)
	}
	if sigs[1] != nil {
		t.Fatalf("unsigned entry must produce nil sig slot; got %x", sigs[1])
	}
	// Frames must mirror entries with synthesised Origin.
	if frames[0].Identity != idTargetX.String() || frames[0].Origin != idOriginC.String() {
		t.Fatalf("frames[0] header wrong: %+v", frames[0])
	}
}

func TestRouteAnnounceV3EntriesToWire_MalformedBase64ProducesNilSig(t *testing.T) {
	// A malformed sig bytes payload must NOT crash ingest. The slot
	// stays nil (treated as unsigned) and the entry itself is still
	// ingested — the verification path will surface the absent-sig
	// case in 13.2-B via the trust-score penalty.
	v3Entries := []protocol.RouteAnnounceV3Entry{
		{Identity: idTargetX.String(), Hops: 1, SeqNo: 1, Sig: "not-valid-base64!!!"},
	}
	frames, sigs := routeAnnounceV3EntriesToWire(idOriginC, v3Entries)
	if len(frames) != 1 {
		t.Fatalf("frames: got %d want 1", len(frames))
	}
	if sigs[0] != nil {
		t.Fatalf("malformed sig must produce nil slot; got %x", sigs[0])
	}
}

func TestHandleRouteAnnounceV3_StoresAttestedSigOnClaim(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	svc.eventBus = newStormBus(t)

	registry := svc.announceLoop.StateRegistry()
	registry.MarkReconnected(idPeerB,
		[]routing.PeerCapability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3})

	sig := []byte{0xaa, 0xbb, 0xcc, 0xdd}
	frame := protocol.RouteAnnounceV3Frame{
		Kind:  protocol.RouteAnnounceV3KindFull,
		Epoch: 1,
		Entries: []protocol.RouteAnnounceV3Entry{
			{Identity: idTargetX.String(), Hops: 1, SeqNo: 1, Sig: base64.StdEncoding.EncodeToString(sig)},
		},
	}
	svc.handleRouteAnnounceV3(idPeerB, domain.PeerAddress("addr-peerB"), frame)

	// Pull the stored route back through the boundary and assert the
	// signature survived storage round-trip.
	got := svc.routingTable.Lookup(idTargetX)
	if len(got) == 0 {
		t.Fatalf("v3 entry not applied")
	}
	if !bytes.Equal(got[0].AttestedSig, sig) {
		t.Fatalf("stored AttestedSig: got %x want %x", got[0].AttestedSig, sig)
	}
}
