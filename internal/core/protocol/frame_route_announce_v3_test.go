package protocol

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// frame_route_announce_v3_test.go covers the Phase 4 compact announce
// wire frame helpers (phase-4 §3.1, overview §7.1): marshal/unmarshal
// round-trip with auto-Type-fill, kind validation on both directions,
// and the canonical signing-bytes contract that the attested-links
// signer/verifier (phase-4 §3.2) share.

func TestRouteAnnounceV3Frame_MarshalUnmarshalRoundTrip(t *testing.T) {
	orig := RouteAnnounceV3Frame{
		Kind:     RouteAnnounceV3KindFull,
		Epoch:    7,
		IssuedAt: "2026-05-27T12:00:00Z",
		Entries: []RouteAnnounceV3Entry{
			{Identity: "aaaa", Hops: 2, SeqNo: 41, Extra: json.RawMessage(`{"k":"v"}`), Sig: "c2ln"},
			{Identity: "bbbb", Hops: 0, SeqNo: 1},
		},
	}

	// Type left blank on purpose — Marshal must fill it from the constant.
	data, err := MarshalRouteAnnounceV3Frame(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	got, err := UnmarshalRouteAnnounceV3Frame(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Type != RouteAnnounceV3FrameType {
		t.Fatalf("Type not auto-filled: got %q", got.Type)
	}
	if got.Kind != orig.Kind || got.Epoch != orig.Epoch || got.IssuedAt != orig.IssuedAt {
		t.Fatalf("header mismatch: got %+v want kind=%q epoch=%d issued=%q", got, orig.Kind, orig.Epoch, orig.IssuedAt)
	}
	if len(got.Entries) != len(orig.Entries) {
		t.Fatalf("entries len: got %d want %d", len(got.Entries), len(orig.Entries))
	}
	for i := range orig.Entries {
		o, g := orig.Entries[i], got.Entries[i]
		if g.Identity != o.Identity || g.Hops != o.Hops || g.SeqNo != o.SeqNo || g.Sig != o.Sig {
			t.Fatalf("entry %d field mismatch: got %+v want %+v", i, g, o)
		}
		if string(g.Extra) != string(o.Extra) {
			t.Fatalf("entry %d Extra mismatch: got %q want %q", i, g.Extra, o.Extra)
		}
	}
}

func TestRouteAnnounceV3Frame_DeltaKindRoundTrip(t *testing.T) {
	data, err := MarshalRouteAnnounceV3Frame(RouteAnnounceV3Frame{Kind: RouteAnnounceV3KindDelta, Epoch: 1})
	if err != nil {
		t.Fatalf("marshal delta: %v", err)
	}
	got, err := UnmarshalRouteAnnounceV3Frame(data)
	if err != nil {
		t.Fatalf("unmarshal delta: %v", err)
	}
	if got.Kind != RouteAnnounceV3KindDelta {
		t.Fatalf("kind: got %q want %q", got.Kind, RouteAnnounceV3KindDelta)
	}
}

func TestRouteAnnounceV3Frame_MarshalRejectsInvalidKind(t *testing.T) {
	if _, err := MarshalRouteAnnounceV3Frame(RouteAnnounceV3Frame{Kind: ""}); err == nil {
		t.Fatal("expected error for empty kind on marshal")
	}
	if _, err := MarshalRouteAnnounceV3Frame(RouteAnnounceV3Frame{Kind: "snapshot"}); err == nil {
		t.Fatal("expected error for unknown kind on marshal")
	}
}

func TestRouteAnnounceV3Frame_UnmarshalRejectsWrongType(t *testing.T) {
	if _, err := UnmarshalRouteAnnounceV3Frame([]byte(`{"type":"announce_routes","kind":"full"}`)); err == nil {
		t.Fatal("expected error for wrong frame type")
	}
}

func TestRouteAnnounceV3Frame_UnmarshalRejectsInvalidKind(t *testing.T) {
	if _, err := UnmarshalRouteAnnounceV3Frame([]byte(`{"type":"route_announce_v3","kind":"bogus"}`)); err == nil {
		t.Fatal("expected error for invalid kind on unmarshal")
	}
}

// TestRouteAnnounceV3Entry_CanonicalSigningBytesDeterministic asserts the
// canonical bytes are reproducible: the same fields must yield identical
// bytes across calls (so signer and verifier agree).
func TestRouteAnnounceV3Entry_CanonicalSigningBytesDeterministic(t *testing.T) {
	e := RouteAnnounceV3Entry{Identity: "deadbeef", Hops: 3, SeqNo: 99, Extra: json.RawMessage(`{"a":1}`)}
	b1 := e.CanonicalSigningBytes()
	b2 := e.CanonicalSigningBytes()
	if !bytes.Equal(b1, b2) {
		t.Fatalf("canonical bytes not deterministic: %x vs %x", b1, b2)
	}
	// Sig is not part of the canonical bytes — adding it must not change them.
	e2 := e
	e2.Sig = "should-not-matter"
	if !bytes.Equal(b1, e2.CanonicalSigningBytes()) {
		t.Fatal("Sig leaked into canonical signing bytes")
	}
}

// TestRouteAnnounceV3Entry_CanonicalSigningBytesFieldSeparation asserts
// the length-prefixing prevents field-boundary collisions: ("ab"+"") and
// ("a"+"b") for (identity, extra) must not collide. Both fields are
// length-prefixed so a byte shift between them produces distinct bytes.
func TestRouteAnnounceV3Entry_CanonicalSigningBytesFieldSeparation(t *testing.T) {
	a := RouteAnnounceV3Entry{Identity: "ab"}
	b := RouteAnnounceV3Entry{Identity: "a", Extra: json.RawMessage("b")}
	if bytes.Equal(a.CanonicalSigningBytes(), b.CanonicalSigningBytes()) {
		t.Fatal("length-prefixing failed: identity/extra boundary collided")
	}

	base := RouteAnnounceV3Entry{Identity: "x"}
	extra := RouteAnnounceV3Entry{Identity: "x", Extra: json.RawMessage(`{"k":"v"}`)}
	if bytes.Equal(base.CanonicalSigningBytes(), extra.CanonicalSigningBytes()) {
		t.Fatal("extra must be bound into canonical bytes")
	}
}

// TestRouteAnnounceV3Entry_CanonicalSigningBytesIgnoresPerEmitterFields
// pins the Phase 4 design decision that hops, epoch, AND seq_no — all
// per-emitter wire fields — are excluded from canonical signing bytes.
// Every transit emit can restamp these fields independently of the
// origin's signed payload (hops by receiver +1 convention, epoch by
// each sender's local table-generation counter, seq_no by
// nextOutboundSeqLockedPerPeer / Broadcast monotonicity guard). A
// signature over any of them would break the moment transit re-emits
// at a different value — see CanonicalSigningBytes doc-comment and
// docs/protocol/attested_links.md "Canonical signing bytes" for the
// full rationale.
func TestRouteAnnounceV3Entry_CanonicalSigningBytesIgnoresPerEmitterFields(t *testing.T) {
	base := RouteAnnounceV3Entry{Identity: "x"}
	hopsVaried := RouteAnnounceV3Entry{Identity: "x", Hops: 7}
	if !bytes.Equal(base.CanonicalSigningBytes(), hopsVaried.CanonicalSigningBytes()) {
		t.Fatal("hops must NOT influence canonical signing bytes (multi-hop sig verification requirement)")
	}
	seqVaried := RouteAnnounceV3Entry{Identity: "x", SeqNo: 9999}
	if !bytes.Equal(base.CanonicalSigningBytes(), seqVaried.CanonicalSigningBytes()) {
		t.Fatal("seq_no must NOT influence canonical signing bytes — transit re-emit synthesises a different wire seq_no while forwarding AttestedSig verbatim")
	}
	// Epoch is per-emitter and is NOT a parameter of CanonicalSigningBytes
	// — the API itself enforces the exclusion. This compile-time guard
	// pins the function signature contract.
	_ = canonicalRouteAnnounceV3Bytes("x", nil)
}

// TestRouteAnnounceV3Frame_OmitsOriginOnWire is the compact-format
// guarantee: the v3 wire bytes must never carry an "origin" key (the
// whole point of the frame — overview §7.1).
func TestRouteAnnounceV3Frame_OmitsOriginOnWire(t *testing.T) {
	data, err := MarshalRouteAnnounceV3Frame(RouteAnnounceV3Frame{
		Kind:    RouteAnnounceV3KindFull,
		Entries: []RouteAnnounceV3Entry{{Identity: "aaaa", Hops: 1, SeqNo: 1}},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(data), "origin") {
		t.Fatalf("v3 frame leaked an origin field: %s", data)
	}
}
