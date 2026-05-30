package protocol

import (
	"bytes"
	"strings"
	"testing"
)

// frame_route_poison_test.go covers the Phase 4 explicit poison-
// reverse wire frame helpers (phase-4 §3.3, overview §7.7):
// marshal/unmarshal round-trip with auto-Type-fill, reason
// validation on both directions, identity-required guard, and the
// canonical sender-sig bytes determinism contract.

func TestRoutePoisonFrame_MarshalUnmarshalRoundTrip(t *testing.T) {
	orig := RoutePoisonFrame{
		Identity:  "aaaa",
		Reason:    RoutePoisonReasonUplinkLost,
		IssuedAt:  "2026-05-28T12:00:00Z",
		SenderSig: "c2ln",
	}
	data, err := MarshalRoutePoisonFrame(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := UnmarshalRoutePoisonFrame(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Type != RoutePoisonFrameType {
		t.Fatalf("Type not auto-filled: got %q", got.Type)
	}
	if got.Identity != orig.Identity || got.Reason != orig.Reason || got.IssuedAt != orig.IssuedAt || got.SenderSig != orig.SenderSig {
		t.Fatalf("round-trip mismatch: got %+v want %+v", got, orig)
	}
}

func TestRoutePoisonFrame_AllReasonsRoundTrip(t *testing.T) {
	reasons := []string{
		RoutePoisonReasonUplinkLost,
		RoutePoisonReasonHealthDead,
		RoutePoisonReasonLoopDetected,
	}
	for _, r := range reasons {
		t.Run(r, func(t *testing.T) {
			data, err := MarshalRoutePoisonFrame(RoutePoisonFrame{Identity: "x", Reason: r, IssuedAt: "t"})
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			got, err := UnmarshalRoutePoisonFrame(data)
			if err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if got.Reason != r {
				t.Fatalf("reason: got %q want %q", got.Reason, r)
			}
		})
	}
}

func TestRoutePoisonFrame_MarshalRejectsInvalidReason(t *testing.T) {
	if _, err := MarshalRoutePoisonFrame(RoutePoisonFrame{Identity: "x", Reason: ""}); err == nil {
		t.Fatal("expected error for empty reason on marshal")
	}
	if _, err := MarshalRoutePoisonFrame(RoutePoisonFrame{Identity: "x", Reason: "bogus"}); err == nil {
		t.Fatal("expected error for unknown reason on marshal")
	}
}

func TestRoutePoisonFrame_MarshalRejectsEmptyIdentity(t *testing.T) {
	if _, err := MarshalRoutePoisonFrame(RoutePoisonFrame{Identity: "", Reason: RoutePoisonReasonUplinkLost}); err == nil {
		t.Fatal("expected error for empty identity on marshal")
	}
}

func TestRoutePoisonFrame_UnmarshalRejectsWrongType(t *testing.T) {
	if _, err := UnmarshalRoutePoisonFrame([]byte(`{"type":"route_announce_v3","identity":"x","reason":"uplink_lost"}`)); err == nil {
		t.Fatal("expected error for wrong frame type")
	}
}

func TestRoutePoisonFrame_UnmarshalRejectsInvalidReason(t *testing.T) {
	if _, err := UnmarshalRoutePoisonFrame([]byte(`{"type":"route_poison_v1","identity":"x","reason":"bogus"}`)); err == nil {
		t.Fatal("expected error for invalid reason on unmarshal")
	}
}

func TestRoutePoisonFrame_UnmarshalRejectsEmptyIdentity(t *testing.T) {
	if _, err := UnmarshalRoutePoisonFrame([]byte(`{"type":"route_poison_v1","identity":"","reason":"uplink_lost"}`)); err == nil {
		t.Fatal("expected error for empty identity on unmarshal")
	}
}

// TestRoutePoisonFrame_CanonicalSenderSigBytesDeterministic asserts
// the canonical bytes are reproducible: identical fields → identical
// bytes (so signer and verifier agree).
func TestRoutePoisonFrame_CanonicalSenderSigBytesDeterministic(t *testing.T) {
	f := RoutePoisonFrame{Identity: "deadbeef", Reason: RoutePoisonReasonHealthDead, IssuedAt: "2026-05-28T12:00:00Z"}
	b1 := f.CanonicalSenderSigBytes()
	b2 := f.CanonicalSenderSigBytes()
	if !bytes.Equal(b1, b2) {
		t.Fatalf("canonical bytes not deterministic: %x vs %x", b1, b2)
	}
	// SenderSig must NOT influence the canonical bytes — that would
	// make the signature self-referential and unverifiable.
	f2 := f
	f2.SenderSig = "should-not-matter"
	if !bytes.Equal(b1, f2.CanonicalSenderSigBytes()) {
		t.Fatal("SenderSig leaked into canonical bytes")
	}
}

// TestRoutePoisonFrame_CanonicalSenderSigBytesFieldSeparation asserts
// the length-prefixing prevents field-boundary collisions: shifting
// a byte between adjacent fields changes the canonical bytes.
func TestRoutePoisonFrame_CanonicalSenderSigBytesFieldSeparation(t *testing.T) {
	a := RoutePoisonFrame{Identity: "ab", Reason: RoutePoisonReasonUplinkLost, IssuedAt: "t"}
	b := RoutePoisonFrame{Identity: "a", Reason: RoutePoisonReasonUplinkLost, IssuedAt: "bt"}
	if bytes.Equal(a.CanonicalSenderSigBytes(), b.CanonicalSenderSigBytes()) {
		t.Fatal("length-prefixing failed: identity/issued_at boundary collided")
	}
}

func TestRoutePoisonFrame_OmitsSenderSigOnWireWhenEmpty(t *testing.T) {
	// omitempty on SenderSig keeps the wire shape clean for unsigned
	// frames (sender-identity-via-session still binds the message).
	data, err := MarshalRoutePoisonFrame(RoutePoisonFrame{
		Identity: "aaaa", Reason: RoutePoisonReasonUplinkLost, IssuedAt: "t",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(data), "sender_sig") {
		t.Fatalf("empty SenderSig must omit the field; got %s", data)
	}
}
