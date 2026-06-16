package protocol

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// frame_route_poison_v2_test.go covers the batched poison-reverse wire
// frame: round-trip with auto-Type-fill, validation (empty/oversized
// list, empty element, bad reason, wrong type), and the order-independent
// canonical sender-sig bytes contract.

func TestRoutePoisonV2Frame_MarshalUnmarshalRoundTrip(t *testing.T) {
	in := RoutePoisonV2Frame{
		Identities: []string{"aa", "bb", "cc"},
		Reason:     RoutePoisonReasonUplinkLost,
		IssuedAt:   "2026-06-15T15:00:00Z",
		SenderSig:  "sig",
	}
	raw, err := MarshalRoutePoisonV2Frame(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out, err := UnmarshalRoutePoisonV2Frame(raw)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Type != RoutePoisonV2FrameType {
		t.Errorf("Type = %q, want %q", out.Type, RoutePoisonV2FrameType)
	}
	if strings.Join(out.Identities, ",") != "aa,bb,cc" || out.Reason != in.Reason || out.IssuedAt != in.IssuedAt || out.SenderSig != in.SenderSig {
		t.Errorf("round-trip mismatch: %+v", out)
	}
}

func TestRoutePoisonV2Frame_Validation(t *testing.T) {
	base := RoutePoisonV2Frame{Identities: []string{"aa"}, Reason: RoutePoisonReasonUplinkLost, IssuedAt: "t"}

	if _, err := MarshalRoutePoisonV2Frame(RoutePoisonV2Frame{Reason: RoutePoisonReasonUplinkLost}); err == nil {
		t.Error("empty identities must be rejected")
	}
	bad := base
	bad.Identities = []string{"aa", ""}
	if _, err := MarshalRoutePoisonV2Frame(bad); err == nil {
		t.Error("empty identity element must be rejected")
	}
	bad = base
	bad.Reason = "totally-made-up"
	if _, err := MarshalRoutePoisonV2Frame(bad); err == nil {
		t.Error("invalid reason must be rejected")
	}
	big := make([]string, maxRoutePoisonV2Identities+1)
	for i := range big {
		big[i] = "x"
	}
	if _, err := MarshalRoutePoisonV2Frame(RoutePoisonV2Frame{Identities: big, Reason: RoutePoisonReasonUplinkLost}); err == nil {
		t.Error("oversized identity list must be rejected")
	}

	// Wrong type on unmarshal.
	wrong, _ := json.Marshal(map[string]any{"type": "route_poison_v1", "identities": []string{"aa"}, "reason": RoutePoisonReasonUplinkLost})
	if _, err := UnmarshalRoutePoisonV2Frame(wrong); err == nil {
		t.Error("wrong type must be rejected on unmarshal")
	}
}

func TestRoutePoisonV2Frame_CanonicalSigOrderIndependent(t *testing.T) {
	a := RoutePoisonV2Frame{Identities: []string{"cc", "aa", "bb"}, Reason: RoutePoisonReasonUplinkLost, IssuedAt: "t"}
	b := RoutePoisonV2Frame{Identities: []string{"aa", "bb", "cc"}, Reason: RoutePoisonReasonUplinkLost, IssuedAt: "t"}
	if !bytes.Equal(a.CanonicalSenderSigBytes(), b.CanonicalSenderSigBytes()) {
		t.Error("canonical sig bytes must be independent of identity order (sorted)")
	}

	// Different identity set must produce different canonical bytes.
	c := RoutePoisonV2Frame{Identities: []string{"aa", "bb", "dd"}, Reason: RoutePoisonReasonUplinkLost, IssuedAt: "t"}
	if bytes.Equal(a.CanonicalSenderSigBytes(), c.CanonicalSenderSigBytes()) {
		t.Error("different identity sets must produce different canonical bytes")
	}

	// Reason / issued_at participate in the canonical bytes.
	d := RoutePoisonV2Frame{Identities: []string{"aa", "bb", "cc"}, Reason: RoutePoisonReasonHealthDead, IssuedAt: "t"}
	if bytes.Equal(b.CanonicalSenderSigBytes(), d.CanonicalSenderSigBytes()) {
		t.Error("reason must participate in canonical bytes")
	}
}

func TestRoutePoisonV2Frame_OmitsSenderSigWhenEmpty(t *testing.T) {
	raw, err := MarshalRoutePoisonV2Frame(RoutePoisonV2Frame{Identities: []string{"aa"}, Reason: RoutePoisonReasonUplinkLost, IssuedAt: "t"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(raw), "sender_sig") {
		t.Errorf("empty sender_sig must be omitted on the wire: %s", raw)
	}
}
