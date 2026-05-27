package protocol

import (
	"strings"
	"testing"
)

// frame_route_sync_test.go covers the Phase 3 PR 12.5 wire
// frame helpers. Roundtrip + auto-Type-fill + reject-wrong-type
// + reject-empty-digest mirror the Phase 2 probe/query frame
// tests one-for-one so future readers see the same shape.

func TestRouteSyncDigestFrame_MarshalUnmarshalRoundTrip(t *testing.T) {
	orig := RouteSyncDigestFrame{
		Type:                 RouteSyncDigestFrameType,
		Digest:               "deadbeefcafef00d",
		KnownIdentitiesCount: 42,
		GeneratedAt:          "2026-05-23T12:00:00Z",
	}
	data, err := MarshalRouteSyncDigestFrame(orig)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := UnmarshalRouteSyncDigestFrame(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got != orig {
		t.Fatalf("round-trip mismatch: got %+v, want %+v", got, orig)
	}
}

func TestRouteSyncDigestFrame_MarshalSetsTypeWhenEmpty(t *testing.T) {
	orig := RouteSyncDigestFrame{Digest: "abc"}
	data, err := MarshalRouteSyncDigestFrame(orig)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !strings.Contains(string(data), `"type":"`+RouteSyncDigestFrameType+`"`) {
		t.Fatalf("marshalled bytes do not contain expected type: %s", data)
	}
}

func TestRouteSyncDigestFrame_UnmarshalRejectsWrongType(t *testing.T) {
	bad := []byte(`{"type":"announce_routes","digest":"x"}`)
	if _, err := UnmarshalRouteSyncDigestFrame(bad); err == nil {
		t.Fatal("Unmarshal accepted wrong type, want error")
	}
}

// TestRouteSyncDigestFrame_UnmarshalRejectsEmptyDigest — without
// a digest the receiver has nothing to compare against, so the
// helper rejects the frame outright. The dispatch handler in
// service.go would otherwise reach Table.SyncDigestFor and emit
// a summary against an unknown digest, leaving the sender unable
// to act on the response.
func TestRouteSyncDigestFrame_UnmarshalRejectsEmptyDigest(t *testing.T) {
	bad := []byte(`{"type":"` + RouteSyncDigestFrameType + `","known_identities_count":1}`)
	if _, err := UnmarshalRouteSyncDigestFrame(bad); err == nil {
		t.Fatal("Unmarshal accepted empty digest, want error")
	}
}

func TestRouteSyncSummaryFrame_MarshalUnmarshalRoundTrip(t *testing.T) {
	orig := RouteSyncSummaryFrame{
		Type:           RouteSyncSummaryFrameType,
		Digest:         "deadbeefcafef00d",
		Match:          true,
		ExpectFullSync: false,
	}
	data, err := MarshalRouteSyncSummaryFrame(orig)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := UnmarshalRouteSyncSummaryFrame(data)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got != orig {
		t.Fatalf("round-trip mismatch: got %+v, want %+v", got, orig)
	}
}

func TestRouteSyncSummaryFrame_MarshalSetsTypeWhenEmpty(t *testing.T) {
	orig := RouteSyncSummaryFrame{Digest: "abc", Match: false}
	data, err := MarshalRouteSyncSummaryFrame(orig)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !strings.Contains(string(data), `"type":"`+RouteSyncSummaryFrameType+`"`) {
		t.Fatalf("marshalled bytes do not contain expected type: %s", data)
	}
}

func TestRouteSyncSummaryFrame_UnmarshalRejectsWrongType(t *testing.T) {
	bad := []byte(`{"type":"announce_routes","digest":"x","match":true}`)
	if _, err := UnmarshalRouteSyncSummaryFrame(bad); err == nil {
		t.Fatal("Unmarshal accepted wrong type, want error")
	}
}

// TestRouteSyncSummaryFrame_UnmarshalAllowsEmptyDigest — unlike the
// digest frame, the summary's Digest can legitimately be empty in
// future protocol revisions (the receive handler treats empty as
// "no correlation, drop the suppression update" — see
// UnmarshalRouteSyncSummaryFrame's doc-comment). This test pins
// that contract so a future cleanup pass does not silently tighten
// the helper.
func TestRouteSyncSummaryFrame_UnmarshalAllowsEmptyDigest(t *testing.T) {
	ok := []byte(`{"type":"` + RouteSyncSummaryFrameType + `","match":false}`)
	got, err := UnmarshalRouteSyncSummaryFrame(ok)
	if err != nil {
		t.Fatalf("Unmarshal of empty-digest summary failed: %v", err)
	}
	if got.Digest != "" {
		t.Fatalf("Digest = %q, want empty", got.Digest)
	}
}
