package protocol

import (
	"encoding/json"
	"reflect"
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
	// reflect.DeepEqual rather than != : RouteSyncDigestFrame carries the
	// Entries slice (Phase-0 version vector) and slices are not comparable.
	if !reflect.DeepEqual(got, orig) {
		t.Fatalf("round-trip mismatch: got %+v, want %+v", got, orig)
	}
}

// TestRouteSyncDigestFrame_RoundTripWithVector verifies the Phase-0
// version vector survives marshal -> unmarshal unchanged. Entries is the
// additive carrier of the (Identity, SeqNo) pairs the Digest summarises.
func TestRouteSyncDigestFrame_RoundTripWithVector(t *testing.T) {
	in := RouteSyncDigestFrame{
		Digest:               "deadbeef",
		KnownIdentitiesCount: 2,
		GeneratedAt:          "2026-05-23T12:00:00Z",
		Entries: []RouteSyncDigestEntry{
			{Identity: "aa11", SeqNo: 7},
			{Identity: "bb22", SeqNo: 9},
		},
	}
	raw, err := MarshalRouteSyncDigestFrame(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out, err := UnmarshalRouteSyncDigestFrame(raw)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(out.Entries) != 2 ||
		out.Entries[0].Identity != "aa11" || out.Entries[0].SeqNo != 7 ||
		out.Entries[1].Identity != "bb22" || out.Entries[1].SeqNo != 9 {
		t.Fatalf("entries round-trip mismatch: %+v", out.Entries)
	}
	// Compact JSON keys keep the per-identity vector small on the wire.
	if !strings.Contains(string(raw), `"i":"aa11"`) || !strings.Contains(string(raw), `"s":7`) {
		t.Fatalf("expected compact entry keys i/s in wire form: %s", raw)
	}
}

// TestRouteSyncDigestFrame_BackCompatNoVector pins the additive contract:
// a legacy frame (no "entries" key) parses with a nil vector, and a
// vector-less frame omits the key entirely (omitempty) so the wire shape
// is byte-identical to the pre-Phase-0 form.
func TestRouteSyncDigestFrame_BackCompatNoVector(t *testing.T) {
	legacy := `{"type":"route_sync_digest_v1","digest":"cafe","known_identities_count":3,"generated_at":"2026-05-23T12:00:00Z"}`
	out, err := UnmarshalRouteSyncDigestFrame([]byte(legacy))
	if err != nil {
		t.Fatalf("unmarshal legacy: %v", err)
	}
	if out.Entries != nil {
		t.Fatalf("entries = %+v, want nil for a legacy frame", out.Entries)
	}

	raw, err := MarshalRouteSyncDigestFrame(RouteSyncDigestFrame{
		Digest: "cafe", KnownIdentitiesCount: 3, GeneratedAt: "2026-05-23T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(raw), "entries") {
		t.Fatalf("vector-less frame must omit the entries key entirely: %s", raw)
	}
}

// TestRouteSyncDigestEntry_JSONShape guards the exact compact wire keys
// other implementations must agree on.
func TestRouteSyncDigestEntry_JSONShape(t *testing.T) {
	raw, err := json.Marshal(RouteSyncDigestEntry{Identity: "ff", SeqNo: 42})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if got, want := string(raw), `{"i":"ff","s":42}`; got != want {
		t.Fatalf("entry JSON = %s, want %s", got, want)
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
