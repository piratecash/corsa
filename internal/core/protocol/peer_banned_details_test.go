package protocol

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestMarshalPeerBannedDetails_RoundTripsUTC pins the wire contract:
// the emitted Until is UTC RFC3339, independent of the input timezone,
// so a dialler in any location parses it unambiguously.
func TestMarshalPeerBannedDetails_RoundTripsUTC(t *testing.T) {
	t.Parallel()

	// Construct the time in a non-UTC zone to make sure the encoder
	// normalises it. If the zone leaks into the wire, the round-trip
	// assertion below catches it.
	tz, err := time.LoadLocation("Europe/Kyiv")
	if err != nil {
		t.Fatalf("load timezone: %v", err)
	}
	local := time.Date(2026, 4, 20, 15, 4, 5, 0, tz)

	raw, err := MarshalPeerBannedDetails(local, PeerBannedReasonBlacklisted)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if raw == nil {
		t.Fatal("expected non-nil details for non-zero until")
	}

	// Wire shape — until must be UTC, reason must survive as-is.
	expectUntil := local.UTC().Format(time.RFC3339)
	if !strings.Contains(string(raw), `"until":"`+expectUntil+`"`) {
		t.Fatalf("until not normalised to UTC: %s", string(raw))
	}
	if !strings.Contains(string(raw), `"reason":"blacklisted"`) {
		t.Fatalf("reason missing from wire: %s", string(raw))
	}

	_, parsed, err := ParsePeerBannedDetails(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !parsed.Equal(local) {
		t.Fatalf("round-trip mismatch: got %v, want %v", parsed, local)
	}
	if parsed.Location() != time.UTC {
		t.Fatalf("parsed time must be UTC, got %v", parsed.Location())
	}
}

// TestMarshalPeerBannedDetails_OmitsWhenEmpty guards the omitempty contract
// so a notice with nothing to say serialises as just {"code":"peer-banned"}
// rather than {"code":"peer-banned","details":{}}. Frame.Details is
// json.RawMessage; if the helper hands back nil the outer Frame drops the
// key entirely via its own omitempty.
func TestMarshalPeerBannedDetails_OmitsWhenEmpty(t *testing.T) {
	t.Parallel()

	raw, err := MarshalPeerBannedDetails(time.Time{}, "")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if raw != nil {
		t.Fatalf("expected nil raw for empty input, got %s", string(raw))
	}
}

// TestParsePeerBannedDetails_EmptyInputIsOK ensures the minimal-notice
// shape is legal. A peer may receive {"type":"connection_notice","code":"peer-banned"}
// with no details — it must not error out, and the returned until is
// zero so the dialler falls back to its local default cap.
func TestParsePeerBannedDetails_EmptyInputIsOK(t *testing.T) {
	t.Parallel()

	details, until, err := ParsePeerBannedDetails(nil)
	if err != nil {
		t.Fatalf("parse empty: %v", err)
	}
	if !until.IsZero() {
		t.Fatalf("expected zero until for empty input, got %v", until)
	}
	if details.Until != "" || details.Reason != "" {
		t.Fatalf("expected zero details, got %+v", details)
	}
}

// TestParsePeerBannedDetails_RejectsMalformedUntil prevents a buggy or
// hostile responder from shoving an unparseable string through the notice
// path. The caller must see an error so it can fall back to a local
// default rather than silently recording a zero ban time.
func TestParsePeerBannedDetails_RejectsMalformedUntil(t *testing.T) {
	t.Parallel()

	raw := json.RawMessage(`{"until":"not-a-timestamp"}`)
	_, until, err := ParsePeerBannedDetails(raw)
	if err == nil {
		t.Fatal("expected parse error on malformed until")
	}
	if !until.IsZero() {
		t.Fatalf("expected zero until on parse error, got %v", until)
	}
}

// TestErrorFromCode_PeerBanned rounds the sentinel through the code →
// error → code table so errors.Is is reliable on the dialler side. Without
// this mapping a pre-welcome reject path that wraps ErrPeerBanned could
// not be distinguished from a generic protocol error by downstream
// handlers.
func TestErrorFromCode_PeerBanned(t *testing.T) {
	t.Parallel()

	err := ErrorFromCode(ErrCodePeerBanned)
	if !errors.Is(err, ErrPeerBanned) {
		t.Fatalf("ErrorFromCode(%q) should satisfy errors.Is(ErrPeerBanned)", ErrCodePeerBanned)
	}
	if got := ErrorCode(err); got != ErrCodePeerBanned {
		t.Fatalf("round-trip lost: ErrorCode(ErrorFromCode(%q)) = %q", ErrCodePeerBanned, got)
	}
}
