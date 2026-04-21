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

// TestNoticeErrorFromFrame_SelfIdentityReasonResolvesToSentinel pins the
// critical reason-aware mapping: a connection_notice{peer-banned} whose
// Details carry reason="self-identity" must decode to ErrSelfIdentity (not
// ErrPeerBanned). The dialler's cooldown dispatcher (onCMDialFailed)
// routes self-identity failures into a 24h address cooldown, while a
// generic ErrPeerBanned goes through the advertise-mismatch path. A plain
// ErrorFromCode(frame.Code) lookup would collapse both into ErrPeerBanned
// and the dialler would keep hammering its own reflected endpoint.
func TestNoticeErrorFromFrame_SelfIdentityReasonResolvesToSentinel(t *testing.T) {
	t.Parallel()

	raw, err := MarshalPeerBannedDetails(time.Time{}, PeerBannedReasonSelfIdentity)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	frame := Frame{
		Type:    FrameTypeConnectionNotice,
		Code:    ErrCodePeerBanned,
		Details: raw,
	}

	got := NoticeErrorFromFrame(frame)
	if !errors.Is(got, ErrSelfIdentity) {
		t.Fatalf("NoticeErrorFromFrame(peer-banned, self-identity) = %v; want errors.Is(ErrSelfIdentity)", got)
	}
	// Reason-aware mapping must NOT leave the frame errors.Is-matching
	// ErrPeerBanned too — otherwise the onCMDialFailed switch would hit
	// whichever arm came first and the discrimination would be order-
	// dependent. ErrSelfIdentity and ErrPeerBanned are siblings.
	if errors.Is(got, ErrPeerBanned) {
		t.Fatalf("self-identity reason must NOT also satisfy errors.Is(ErrPeerBanned); got %v", got)
	}
}

// TestNoticeErrorFromFrame_PeerBanReasonFallsBackToBanned pins the
// default branch: reasons other than self-identity (peer-ban, blacklisted,
// or any future/unknown value) must decode to ErrPeerBanned so the
// dialler walks the standard ban-respect path. This is the contract the
// pre-self-identity release depended on; the new mapping must not break
// the established reason when it is the one that surfaced.
func TestNoticeErrorFromFrame_PeerBanReasonFallsBackToBanned(t *testing.T) {
	t.Parallel()

	cases := []PeerBannedReason{
		PeerBannedReasonPeerBan,
		PeerBannedReasonBlacklisted,
		PeerBannedReason("unknown-forward-compat-reason"),
	}

	for _, reason := range cases {
		reason := reason
		t.Run(string(reason), func(t *testing.T) {
			t.Parallel()

			raw, err := MarshalPeerBannedDetails(time.Time{}, reason)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			frame := Frame{
				Type:    FrameTypeConnectionNotice,
				Code:    ErrCodePeerBanned,
				Details: raw,
			}

			got := NoticeErrorFromFrame(frame)
			if !errors.Is(got, ErrPeerBanned) {
				t.Fatalf("NoticeErrorFromFrame(peer-banned, %q) = %v; want errors.Is(ErrPeerBanned)", reason, got)
			}
			if errors.Is(got, ErrSelfIdentity) {
				t.Fatalf("non-self-identity reason %q must NOT satisfy errors.Is(ErrSelfIdentity); got %v", reason, got)
			}
		})
	}
}

// TestNoticeErrorFromFrame_MinimalPeerBannedFallsBackToBanned pins the
// minimal wire shape: {code:"peer-banned"} with no Details at all must
// decode to ErrPeerBanned. Without this, a legacy responder emitting the
// older minimal form would be treated as "unknown" and drop off the
// ban-honour path.
func TestNoticeErrorFromFrame_MinimalPeerBannedFallsBackToBanned(t *testing.T) {
	t.Parallel()

	frame := Frame{
		Type: FrameTypeConnectionNotice,
		Code: ErrCodePeerBanned,
	}

	got := NoticeErrorFromFrame(frame)
	if !errors.Is(got, ErrPeerBanned) {
		t.Fatalf("NoticeErrorFromFrame(minimal peer-banned) = %v; want errors.Is(ErrPeerBanned)", got)
	}
}

// TestNoticeErrorFromFrame_MalformedDetailsDegradesToBanned covers the
// hostile/buggy responder path: if Details is not valid JSON the outer
// Code is still trustworthy, so the caller must at least learn
// "peer-banned" rather than silently switching to a different branch.
// This prevents a malformed self-identity claim from bypassing the
// ban-respect path entirely and hammering the endpoint.
func TestNoticeErrorFromFrame_MalformedDetailsDegradesToBanned(t *testing.T) {
	t.Parallel()

	frame := Frame{
		Type:    FrameTypeConnectionNotice,
		Code:    ErrCodePeerBanned,
		Details: json.RawMessage(`{not-valid-json`),
	}

	got := NoticeErrorFromFrame(frame)
	if !errors.Is(got, ErrPeerBanned) {
		t.Fatalf("NoticeErrorFromFrame(malformed details) = %v; want errors.Is(ErrPeerBanned)", got)
	}
	if errors.Is(got, ErrSelfIdentity) {
		t.Fatalf("malformed details must NOT surface ErrSelfIdentity; got %v", got)
	}
}

// TestNoticeErrorFromFrame_NonPeerBannedCodeFallsThrough guards the
// code-gate at the top of NoticeErrorFromFrame: for any code other than
// peer-banned the helper must defer to ErrorFromCode and NOT try to
// parse Details as peer-banned payload. A notice with
// code="rate-limited" must surface ErrRateLimited exactly as
// ErrorFromCode would, regardless of whether Details contains unrelated
// bytes.
func TestNoticeErrorFromFrame_NonPeerBannedCodeFallsThrough(t *testing.T) {
	t.Parallel()

	frame := Frame{
		Type: FrameTypeConnectionNotice,
		Code: ErrCodeRateLimited,
	}

	got := NoticeErrorFromFrame(frame)
	if !errors.Is(got, ErrRateLimited) {
		t.Fatalf("NoticeErrorFromFrame(rate-limited) = %v; want errors.Is(ErrRateLimited)", got)
	}
	if errors.Is(got, ErrPeerBanned) || errors.Is(got, ErrSelfIdentity) {
		t.Fatalf("non-peer-banned code must NOT leak peer-banned/self-identity sentinels; got %v", got)
	}
}
