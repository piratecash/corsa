package protocol

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// TestMarshalFrameLineWithLimit_OversizeReturnsErrFrameTooLarge proves
// the writer-side wire-size guard rejects frames whose JSON form
// exceeds the supplied budget and that the returned error wraps
// ErrFrameTooLarge so callers can dispatch on it via errors.Is.
// Without this guard, upstream layers that build oversize
// announce_routes / peers responses would push them on the wire, the
// receiver's maxCommandLineBytes guard would close the connection,
// and both sides would end up in a reconnect loop.
func TestMarshalFrameLineWithLimit_OversizeReturnsErrFrameTooLarge(t *testing.T) {
	// Body alone large enough that the encoded form blows past
	// MaxFrameLine even after JSON envelope fits around it.
	frame := Frame{Type: "test", Body: strings.Repeat("x", MaxFrameLine+1)}
	_, err := MarshalFrameLineWithLimit(frame, MaxFrameLine)
	if err == nil {
		t.Fatal("MarshalFrameLineWithLimit: expected ErrFrameTooLarge for oversize frame, got nil")
	}
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("MarshalFrameLineWithLimit: expected errors.Is(err, ErrFrameTooLarge), got %v", err)
	}
}

// TestMarshalFrameLineWithLimit_AtLimitSucceeds proves a frame whose
// serialized form fits inside the budget passes without error.
// Bookend test for the oversize path — without it, a regression that
// always rejects would silently disable the announce plane and look
// like a reconnect fix.
//
// The test computes the JSON envelope overhead exactly so the
// boundary condition is enforced byte-for-byte: with len(data)+1 ==
// MaxFrameLine the call must succeed, and one extra byte must flip it
// to ErrFrameTooLarge.
func TestMarshalFrameLineWithLimit_AtLimitSucceeds(t *testing.T) {
	// Determine the envelope size empirically. The probe MUST carry a
	// non-empty Body — Frame.Body is `json:",omitempty"`, so probing
	// with Body=="" silently drops the `"body":"..."` field and
	// underestimates the envelope by len(`,"body":""`) bytes. Probe
	// with a single-byte body and subtract that byte to get the true
	// fixed envelope cost.
	probe, err := json.Marshal(Frame{Type: "test", Body: "x"})
	if err != nil {
		t.Fatalf("envelope probe marshal: %v", err)
	}
	envelopeLen := len(probe) - 1 // subtract the single body byte
	// Budget for body: MaxFrameLine - envelopeLen - 1 (newline).
	bodyLen := MaxFrameLine - envelopeLen - 1
	if bodyLen <= 0 {
		t.Fatalf("envelope %d already exceeds MaxFrameLine %d", envelopeLen, MaxFrameLine)
	}
	frame := Frame{Type: "test", Body: strings.Repeat("x", bodyLen)}
	line, err := MarshalFrameLineWithLimit(frame, MaxFrameLine)
	if err != nil {
		t.Fatalf("MarshalFrameLineWithLimit: unexpected error for at-limit frame: %v", err)
	}
	if len(line) != MaxFrameLine {
		t.Fatalf("at-limit line length %d, expected exactly %d (envelope+body+\\n)", len(line), MaxFrameLine)
	}
	// One extra body byte must cross the budget.
	overFrame := Frame{Type: "test", Body: strings.Repeat("x", bodyLen+1)}
	if _, err := MarshalFrameLineWithLimit(overFrame, MaxFrameLine); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("MarshalFrameLineWithLimit: one byte over MaxFrameLine must fail with ErrFrameTooLarge, got %v", err)
	}
}

// TestMarshalFrameLineWithLimit_NewlineOffByOne pins the contract
// that the budget is computed against the FULL wire line including
// the trailing newline. A JSON payload whose len(data) equals maxBytes
// exactly would produce a wire line of maxBytes+1 once the newline is
// appended — which the receive-side reader rejects — so the writer
// guard must reject it too.
func TestMarshalFrameLineWithLimit_NewlineOffByOne(t *testing.T) {
	// Same omitempty trap as in the at-limit test: probe with a
	// non-empty body and subtract the single byte we contributed.
	probe, err := json.Marshal(Frame{Type: "test", Body: "x"})
	if err != nil {
		t.Fatalf("envelope probe marshal: %v", err)
	}
	envelopeLen := len(probe) - 1
	bodyLen := MaxFrameLine - envelopeLen // len(data) == MaxFrameLine, line == MaxFrameLine+1
	if bodyLen <= 0 {
		t.Fatalf("envelope %d already exceeds MaxFrameLine %d", envelopeLen, MaxFrameLine)
	}
	frame := Frame{Type: "test", Body: strings.Repeat("x", bodyLen)}
	// Reuse probe/err (already declared above for the envelope probe);
	// `:=` here would fail with no-new-variables. Verifying the actual
	// JSON length lands exactly on MaxFrameLine pins the off-by-one
	// boundary the test is designed to enforce.
	probe, err = json.Marshal(frame)
	if err != nil {
		t.Fatalf("probe marshal: %v", err)
	}
	if len(probe) != MaxFrameLine {
		t.Fatalf("probe length %d, expected exactly MaxFrameLine %d for off-by-one boundary", len(probe), MaxFrameLine)
	}
	if _, err := MarshalFrameLineWithLimit(frame, MaxFrameLine); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("frame with len(data)==MaxFrameLine must be rejected (wire line would be MaxFrameLine+1), got %v", err)
	}
}

// TestMarshalFrameLineWithLimit_RawLineSizeChecked pins the documented
// contract that the WithLimit entry point also size-checks the
// RawLine fast-path. RawLine still hits the same wire budget at the
// remote — only the JSON marshal step is skipped.
func TestMarshalFrameLineWithLimit_RawLineSizeChecked(t *testing.T) {
	huge := strings.Repeat("x", MaxFrameLine+128)
	frame := Frame{RawLine: huge}
	if _, err := MarshalFrameLineWithLimit(frame, MaxFrameLine); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("MarshalFrameLineWithLimit: oversize RawLine must be rejected, got %v", err)
	}
	small := strings.Repeat("x", MaxFrameLine-1)
	frame = Frame{RawLine: small}
	out, err := MarshalFrameLineWithLimit(frame, MaxFrameLine)
	if err != nil {
		t.Fatalf("MarshalFrameLineWithLimit: under-limit RawLine must pass, got %v", err)
	}
	if out != small {
		t.Fatal("MarshalFrameLineWithLimit: under-limit RawLine must be returned verbatim")
	}
}

// TestMarshalFrameLine_RawLineFastPath_NoSizeCheck pins the legacy
// contract for the unguarded MarshalFrameLine entry point: it does
// not enforce any size budget on RawLine. Callers that need the
// budget must use MarshalFrameLineWithLimit.
func TestMarshalFrameLine_RawLineFastPath_NoSizeCheck(t *testing.T) {
	huge := strings.Repeat("x", MaxFrameLine+128)
	frame := Frame{RawLine: huge}
	out, err := MarshalFrameLine(frame)
	if err != nil {
		t.Fatalf("MarshalFrameLine: RawLine fast-path must not enforce size, got error: %v", err)
	}
	if out != huge {
		t.Fatal("MarshalFrameLine: RawLine fast-path must return RawLine verbatim")
	}
}

// TestMarshalFrameLine_NoSizeGuard documents the legacy entry point's
// contract: oversize JSON-marshalled frames are NOT rejected. Callers
// that need a size budget must opt in via MarshalFrameLineWithLimit —
// the unguarded form exists so generic infrastructure (e.g.
// netcore.NetCore.Send) can serialize without knowing whether the
// caller is on the command or response plane.
func TestMarshalFrameLine_NoSizeGuard(t *testing.T) {
	frame := Frame{Type: "test", Body: strings.Repeat("x", MaxFrameLine+1)}
	line, err := MarshalFrameLine(frame)
	if err != nil {
		t.Fatalf("MarshalFrameLine: unguarded entry point must not enforce size, got %v", err)
	}
	if len(line) <= MaxFrameLine {
		t.Fatalf("MarshalFrameLine: probe line %d should exceed MaxFrameLine %d", len(line), MaxFrameLine)
	}
}

// TestMaxFrameLine_MatchesAdmission documents the cross-package
// invariant that MaxFrameLine MUST equal the receive-side
// maxCommandLineBytes constant. The actual cross-check lives in
// node/admission_test.go (it can see both); this local test pins the
// numeric value so a careless edit to MaxFrameLine fails here too.
func TestMaxFrameLine_MatchesAdmission(t *testing.T) {
	const expected = 128 * 1024
	if MaxFrameLine != expected {
		t.Fatalf("MaxFrameLine = %d, want %d (must equal admission.maxCommandLineBytes)", MaxFrameLine, expected)
	}
}

// TestMaxResponseLine_MatchesAdmission documents the cross-package
// invariant that MaxResponseLine MUST equal the receive-side
// maxResponseLineBytes constant. Like MaxFrameLine, the actual
// cross-check lives in node/admission_test.go; this local test pins
// the numeric value so a careless edit to MaxResponseLine fails here
// too.
func TestMaxResponseLine_MatchesAdmission(t *testing.T) {
	const expected = 8 * 1024 * 1024
	if MaxResponseLine != expected {
		t.Fatalf("MaxResponseLine = %d, want %d (must equal admission.maxResponseLineBytes)", MaxResponseLine, expected)
	}
}

// jsonRoundTrip is a sanity helper: we keep a marshal-decode cycle so
// the test file stays self-contained even if external helpers move.
func jsonRoundTrip(t *testing.T, frame Frame) Frame {
	t.Helper()
	data, err := json.Marshal(frame)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	var out Frame
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	return out
}

// TestMarshalFrameLine_RoundTripUnderLimit guards against silent
// envelope-shape drift: a frame whose body fits under the limit must
// round-trip cleanly through Marshal+Unmarshal so dispatchers can
// rely on byte-for-byte equality of the decoded shape.
func TestMarshalFrameLine_RoundTripUnderLimit(t *testing.T) {
	frame := Frame{Type: "test", Body: "hello"}
	out := jsonRoundTrip(t, frame)
	if out.Type != frame.Type || out.Body != frame.Body {
		t.Fatalf("round-trip mismatch: %+v vs %+v", out, frame)
	}
}
