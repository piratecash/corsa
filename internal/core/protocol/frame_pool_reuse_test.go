package protocol

import (
	"bytes"
	"testing"
)

// NOTE ON FRAME SHAPES. Two distinct announce wire shapes exist in this code:
//
//   - LEGACY (announce_routes / routes_update): a generic Frame with a "routes"
//     array → Frame.AnnounceRoutes []AnnounceRouteFrame, parsed by the
//     universal ParseFrameLine. This is the path that exercises the
//     frameLineBufPool scratch-buffer reuse, so the pool-safety tests below use
//     this shape deliberately.
//
//   - V3 (route_announce_v3): a RawLine-backed frame with an "entries" array →
//     RouteAnnounceV3Entry, decoded by UnmarshalRouteAnnounceV3Frame from
//     []byte(frame.RawLine) — NOT through frameLineBufPool. Its verbatim-Extra
//     contract is pinned separately by TestUnmarshalRouteAnnounceV3Frame_*.
//
// Keeping the fixtures honest avoids a false impression that the pool tests
// cover RouteAnnounceV3Entry.Extra; they do not — that is a different decoder.

// TestParseFrameLine_PoolReuseDoesNotAliasLegacyExtra pins the safety contract
// of the frameLineBufPool optimisation in ParseFrameLine: the recycled scratch
// buffer that backs json.Unmarshal's input must never be aliased into a parsed
// frame.
//
// Legacy AnnounceRouteFrame.Extra is the most exposed surface on this path — a
// json.RawMessage built by AnnounceRouteFrame.UnmarshalJSON. The standard
// library copies RawMessage out of the input via append, so Extra owns
// independent memory; this test guards against a future regression (here or in
// stdlib) that would let Extra alias the pooled buffer, which a subsequent
// ParseFrameLine would then overwrite in place.
//
// The second frame is sized to fit within the same pooled capacity as the
// first so that ParseFrameLine's `append((*bufp)[:0], line...)` reuses the
// SAME backing array and overwrites it byte-for-byte. A larger second frame
// would force append to reallocate, leaving the original array untouched and
// silently weakening the test.
func TestParseFrameLine_PoolReuseDoesNotAliasLegacyExtra(t *testing.T) {
	// Frame #1: a legacy announce_routes frame with an unknown field
	// ("anchor"), which AnnounceRouteFrame.UnmarshalJSON captures verbatim into
	// Extra.
	line1 := `{"type":"announce_routes","routes":[{"identity":"dest","origin":"src","hops":2,"seq":5,"anchor":"keepme"}]}`
	frame1, err := ParseFrameLine(line1)
	if err != nil {
		t.Fatalf("parse line1: %v", err)
	}
	if len(frame1.AnnounceRoutes) != 1 {
		t.Fatalf("line1: want 1 route, got %d", len(frame1.AnnounceRoutes))
	}
	gotExtra := frame1.AnnounceRoutes[0].Extra
	if len(gotExtra) == 0 {
		t.Fatalf("line1: expected Extra to be populated for the unknown 'anchor' field")
	}

	// Independent reference snapshot of the bytes that must remain stable, plus
	// the known fields we also expect to survive any later parse.
	wantExtra := append([]byte(nil), gotExtra...)
	wantIdentity := frame1.AnnounceRoutes[0].Identity
	wantOrigin := frame1.AnnounceRoutes[0].Origin

	// Frame #2: same structure and comparable length, but every variable byte
	// differs from frame #1 (all 'X'). Parsing it recycles the pooled buffer
	// and overwrites the backing array in place. Driven several times so a
	// buggy alias is overwritten with certainty rather than by luck.
	line2 := `{"type":"announce_routes","routes":[{"identity":"XXXX","origin":"XXXX","hops":1,"seq":1,"anchor":"XXXXXX"}]}`
	if len(line2) < len(line1) {
		t.Fatalf("test setup: line2 (%d) must be >= line1 (%d) to overwrite the pooled buffer in place",
			len(line2), len(line1))
	}
	for i := 0; i < 16; i++ {
		f2, err := ParseFrameLine(line2)
		if err != nil {
			t.Fatalf("parse line2 (iter %d): %v", i, err)
		}
		if len(f2.AnnounceRoutes) != 1 {
			t.Fatalf("line2 (iter %d): want 1 route, got %d", i, len(f2.AnnounceRoutes))
		}
	}

	if !bytes.Equal(frame1.AnnounceRoutes[0].Extra, wantExtra) {
		t.Fatalf("frame1 Extra mutated after pool reuse:\n got %q\nwant %q",
			frame1.AnnounceRoutes[0].Extra, wantExtra)
	}
	if frame1.AnnounceRoutes[0].Identity != wantIdentity || frame1.AnnounceRoutes[0].Origin != wantOrigin {
		t.Fatalf("frame1 known fields mutated after pool reuse: identity=%q origin=%q",
			frame1.AnnounceRoutes[0].Identity, frame1.AnnounceRoutes[0].Origin)
	}
}

// TestParseFrameLine_PoolReuseAcrossGrowth complements the in-place test above
// by exercising the buffer-growth path: a small legacy frame followed by a
// large one (forcing append to reallocate), then a small one again. All three
// frames are retained and their distinguishing content cross-checked, so any
// alias of the pooled buffer into a retained frame would surface as
// cross-contamination.
func TestParseFrameLine_PoolReuseAcrossGrowth(t *testing.T) {
	small := `{"type":"announce_routes","routes":[{"identity":"aaaa","origin":"bbbb","hops":2,"seq":7,"anchor":"small"}]}`

	var big bytes.Buffer
	big.WriteString(`{"type":"announce_routes","routes":[`)
	for i := 0; i < 300; i++ {
		if i > 0 {
			big.WriteByte(',')
		}
		big.WriteString(`{"identity":"cccccccccc","origin":"dddddddddd","hops":4,"seq":42,"flood":"eeeeeeeeeeeeeeee"}`)
	}
	big.WriteString(`]}`)

	f1, err := ParseFrameLine(small)
	if err != nil {
		t.Fatalf("parse small #1: %v", err)
	}
	keep1 := append([]byte(nil), f1.AnnounceRoutes[0].Extra...)

	fBig, err := ParseFrameLine(big.String())
	if err != nil {
		t.Fatalf("parse big: %v", err)
	}
	if len(fBig.AnnounceRoutes) != 300 {
		t.Fatalf("big: want 300 routes, got %d", len(fBig.AnnounceRoutes))
	}

	f2, err := ParseFrameLine(small)
	if err != nil {
		t.Fatalf("parse small #2: %v", err)
	}

	if !bytes.Equal(f1.AnnounceRoutes[0].Extra, keep1) {
		t.Fatalf("small #1 Extra mutated across growth:\n got %q\nwant %q",
			f1.AnnounceRoutes[0].Extra, keep1)
	}
	if string(f2.AnnounceRoutes[0].Extra) != `{"anchor":"small"}` {
		t.Fatalf("small #2 Extra wrong: got %q", f2.AnnounceRoutes[0].Extra)
	}
	// The big frame's first entry must still carry its own 'flood' field, not
	// anything bled in from a recycled small-frame buffer.
	if !bytes.Contains(fBig.AnnounceRoutes[0].Extra, []byte(`"flood"`)) {
		t.Fatalf("big frame Extra lost its own content: %q", fBig.AnnounceRoutes[0].Extra)
	}
}

// TestUnmarshalRouteAnnounceV3Frame_ExtraOwnsItsBytes pins the verbatim-Extra
// contract on the REAL v3 receive path. This decoder does not use
// frameLineBufPool — the pool tests above cover the legacy path — but the v3
// path is the production churn path (route_announce_v3_delta), and multi-hop
// transit re-emit relies on RouteAnnounceV3Entry.Extra preserving the origin's
// exact bytes (the Sig is computed over them, see CanonicalSigningBytes). So a
// regression that let Extra alias the caller's input buffer would be a
// correctness bug, not just an allocation one.
//
// The guard: decode from a mutable []byte, capture Extra, then scribble over
// the entire source buffer. If Extra aliased the input, the captured bytes
// would change.
func TestUnmarshalRouteAnnounceV3Frame_ExtraOwnsItsBytes(t *testing.T) {
	src := []byte(`{"type":"route_announce_v3","kind":"delta","epoch":7,"entries":[` +
		`{"identity":"dest","hops":2,"seq_no":41,"extra":{"anchor":"keepme"}}]}`)

	f, err := UnmarshalRouteAnnounceV3Frame(src)
	if err != nil {
		t.Fatalf("unmarshal v3: %v", err)
	}
	if len(f.Entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(f.Entries))
	}
	gotExtra := f.Entries[0].Extra
	if len(gotExtra) == 0 {
		t.Fatalf("expected v3 entry Extra to be populated")
	}
	wantExtra := append([]byte(nil), gotExtra...)
	wantIdentity := f.Entries[0].Identity

	// Overwrite the entire source buffer. A correct decoder copied Extra out
	// (json.RawMessage.UnmarshalJSON appends), so this must not touch it.
	for i := range src {
		src[i] = 'Z'
	}

	if !bytes.Equal(f.Entries[0].Extra, wantExtra) {
		t.Fatalf("v3 entry Extra aliased the input buffer:\n got %q\nwant %q",
			f.Entries[0].Extra, wantExtra)
	}
	if f.Entries[0].Identity != wantIdentity {
		t.Fatalf("v3 entry Identity aliased the input buffer: got %q want %q",
			f.Entries[0].Identity, wantIdentity)
	}
	if string(f.Entries[0].Extra) != `{"anchor":"keepme"}` {
		t.Fatalf("v3 entry Extra content wrong: got %q", f.Entries[0].Extra)
	}
}
