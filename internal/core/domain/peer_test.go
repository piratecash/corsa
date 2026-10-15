package domain

import (
	"encoding/json"
	"testing"
)

// TestPeerPort_IsValid pins the boundary contract: only 1..65535 is
// valid; zero, negatives, and >65535 are explicit "absent / invalid"
// sentinels consumed by the config.DefaultPeerPort fallback path.
func TestPeerPort_IsValid(t *testing.T) {
	cases := []struct {
		name  string
		value PeerPort
		want  bool
	}{
		{name: "zero_invalid", value: 0, want: false},
		{name: "negative_invalid", value: -1, want: false},
		{name: "one_valid", value: 1, want: true},
		{name: "default_peer_port_valid", value: 64646, want: true},
		{name: "upper_bound_valid", value: 65535, want: true},
		{name: "above_upper_bound_invalid", value: 65536, want: false},
		{name: "large_invalid", value: 1 << 30, want: false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.value.IsValid(); got != tc.want {
				t.Fatalf("PeerPort(%d).IsValid() = %v, want %v", tc.value, got, tc.want)
			}
		})
	}
}

// TestPeerPort_UnmarshalJSON_NumberShape verifies the canonical wire
// shape: a JSON integer decodes as-is. This is the only shape any
// supported peer writes.
func TestPeerPort_UnmarshalJSON_NumberShape(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want PeerPort
	}{
		{name: "valid_low_bound", raw: `1`, want: 1},
		{name: "default_peer_port", raw: `64646`, want: 64646},
		{name: "valid_high_bound", raw: `65535`, want: 65535},
		{name: "out_of_range_kept_verbatim", raw: `70000`, want: 70000},
		{name: "zero", raw: `0`, want: 0},
		{name: "negative", raw: `-5`, want: -5},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var p PeerPort
			if err := json.Unmarshal([]byte(tc.raw), &p); err != nil {
				t.Fatalf("json.Unmarshal(%q): %v", tc.raw, err)
			}
			if p != tc.want {
				t.Fatalf("PeerPort from %q = %d, want %d", tc.raw, p, tc.want)
			}
		})
	}
}

// TestPeerPort_UnmarshalJSON_UnsupportedShapes asserts that every
// non-integer payload collapses silently to zero rather than failing the
// decode. This is what keeps a single malformed advertise_port from
// rejecting an otherwise valid Frame on the wire. A JSON string is one of
// those shapes even when it spells a valid port: advertise_port has only
// ever been written as a JSON integer, so a quoted number is not a
// compatibility form to honour but a malformed value like any other.
func TestPeerPort_UnmarshalJSON_UnsupportedShapes(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{name: "null_value", raw: `null`},
		{name: "boolean_true", raw: `true`},
		{name: "boolean_false", raw: `false`},
		{name: "fractional_number", raw: `64646.5`},
		{name: "object", raw: `{"port":64646}`},
		{name: "array", raw: `[64646]`},
		{name: "numeric_string", raw: `"64646"`},
		{name: "low_bound_string", raw: `"1"`},
		{name: "high_bound_string", raw: `"65535"`},
		{name: "negative_string", raw: `"-5"`},
		{name: "empty_string", raw: `""`},
		{name: "non_numeric_string", raw: `"abc"`},
		{name: "fractional_string", raw: `"64646.5"`},
		{name: "whitespace_string", raw: `"   "`},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := PeerPort(42) // prefilled — must be overwritten to zero.
			if err := json.Unmarshal([]byte(tc.raw), &p); err != nil {
				t.Fatalf("json.Unmarshal(%q): %v", tc.raw, err)
			}
			if p != 0 {
				t.Fatalf("PeerPort from %q = %d, want 0", tc.raw, p)
			}
			if p.IsValid() {
				t.Fatalf("PeerPort from %q reported IsValid=true; invalid wire values must never validate", tc.raw)
			}
		})
	}
}

// TestPeerPort_Marshal_RoundTrip pins the canonical wire shape on the
// writer side: PeerPort marshals as a JSON integer, and the emitted form
// round-trips through the lenient UnmarshalJSON to the same value, so a
// port one node writes is the port its peer reads back.
func TestPeerPort_Marshal_RoundTrip(t *testing.T) {
	t.Parallel()
	values := []PeerPort{1, 64646, 65535}
	for _, v := range values {
		v := v
		t.Run(encodeForName(v), func(t *testing.T) {
			t.Parallel()
			raw, err := json.Marshal(v)
			if err != nil {
				t.Fatalf("json.Marshal(%d): %v", v, err)
			}
			// Writer side must emit a JSON integer, not a JSON string.
			if len(raw) == 0 || raw[0] == '"' {
				t.Fatalf("PeerPort(%d) marshaled as non-integer %s; the wire contract requires a JSON integer", v, raw)
			}
			var got PeerPort
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatalf("round-trip unmarshal: %v", err)
			}
			if got != v {
				t.Fatalf("round-trip value: got %d want %d", got, v)
			}
		})
	}
}

func encodeForName(p PeerPort) string {
	raw, _ := json.Marshal(p)
	return string(raw)
}
