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
// shape: a JSON integer decodes as-is. This is what a conformant v11
// peer emits and what v11 itself writes.
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

// TestPeerPort_UnmarshalJSON_StringCompat covers the lenient v10-style
// compatibility path: a JSON string containing a decimal integer must
// decode successfully rather than failing the whole frame decode. Out-of-
// range numeric strings still parse — the caller applies IsValid to gate
// the fallback.
func TestPeerPort_UnmarshalJSON_StringCompat(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want PeerPort
	}{
		{name: "valid_string", raw: `"64646"`, want: 64646},
		{name: "low_bound_string", raw: `"1"`, want: 1},
		{name: "high_bound_string", raw: `"65535"`, want: 65535},
		{name: "zero_string", raw: `"0"`, want: 0},
		{name: "negative_string", raw: `"-5"`, want: -5},
		{name: "empty_string_collapses_to_zero", raw: `""`, want: 0},
		{name: "non_numeric_collapses_to_zero", raw: `"abc"`, want: 0},
		{name: "fractional_string_collapses_to_zero", raw: `"64646.5"`, want: 0},
		{name: "whitespace_string_collapses_to_zero", raw: `"   "`, want: 0},
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
// non-integer, non-string payload collapses silently to zero rather than
// failing the decode. This is what keeps a single malformed advertise_port
// from rejecting an otherwise valid Frame on the wire.
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
// round-trips through the lenient UnmarshalJSON to the same value.
// This proves v11 ↔ v11 wire compatibility when the emitted port is
// parsed back by another v11 node.
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
				t.Fatalf("PeerPort(%d) marshaled as non-integer %s; v11 wire contract requires a JSON integer", v, raw)
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
