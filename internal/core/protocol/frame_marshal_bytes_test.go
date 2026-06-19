package protocol

import (
	"bytes"
	"testing"
)

// frame_marshal_bytes_test.go pins that MarshalFrameLineBytes produces exactly
// the same wire bytes as MarshalFrameLine's string — it is a copy-saving
// variant for the []byte senders, never a behavioural change.

func TestMarshalFrameLineBytes_MatchesString(t *testing.T) {
	cases := []Frame{
		{Type: "ok"},
		{Type: "error", Code: ErrCodeEncodeFailed, Error: "boom"},
		{Type: "auth_ok"},
		{Type: "get_peers"},
	}
	for _, frame := range cases {
		want, err := MarshalFrameLine(frame)
		if err != nil {
			t.Fatalf("MarshalFrameLine(%q) err=%v", frame.Type, err)
		}
		got, err := MarshalFrameLineBytes(frame)
		if err != nil {
			t.Fatalf("MarshalFrameLineBytes(%q) err=%v", frame.Type, err)
		}
		if !bytes.Equal(got, []byte(want)) {
			t.Fatalf("frame %q: bytes %q != string %q", frame.Type, got, want)
		}
		// The wire line must be newline-terminated, matching the reader's
		// line framing.
		if len(got) == 0 || got[len(got)-1] != '\n' {
			t.Fatalf("frame %q: wire line not newline-terminated: %q", frame.Type, got)
		}
	}
}

func TestMarshalFrameLineBytes_RawLinePassthrough(t *testing.T) {
	raw := "{\"type\":\"route_announce_v3\",\"kind\":\"delta\"}\n"
	frame := Frame{Type: "route_announce_v3", RawLine: raw}

	got, err := MarshalFrameLineBytes(frame)
	if err != nil {
		t.Fatalf("MarshalFrameLineBytes raw err=%v", err)
	}
	if string(got) != raw {
		t.Fatalf("raw passthrough: got %q want %q", got, raw)
	}
	// The returned slice must be caller-owned (a distinct backing array), so
	// mutating it cannot corrupt the source frame's RawLine string.
	if len(got) > 0 {
		got[0] = 'X'
		if frame.RawLine[0] == 'X' {
			t.Fatal("returned bytes alias the frame RawLine string backing")
		}
	}
}
