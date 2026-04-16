package domain

import (
	"net/netip"
	"testing"
)

func TestParseCaptureFormat(t *testing.T) {
	tests := []struct {
		input   string
		want    CaptureFormat
		wantOK  bool
	}{
		{"", CaptureFormatCompact, true},
		{"compact", CaptureFormatCompact, true},
		{"COMPACT", CaptureFormatCompact, true},
		{"pretty", CaptureFormatPretty, true},
		{"Pretty", CaptureFormatPretty, true},
		{"  pretty  ", CaptureFormatPretty, true},
		{"unknown", "", false},
		{"json", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, ok := ParseCaptureFormat(tt.input)
			if ok != tt.wantOK {
				t.Errorf("ParseCaptureFormat(%q) ok=%v, want %v", tt.input, ok, tt.wantOK)
			}
			if got != tt.want {
				t.Errorf("ParseCaptureFormat(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestSanitizeCapturePathComponent(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"simple", "simple"},
		{"with:colon", "with-colon"},
		{"with/slash", "withslash"},
		{"with\\backslash", "withbackslash"},
		{"with%percent", "with-percent"},
		{"trailing..", "trailing"},
		{"  spaces  ", "spaces"},
		{"", "unknown"},
		{"\x00\x01\x02", "unknown"},
		{"2026-04-14T12:44:03Z", "2026-04-14T12-44-03Z"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := SanitizeCapturePathComponent(tt.input)
			if got != tt.want {
				t.Errorf("SanitizeCapturePathComponent(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestFormatIPForFilename(t *testing.T) {
	tests := []struct {
		name string
		addr netip.Addr
		want string
	}{
		{
			name: "ipv4",
			addr: netip.MustParseAddr("203.0.113.10"),
			want: "203.0.113.10",
		},
		{
			name: "ipv6",
			addr: netip.MustParseAddr("2001:db8::1"),
			want: "2001-db8--1",
		},
		{
			name: "ipv6 with zone stripped",
			addr: netip.MustParseAddr("fe80::1%eth0"),
			want: "fe80--1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatIPForFilename(tt.addr)
			if got != tt.want {
				t.Errorf("FormatIPForFilename(%s) = %q, want %q", tt.addr, got, tt.want)
			}
		})
	}
}

func TestWireDirection_IsValid(t *testing.T) {
	if !WireDirectionIn.IsValid() {
		t.Error("expected WireDirectionIn to be valid")
	}
	if !WireDirectionOut.IsValid() {
		t.Error("expected WireDirectionOut to be valid")
	}
	if WireDirection("unknown").IsValid() {
		t.Error("expected unknown WireDirection to be invalid")
	}
}

func TestSendOutcome_IsFailed(t *testing.T) {
	if SendOutcomeSent.IsFailed() {
		t.Error("sent should not be failed")
	}
	if !SendOutcomeWriteFailed.IsFailed() {
		t.Error("write_failed should be failed")
	}
}

func TestCaptureScope_IsValid(t *testing.T) {
	if !CaptureScopeConnID.IsValid() {
		t.Error("conn_id scope should be valid")
	}
	if !CaptureScopeIP.IsValid() {
		t.Error("ip scope should be valid")
	}
	if !CaptureScopeAll.IsValid() {
		t.Error("all scope should be valid")
	}
	if CaptureScope("custom").IsValid() {
		t.Error("custom scope should be invalid")
	}
}

func TestPayloadKind_IsValid(t *testing.T) {
	valid := []PayloadKind{
		PayloadKindJSON, PayloadKindInvalidJSON, PayloadKindNonJSON,
		PayloadKindFrameTooLarge, PayloadKindMalformed,
	}
	for _, k := range valid {
		if !k.IsValid() {
			t.Errorf("expected %s to be valid", k)
		}
	}
	if PayloadKind("custom").IsValid() {
		t.Error("custom kind should be invalid")
	}
}
