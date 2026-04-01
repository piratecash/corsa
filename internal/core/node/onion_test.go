package node

import (
	"strings"
	"testing"

	"corsa/internal/core/config"
	"corsa/internal/core/domain"
)

// validV3Onion is a 56-char base32 string used as a valid Tor v3 .onion host in tests.
const validV3Onion = "2gzyxa5ihm7nsber23gk5eqx3mp4wrymfbhqgk2ycdjp3yzcrllbiqad"

// validV2Onion is a 16-char base32 string used as a valid (deprecated) Tor v2 .onion host in tests.
const validV2Onion = "abcdefghijklmnop"

func TestIsOnionAddress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		host string
		want bool
	}{
		// Valid v3 (56 base32 chars)
		{validV3Onion + ".onion", true},
		{strings.ToUpper(validV3Onion) + ".ONION", true},
		// Valid v2 (16 base32 chars, deprecated but still accepted)
		{validV2Onion + ".onion", true},
		// Invalid: wrong length
		{"short.onion", false},
		{"abc123def456ghij.onion", false}, // 16 but has digits 0,1,8,9 invalid in base32
		{".onion", false},
		// Invalid: not .onion
		{"127.0.0.1", false},
		{"example.com", false},
		{"notonion", false},
		{"", false},
		{"onion", false},
		// Invalid: right length but invalid base32 chars (digit 0, 1, 8, 9)
		{strings.Repeat("0", 56) + ".onion", false},
		{strings.Repeat("a", 55) + "1" + ".onion", false},
		// Valid: all a's (valid base32)
		{strings.Repeat("a", 56) + ".onion", true},
		{strings.Repeat("a", 16) + ".onion", true},
		// Invalid: 17 chars (not 16 or 56)
		{strings.Repeat("a", 17) + ".onion", false},
	}
	for _, tt := range tests {
		if got := isOnionAddress(tt.host); got != tt.want {
			t.Errorf("isOnionAddress(%q) = %v, want %v", tt.host, got, tt.want)
		}
	}
}

func TestNormalizePeerAddressAcceptsValidOnion(t *testing.T) {
	t.Parallel()

	svc := &Service{
		cfg: config.Node{
			ListenAddress:    ":64646",
			AdvertiseAddress: "127.0.0.1:64646",
		},
	}

	onionHost := validV3Onion + ".onion"
	addr, ok := svc.normalizePeerAddress(domain.PeerAddress("1.2.3.4:12345"), domain.PeerAddress(onionHost+":64646"))
	if !ok {
		t.Fatal("expected valid v3 .onion address to be accepted")
	}
	if addr != domain.PeerAddress(onionHost+":64646") {
		t.Fatalf("unexpected normalized address: %s", addr)
	}
}

func TestNormalizePeerAddressRejectsInvalidOnion(t *testing.T) {
	t.Parallel()

	svc := &Service{
		cfg: config.Node{
			ListenAddress:    ":64646",
			AdvertiseAddress: "127.0.0.1:64646",
		},
	}

	// "short.onion" is not a valid onion address — should fall through to normal IP logic.
	_, ok := svc.normalizePeerAddress(domain.PeerAddress(""), domain.PeerAddress("short.onion:64646"))
	if ok {
		t.Fatal("expected invalid .onion to be rejected (no observed, non-IP advertised)")
	}
}

func TestNormalizePeerAddressOnionWithCustomPort(t *testing.T) {
	t.Parallel()

	svc := &Service{
		cfg: config.Node{
			ListenAddress:    ":64646",
			AdvertiseAddress: "127.0.0.1:64646",
		},
	}

	onionHost := validV3Onion + ".onion"
	addr, ok := svc.normalizePeerAddress(domain.PeerAddress(""), domain.PeerAddress(onionHost+":9999"))
	if !ok {
		t.Fatal("expected .onion address to be accepted")
	}
	if addr != domain.PeerAddress(onionHost+":9999") {
		t.Fatalf("unexpected normalized address: %s", addr)
	}
}

func TestNormalizePeerAddressOnionWithNoObserved(t *testing.T) {
	t.Parallel()

	svc := &Service{
		cfg: config.Node{
			ListenAddress:    ":64646",
			AdvertiseAddress: "127.0.0.1:64646",
		},
	}

	onionHost := validV3Onion + ".onion"
	addr, ok := svc.normalizePeerAddress(domain.PeerAddress(""), domain.PeerAddress(onionHost+":64646"))
	if !ok {
		t.Fatal("expected .onion address to be accepted")
	}
	if addr != domain.PeerAddress(onionHost+":64646") {
		t.Fatalf("unexpected normalized address: %s", addr)
	}
}
