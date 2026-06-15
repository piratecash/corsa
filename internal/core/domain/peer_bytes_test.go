package domain

import (
	"encoding/json"
	"net/netip"
	"testing"
)

const sampleFP = "aa00112233445566778899aabbccddeeff001122"

func TestParsePeerIdentity(t *testing.T) {
	id, err := ParsePeerIdentity(sampleFP)
	if err != nil {
		t.Fatalf("valid lowercase hex must parse: %v", err)
	}
	if id.String() != sampleFP {
		t.Fatalf("String round-trip mismatch: got %q want %q", id.String(), sampleFP)
	}

	// Empty decodes to the zero (absent) identity with no error.
	zero, err := ParsePeerIdentity("")
	if err != nil || !zero.IsZero() {
		t.Fatalf("empty must yield zero identity, nil error: id=%v err=%v", zero, err)
	}
	if zero.String() != "" {
		t.Fatalf("zero identity String must be empty, got %q", zero.String())
	}

	// Uppercase hex must be rejected (protocol invariant: lowercase only).
	if _, err := ParsePeerIdentity("AA00112233445566778899AABBCCDDEEFF001122"); err == nil {
		t.Fatal("uppercase hex must be rejected")
	}
	// Non-hex and wrong-length must be rejected.
	if _, err := ParsePeerIdentity("not-a-valid-hex-identity-string-zzzzzzzz"); err == nil {
		t.Fatal("non-hex must be rejected")
	}
	if _, err := ParsePeerIdentity("aa00"); err == nil {
		t.Fatal("wrong length must be rejected")
	}
}

func TestPeerIdentityTextAndJSON(t *testing.T) {
	id, _ := ParsePeerIdentity(sampleFP)

	// TextMarshaler round-trip.
	text, err := id.MarshalText()
	if err != nil || string(text) != sampleFP {
		t.Fatalf("MarshalText: got %q err %v", text, err)
	}
	var back PeerIdentity
	if err := back.UnmarshalText([]byte(sampleFP)); err != nil || back != id {
		t.Fatalf("UnmarshalText round-trip failed: %v err %v", back, err)
	}

	// JSON value field stays a hex string; zero marshals to "".
	blob, err := json.Marshal(struct {
		A PeerIdentity `json:"a"`
		Z PeerIdentity `json:"z"`
	}{A: id})
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	if want := `{"a":"` + sampleFP + `","z":""}`; string(blob) != want {
		t.Fatalf("json shape mismatch:\n got %s\nwant %s", blob, want)
	}

	// JSON map key uses the hex text.
	m := map[PeerIdentity]int{id: 7}
	mb, _ := json.Marshal(m)
	if want := `{"` + sampleFP + `":7}`; string(mb) != want {
		t.Fatalf("json map-key shape mismatch: got %s want %s", mb, want)
	}

	// Reverse path: encoding/json must decode the hex map key back through
	// UnmarshalText, reconstructing the identity.
	var decoded map[PeerIdentity]int
	if err := json.Unmarshal(mb, &decoded); err != nil {
		t.Fatalf("json map-key unmarshal: %v", err)
	}
	if decoded[id] != 7 || len(decoded) != 1 {
		t.Fatalf("json map-key unmarshal mismatch: got %v", decoded)
	}
}

func TestPeerIdentitySQL(t *testing.T) {
	id, _ := ParsePeerIdentity(sampleFP)

	// Value persists as the canonical hex text.
	v, err := id.Value()
	if err != nil || v.(string) != sampleFP {
		t.Fatalf("Value: got %v err %v", v, err)
	}

	// Scan accepts string, []byte and nil.
	var fromStr, fromBytes, fromNil PeerIdentity
	if err := fromStr.Scan(sampleFP); err != nil || fromStr != id {
		t.Fatalf("Scan(string): %v err %v", fromStr, err)
	}
	if err := fromBytes.Scan([]byte(sampleFP)); err != nil || fromBytes != id {
		t.Fatalf("Scan([]byte): %v err %v", fromBytes, err)
	}
	if err := fromNil.Scan(nil); err != nil || !fromNil.IsZero() {
		t.Fatalf("Scan(nil) must yield zero: %v err %v", fromNil, err)
	}
}

func TestPeerIdentityCompare(t *testing.T) {
	a, _ := ParsePeerIdentity("aa00000000000000000000000000000000000001")
	b, _ := ParsePeerIdentity("bb00000000000000000000000000000000000001")
	if a.Compare(b) >= 0 || b.Compare(a) <= 0 || a.Compare(a) != 0 {
		t.Fatal("Compare must order by bytes (a<b), matching lowercase-hex lexical order")
	}

	got, err := PeerIdentityFromBytes(a[:])
	if err != nil || got != a {
		t.Fatalf("PeerIdentityFromBytes round-trip: %v err %v", got, err)
	}
	if _, err := PeerIdentityFromBytes([]byte{1, 2, 3}); err == nil {
		t.Fatal("PeerIdentityFromBytes must reject wrong length")
	}
}

func TestPeerIP(t *testing.T) {
	ip, err := ParsePeerIP("203.0.113.7")
	if err != nil || !ip.IsValid() || ip.String() != "203.0.113.7" {
		t.Fatalf("ParsePeerIP(v4): ip=%v valid=%v err=%v", ip.String(), ip.IsValid(), err)
	}

	// Empty → zero/invalid sentinel with empty textual form.
	zero, err := ParsePeerIP("")
	if err != nil || zero.IsValid() || zero.String() != "" {
		t.Fatalf("empty PeerIP must be invalid with empty string: %v err %v", zero.String(), err)
	}
	text, err := zero.MarshalText()
	if err != nil || len(text) != 0 {
		t.Fatalf("zero PeerIP MarshalText must be empty: %q err %v", text, err)
	}

	// IPv4-mapped IPv6 collapses to the bare IPv4 form (Unmap).
	mapped, err := ParsePeerIP("::ffff:203.0.113.7")
	if err != nil || mapped.String() != "203.0.113.7" {
		t.Fatalf("IPv4-mapped must Unmap to bare IPv4: got %q err %v", mapped.String(), err)
	}

	// PeerIPFromAddr also unmaps and round-trips text.
	from := PeerIPFromAddr(netip.MustParseAddr("203.0.113.8"))
	var back PeerIP
	if err := back.UnmarshalText([]byte("203.0.113.8")); err != nil || back != from {
		t.Fatalf("PeerIP text round-trip: %v err %v", back.String(), err)
	}
}
