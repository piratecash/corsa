package domain

import "testing"

// Only the two origins are valid: an empty or unknown origin read from a file
// is not silently taken for either.
func TestPeerIdentityOriginValid(t *testing.T) {
	t.Parallel()
	for origin, want := range map[PeerIdentityOrigin]bool{
		PeerIdentityClaimed:  true,
		PeerIdentityProvenV2: true,
		"":                   false,
		"proven":             false,
	} {
		if got := origin.Valid(); got != want {
			t.Errorf("PeerIdentityOrigin(%q).Valid() = %v, want %v", string(origin), got, want)
		}
	}
}
