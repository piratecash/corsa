package domain

// PeerIdentityOrigin is what stands behind an identity a node recorded for an
// address: a v2 session that proved it, or only a claim — a v1 hello or
// welcome — which proves nothing about who stands on the address. A claim
// never replaces what v2 recorded, the claims of an identity are forgotten
// once it proves itself over v2, and later claims of it are not recorded.
type PeerIdentityOrigin string

const (
	// PeerIdentityClaimed: the identity was named, not proven.
	PeerIdentityClaimed PeerIdentityOrigin = "claimed"
	// PeerIdentityProvenV2: a v2 session on the address proved the identity.
	PeerIdentityProvenV2 PeerIdentityOrigin = "proven_v2"
)

// Valid reports whether o is one of the two origins.
func (o PeerIdentityOrigin) Valid() bool {
	return o == PeerIdentityClaimed || o == PeerIdentityProvenV2
}
