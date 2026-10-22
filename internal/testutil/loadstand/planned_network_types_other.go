//go:build !linux

package loadstand

import "net/netip"

// Portable planning and fake backends do not hold Linux admission metadata.
type plannedNetworkMember struct {
	id                 MemberID
	hostLink, peerLink string
	address            netip.Addr
}

type plannedNetworkLink struct {
	name  string
	index int
}
