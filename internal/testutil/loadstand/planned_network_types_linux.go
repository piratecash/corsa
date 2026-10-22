//go:build linux

package loadstand

import (
	"net/netip"

	"github.com/piratecash/corsa/internal/core/domain"
)

// Linux kernel admission retains exact peer and link ownership metadata.
type plannedNetworkMember struct {
	role               MemberRole
	endpoint           domain.PeerAddress
	id                 MemberID
	hostLink, peerLink string
	address            netip.Addr
}

type plannedNetworkLink struct {
	name, alias string
	index       int
}
