package main

import (
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/startupfence"
)

func parseNode(raw string) (domain.PeerIdentity, error) {
	node := domain.PeerIdentityFromWire(raw)
	if node.IsZero() || node.String() != raw {
		return domain.PeerIdentity{}, startupfence.ErrInvalid
	}
	return node, nil
}
