package service

import "github.com/piratecash/corsa/internal/core/domain"

// PeerContactLink returns an exportable local public key binding, if present.
func (c *DesktopClient) PeerContactLink(peer domain.PeerIdentity) string {
	if c == nil || c.localNode == nil {
		return ""
	}
	return c.localNode.PeerContactLink(peer)
}
