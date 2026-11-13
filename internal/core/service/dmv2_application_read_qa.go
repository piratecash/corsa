//go:build s6_direct_dmv2_qa

package service

import (
	"errors"
	"time"
)

// EnableDirectDMV2ReadForTest selects the configured network before the SDK
// subscription starts. This method exists only in the overlay.
func (c *DesktopClient) EnableDirectDMV2ReadForTest() {
	c.dm.dmV2ReadNetwork = c.NetworkName()
}

// EnableDirectDMV2SendForTest selects an explicit experimental TTL. The
// normal constructor leaves the feature off.
func (c *DesktopClient) EnableDirectDMV2SendForTest(ttl time.Duration) error {
	if ttl < time.Second || ttl%time.Second != 0 || c.localNode == nil || c.NetworkName() == "" {
		return errors.New("invalid direct dm-v2 test profile")
	}
	c.EnableDirectDMV2ReadForTest()
	c.dm.dmV2SendTTL = ttl
	c.localNode.EnableDirectDMV2OutboxForTest()
	return nil
}
