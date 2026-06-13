package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestTrackRelayMessage_SkipsOriginAuthored is the regression guard for the
// indefinite-DM-gossip bug: a DM this node ORIGINATED (sender == self) must
// not enter the relay-retry contour. Origin re-sends are owned by the
// sender-owned delivery engine (delivery_retry.go), which retries finitely
// (TTL / attempts cap) and terminalizes durably. The relay-retry contour is
// for TRANSIT forwarding only (relay.md INV-1/INV-2); tracking our own
// message here re-gossiped it forever because every mesh echo re-armed the
// 3-minute window long past the sender-owned cap.
func TestTrackRelayMessage_SkipsOriginAuthored(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	other1, _ := identity.Generate()
	other2, _ := identity.Generate()

	origin := protocol.Envelope{
		ID:        "origin-dm-1",
		Topic:     "dm",
		Sender:    svc.identity.Address,
		Recipient: other1.Address,
		Flag:      protocol.MessageFlagSenderDelete,
	}
	svc.trackRelayMessage(origin)
	svc.deliveryMu.RLock()
	_, originTracked := svc.relayRetry[relayMessageKey(origin.ID)]
	svc.deliveryMu.RUnlock()
	if originTracked {
		t.Fatal("trackRelayMessage must NOT arm relayRetry for an origin-authored DM (sender == self)")
	}

	// Transit DM (neither party is us) is the legitimate relay-retry subject.
	transit := protocol.Envelope{
		ID:        "transit-dm-1",
		Topic:     "dm",
		Sender:    other1.Address,
		Recipient: other2.Address,
		Flag:      protocol.MessageFlagSenderDelete,
	}
	svc.trackRelayMessage(transit)
	svc.deliveryMu.RLock()
	_, transitTracked := svc.relayRetry[relayMessageKey(transit.ID)]
	svc.deliveryMu.RUnlock()
	if !transitTracked {
		t.Fatal("trackRelayMessage must arm relayRetry for a transit DM (in-flight forwarding)")
	}
}

// TestRetryableRelayMessages_SkipsOriginAuthored verifies the read side of the
// relay-retry contour also refuses origin-authored DMs, even if an entry was
// somehow armed (a stale arm from before the fix, or an echo). Without this,
// retryRelayDeliveries re-gossiped our own undelivered DM on every 2s tick.
func TestRetryableRelayMessages_SkipsOriginAuthored(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	other1, _ := identity.Generate()
	other2, _ := identity.Generate()
	now := time.Now().UTC()

	origin := protocol.Envelope{
		ID:        "origin-dm-2",
		Topic:     "dm",
		Sender:    svc.identity.Address,
		Recipient: other1.Address,
		Flag:      protocol.MessageFlagSenderDelete,
		CreatedAt: now,
	}
	transit := protocol.Envelope{
		ID:        "transit-dm-2",
		Topic:     "dm",
		Sender:    other1.Address,
		Recipient: other2.Address,
		Flag:      protocol.MessageFlagSenderDelete,
		CreatedAt: now,
	}

	svc.gossipMu.Lock()
	svc.topics["dm"] = []protocol.Envelope{origin, transit}
	svc.gossipMu.Unlock()

	// Arm BOTH so the test proves the read-side filter, not just an absent arm.
	svc.deliveryMu.Lock()
	svc.relayRetry[relayMessageKey(origin.ID)] = relayAttempt{FirstSeen: now}
	svc.relayRetry[relayMessageKey(transit.ID)] = relayAttempt{FirstSeen: now}
	svc.deliveryMu.Unlock()

	got := svc.retryableRelayMessages(now)

	for _, m := range got {
		if m.ID == origin.ID {
			t.Fatal("retryableRelayMessages must skip origin-authored DM (sender == self)")
		}
	}
	foundTransit := false
	for _, m := range got {
		if m.ID == transit.ID {
			foundTransit = true
		}
	}
	if !foundTransit {
		t.Fatal("retryableRelayMessages must still return a transit DM within the window")
	}

	// The stale origin arm must be reaped so it does not linger in the map.
	svc.deliveryMu.RLock()
	_, originStillArmed := svc.relayRetry[relayMessageKey(origin.ID)]
	svc.deliveryMu.RUnlock()
	if originStillArmed {
		t.Fatal("retryableRelayMessages must drop the stale relayRetry entry for an origin-authored DM")
	}
}
