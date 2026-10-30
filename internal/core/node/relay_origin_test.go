package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// TestEnvelopeEntersRelayRetry is the regression guard for the
// indefinite-DM-gossip bug: a DM this node ORIGINATED (sender == self) must
// not enter the relay-retry contour. Origin re-sends are owned by the
// sender-owned delivery engine (delivery_retry.go), which emits only into
// a route that exists and ends on the recipient's receipt, the author's
// withdrawal, or the message's own TTL. The relay-retry contour is for
// TRANSIT forwarding only (relay.md INV-1/INV-2); tracking our own message
// here re-gossiped it forever, and BLINDLY — every mesh echo re-armed the
// 3-minute window and flooded the mesh whether or not the recipient was
// reachable.
//
// The admission commit arms by this predicate and retryableRelayMessages
// reaps by it, so pinning it pins both sides.
func TestEnvelopeEntersRelayRetry(t *testing.T) {
	t.Parallel()
	const self = "self-identity"
	cases := []struct {
		name     string
		envelope protocol.Envelope
		want     bool
	}{
		{"transit DM between two other identities", protocol.Envelope{Topic: "dm", Sender: "alice", Recipient: "bob"}, true},
		{"origin-authored DM is the sender engine's", protocol.Envelope{Topic: "dm", Sender: self, Recipient: "bob"}, false},
		{"DM addressed to this node has arrived", protocol.Envelope{Topic: "dm", Sender: "alice", Recipient: self}, false},
		{"control DM retries at the application layer", protocol.Envelope{Topic: protocol.TopicControlDM, Sender: "alice", Recipient: "bob"}, false},
		{"broadcast recipient", protocol.Envelope{Topic: "dm", Sender: "alice", Recipient: "*"}, false},
		{"no recipient", protocol.Envelope{Topic: "dm", Sender: "alice"}, false},
		{"non-DM topic", protocol.Envelope{Topic: "global", Sender: "alice", Recipient: "bob"}, false},
	}
	for _, tc := range cases {
		if got := envelopeEntersRelayRetry(tc.envelope, self); got != tc.want {
			t.Errorf("%s: envelopeEntersRelayRetry = %v, want %v", tc.name, got, tc.want)
		}
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
	svc.relayRetry[relayMessageIdentityKey(messageIdentityOf(origin))] = relayAttempt{FirstSeen: now}
	svc.relayRetry[relayMessageIdentityKey(messageIdentityOf(transit))] = relayAttempt{FirstSeen: now}
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
	_, originStillArmed := svc.relayRetry[relayMessageIdentityKey(messageIdentityOf(origin))]
	svc.deliveryMu.RUnlock()
	if originStillArmed {
		t.Fatal("retryableRelayMessages must drop the stale relayRetry entry for an origin-authored DM")
	}
}
