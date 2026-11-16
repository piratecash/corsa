package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/routing"
)

// routeToward is a one-hop route to target through nextHop.
func routeToward(t *testing.T, target, nextHop string) routing.RouteEntry {
	t.Helper()
	return routing.RouteEntry{
		Identity: domain.PeerIdentityFromWire(target),
		Origin:   domain.PeerIdentityFromWire(target),
		NextHop:  domain.PeerIdentityFromWire(nextHop),
		Hops:     1, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}
}

// Retries use the original date for every copy of the old envelope.
func TestEveryCopyOfOneDispatchKeepsItsOriginalDate(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	recipientID := domain.PeerIdentityFromWire(recipient.Address)
	bystander, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	// One dispatch, several copies: the recipient is the next hop, a second
	// peer takes the gossip fan-out, and the recipient also holds a push
	// subscription. That subscription has no registered connection, so its
	// push copy never reaches a wire (the push drops the subscriber instead);
	// it is here so the dispatch walks its push-subscriber branch as well,
	// and the copies compared below are the ones the two peers receive.
	recipientCh := attachCapableRelayPeer(t, svc, "recipient-peer:64646", recipientID)
	gossipCh := attachCapableRelayPeer(t, svc, "bystander:64646", domain.PeerIdentityFromWire(bystander.Address))
	if _, err := svc.routingTable.UpdateRoute(routeToward(t, recipient.Address, recipient.Address)); err != nil {
		t.Fatalf("seed route: %v", err)
	}
	svc.gossipMu.Lock()
	if svc.subs[recipient.Address] == nil {
		svc.subs[recipient.Address] = make(map[string]*subscriber)
	}
	svc.subs[recipient.Address]["sub-1"] = &subscriber{id: "sub-1", recipient: recipient.Address}
	svc.gossipMu.Unlock()

	writtenAt := time.Now().UTC().Add(-90 * 24 * time.Hour).Truncate(time.Second)
	envelope := protocol.Envelope{
		ID: "one-date-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipient.Address,
		Payload: []byte("sealed"), CreatedAt: writtenAt,
		// StoredAt left zero: a locally authored message carries the full
		// hop budget, which is what lets the gossip fan-out run at all
		// (envelopeEmitHops reads Hops==0 with a set StoredAt as exhausted).
	}
	now := time.Now().UTC()
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(envelope, now, true)
	svc.deliveryMu.Unlock()

	svc.retryDueDeliveries(now)

	// Read EVERY copy from BOTH peers: the defect this covers is two frames
	// for one id disagreeing, so stopping at the first would prove nothing.
	dates := map[string][]string{}
	seen := 0
	deadline := time.After(1500 * time.Millisecond)
	for draining := true; draining; {
		var item peerSendItem
		select {
		case item = <-recipientCh:
		case item = <-gossipCh:
		case <-deadline:
			draining = false
			continue
		}
		id := item.ID
		got := item.CreatedAt
		if item.Item != nil {
			id, got = item.Item.ID, item.Item.CreatedAt
		}
		if id != "one-date-1" {
			continue
		}
		seen++
		dates[got] = append(dates[got], item.Type)
	}
	if seen < 2 {
		t.Fatalf("only %d copies reached the wire; the test needs several to compare", seen)
	}
	if len(dates) != 1 || len(dates[writtenAt.Format(time.RFC3339)]) != seen {
		t.Fatalf("expected the original date on each of %d frames, got %v", seen, dates)
	}
	// And the local record is untouched — this is the date the user sees.
	svc.deliveryMu.RLock()
	kept := svc.awaitingDelivered[messageIdentityOf(envelope)].Envelope.CreatedAt
	svc.deliveryMu.RUnlock()
	if !kept.Equal(writtenAt) {
		t.Errorf("the local envelope was re-dated: %v, want %v", kept, writtenAt)
	}
}
