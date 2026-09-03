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

// wireCreatedAt drains a peer's send queue for the frame carrying id and
// returns the created_at it actually put on the wire. Reading the FRAME and
// not the helper is the point of this file: the re-stamp has to be on the
// emission path, and there are five separate frame builders that could each
// forget it.
func wireCreatedAt(t *testing.T, sendCh chan peerSendItem, id string, wantTypes ...string) string {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case item := <-sendCh:
			frameID := item.ID
			if frameID == "" && item.Item != nil {
				frameID = item.Item.ID
			}
			if frameID != id {
				continue
			}
			for _, want := range wantTypes {
				if item.Type != want {
					continue
				}
				if item.Item != nil {
					return item.Item.CreatedAt
				}
				return item.CreatedAt
			}
		case <-deadline:
			t.Fatalf("no %v frame for %s reached the wire", wantTypes, id)
		}
	}
}

// TestRelayForwardRestampsSomebodyElsesOldMessage is the case a helper-only
// test could not have caught.
//
// A v29 sender → v30 relay → v29 relay route: the middle node accepts the
// old message because it no longer applies a ceiling, and then has to hand
// it on in a form the next node will accept too. Forwarding it with the
// original date puts the drop one hop further along and leaves the sender
// exactly as stuck, so the fast-path forward re-stamps like every other
// transit emission.
func TestRelayForwardRestampsSomebodyElsesOldMessage(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	origin, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	// The next hop towards the recipient: a capable relay peer that is NOT
	// the recipient, so this is a genuine transit forward.
	nextHop, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	sendCh := attachCapableRelayPeer(t, svc, "next-hop:64646", domain.PeerIdentityFromWire(nextHop.Address))
	if _, err := svc.routingTable.UpdateRoute(routeToward(t, recipient.Address, nextHop.Address)); err != nil {
		t.Fatalf("seed route: %v", err)
	}

	writtenAt := time.Now().UTC().Add(-90 * 24 * time.Hour)
	frame := protocol.Frame{
		Type:        "relay_message",
		ID:          "transit-old-1",
		Address:     origin.Address,
		Recipient:   recipient.Address,
		Topic:       "dm",
		Body:        "sealed",
		Flag:        string(protocol.MessageFlagSenderDelete),
		CreatedAt:   writtenAt.Format(time.RFC3339),
		HopCount:    1,
		MaxHops:     defaultMaxHops,
		PreviousHop: "10.0.0.9:64646",
	}
	svc.handleRelayMessage(domain.PeerAddress("10.0.0.9:64646"), nil, frame)

	onWire := wireCreatedAt(t, sendCh, "transit-old-1", "relay_message", "push_message")
	stamped, err := time.Parse(time.RFC3339, onWire)
	if err != nil {
		t.Fatalf("forwarded created_at %q is not RFC3339: %v", onWire, err)
	}
	if !stamped.After(writtenAt.Add(time.Hour)) {
		t.Errorf("the forwarded copy still carries the original date (%s); the next pre-v30 hop drops it", onWire)
	}
}

// TestOwnRetryRestampsOnTheWire is the sender-side half, asserted on the
// frame rather than on the helper.
func TestOwnRetryRestampsOnTheWire(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true

	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	nextHop, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	sendCh := attachCapableRelayPeer(t, svc, "uplink:64646", domain.PeerIdentityFromWire(nextHop.Address))
	if _, err := svc.routingTable.UpdateRoute(routeToward(t, recipient.Address, nextHop.Address)); err != nil {
		t.Fatalf("seed route: %v", err)
	}

	writtenAt := time.Now().UTC().Add(-90 * 24 * time.Hour)
	envelope := protocol.Envelope{
		ID: "own-old-1", Topic: "dm",
		Sender: svc.Address(), Recipient: recipient.Address,
		Payload: []byte("sealed"), CreatedAt: writtenAt, StoredAt: writtenAt,
	}
	now := time.Now().UTC()
	svc.deliveryMu.Lock()
	svc.registerAwaitingDeliveredLocked(envelope, now, true)
	svc.deliveryMu.Unlock()

	svc.retryDueDeliveries(now)

	onWire := wireCreatedAt(t, sendCh, "own-old-1", "relay_message", "push_message")
	stamped, err := time.Parse(time.RFC3339, onWire)
	if err != nil {
		t.Fatalf("emitted created_at %q is not RFC3339: %v", onWire, err)
	}
	if !stamped.After(writtenAt.Add(time.Hour)) {
		t.Errorf("the transit copy still carries the original date (%s)", onWire)
	}

	// The sender's own record is untouched: this is the date the user sees.
	svc.deliveryMu.RLock()
	kept := svc.awaitingDelivered["own-old-1"].Envelope.CreatedAt
	svc.deliveryMu.RUnlock()
	if !kept.Equal(writtenAt) {
		t.Errorf("the local envelope was re-dated too: %v, want %v", kept, writtenAt)
	}
}

// TestEveryCopyOfOneDispatchCarriesOneDate is the invariant the whole
// measure now rests on, and the reason it is decided once per MESSAGE.
//
// A connected peer is routinely both a push subscriber and the routing
// table's next hop for itself, so one dispatch hands it the same id twice —
// and a table-directed relay through an INBOUND-only peer serialises its
// frame on a path of its own. While the date was decided per copy (first by
// the builders from an address, then at the transport exits) some copy
// always escaped, and the recipient stored whichever arrived first. Deciding
// it on the envelope, before any frame exists, makes disagreement
// impossible: every copy is built from the same value.
func TestEveryCopyOfOneDispatchCarriesOneDate(t *testing.T) {
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

	// Three sinks in one dispatch: the recipient is a subscriber AND the
	// next hop, and a second peer takes the gossip fan-out.
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
	if len(dates) != 1 {
		t.Fatalf("one dispatch produced %d different created_at values across %d frames: %v",
			len(dates), seen, dates)
	}
	// And the local record is untouched — this is the date the user sees.
	svc.deliveryMu.RLock()
	kept := svc.awaitingDelivered["one-date-1"].Envelope.CreatedAt
	svc.deliveryMu.RUnlock()
	if !kept.Equal(writtenAt) {
		t.Errorf("the local envelope was re-dated: %v, want %v", kept, writtenAt)
	}
}

// TestTransitPushIsRestampedOnAdmission covers the door a
// start-of-outgoing-path rule missed: a transit message that arrives as a
// push_message rather than a relay_message.
//
// v29 → gossip → v30 → v29. The middle node admits the old message and
// forwards it onward; forwarding it with the original date just moves the
// drop one hop along. Normalising at admission covers this path, the relay
// fallback and anything else that enters through storeIncomingMessage,
// without any of them knowing the measure exists.
func TestTransitPushIsRestampedOnAdmission(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	registerForeignKey(t, svc, sender)

	// Somewhere to forward it: a capable peer that is neither party.
	onward, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	sendCh := attachCapableRelayPeer(t, svc, "onward:64646", domain.PeerIdentityFromWire(onward.Address))

	writtenAt := time.Now().UTC().Add(-90 * 24 * time.Hour)
	msg := incomingMessage{
		ID:        "transit-push-1",
		Topic:     "dm",
		Sender:    sender.Address,
		Recipient: recipient.Address,
		Flag:      protocol.MessageFlagSenderDelete,
		CreatedAt: writtenAt,
		Body:      sealDMBody(t, sender, recipient.Address, identity.BoxPublicKeyBase64(recipient.BoxPublicKey)),
	}
	if ok, _, errCode := svc.storeIncomingMessage(msg, false); !ok {
		t.Fatalf("the transit DM must be admitted for forwarding (errCode %q)", errCode)
	}

	onWire := wireCreatedAt(t, sendCh, "transit-push-1", "relay_message", "push_message")
	stamped, err := time.Parse(time.RFC3339, onWire)
	if err != nil {
		t.Fatalf("forwarded created_at %q is not RFC3339: %v", onWire, err)
	}
	if !stamped.After(writtenAt.Add(time.Hour)) {
		t.Errorf("the onward copy still carries the original date (%s); the next pre-v30 hop drops it", onWire)
	}
}

// TestFirstSendOfAnOldOwnMessageIsRestamped covers the very first attempt,
// not the retry.
//
// A message can be old the moment it is sent — an import, a restored
// history, a clock that was wrong — and the first copy goes out from
// storeIncomingMessage, not from the retry tick. Leaving that one with the
// original date meant a pre-v30 relay dropped it silently, charged the route
// a failure, and the message only got a usable date half a minute later.
func TestFirstSendOfAnOldOwnMessageIsRestamped(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	registerSelfKey(t, svc)

	recipient, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	nextHop, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	sendCh := attachCapableRelayPeer(t, svc, "uplink:64646", domain.PeerIdentityFromWire(nextHop.Address))
	if _, err := svc.routingTable.UpdateRoute(routeToward(t, recipient.Address, nextHop.Address)); err != nil {
		t.Fatalf("seed route: %v", err)
	}

	writtenAt := time.Now().UTC().Add(-90 * 24 * time.Hour)
	msg := incomingMessage{
		ID:        "own-first-send-1",
		Topic:     "dm",
		Sender:    svc.identity.Address,
		Recipient: recipient.Address,
		Flag:      protocol.MessageFlagSenderDelete,
		CreatedAt: writtenAt,
		Body:      sealDMBody(t, svc.identity, recipient.Address, identity.BoxPublicKeyBase64(recipient.BoxPublicKey)),
	}
	if ok, _, errCode := svc.storeIncomingMessage(msg, false); !ok {
		t.Fatalf("our own message must be stored and sent (errCode %q)", errCode)
	}

	onWire := wireCreatedAt(t, sendCh, "own-first-send-1", "relay_message", "push_message")
	stamped, err := time.Parse(time.RFC3339, onWire)
	if err != nil {
		t.Fatalf("emitted created_at %q is not RFC3339: %v", onWire, err)
	}
	if !stamped.After(writtenAt.Add(time.Hour)) {
		t.Errorf("the first copy still carries the original date (%s); a pre-v30 relay drops it", onWire)
	}

	// The retry entry keeps the real date — that is what the user sees.
	svc.deliveryMu.RLock()
	entry, awaiting := svc.awaitingDelivered["own-first-send-1"]
	svc.deliveryMu.RUnlock()
	if !awaiting {
		t.Fatal("our own message must be awaiting its receipt")
	}
	if !entry.Envelope.CreatedAt.Equal(writtenAt) {
		t.Errorf("the retry entry was re-dated: %v, want %v", entry.Envelope.CreatedAt, writtenAt)
	}
}
