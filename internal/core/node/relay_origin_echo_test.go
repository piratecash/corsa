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

// registerSelfKey publishes this node's own identity keys into the knowledge
// maps so storeIncomingMessage's VerifyEnvelope accepts a DM we authored.
func registerSelfKey(t *testing.T, svc *Service) {
	t.Helper()
	svc.knowledgeMu.Lock()
	svc.pubKeys[svc.identity.Address] = identity.PublicKeyBase64(svc.identity.PublicKey)
	svc.boxKeys[svc.identity.Address] = identity.BoxPublicKeyBase64(svc.identity.BoxPublicKey)
	svc.boxSigs[svc.identity.Address] = identity.SignBoxKeyBinding(svc.identity)
	svc.known[svc.identity.Address] = struct{}{}
	svc.knowledgeMu.Unlock()
}

// attachCapableRelayPeer registers a connected mesh_relay_v1 peer whose
// outbound frames land on the returned channel, so a test can observe what
// storeIncomingMessage re-propagates.
func attachCapableRelayPeer(t *testing.T, svc *Service, addr string, peerIdentity domain.PeerIdentity) chan protocol.Frame {
	t.Helper()
	sendCh := make(chan protocol.Frame, 32)
	svc.peerMu.Lock()
	svc.sessions[domain.PeerAddress(addr)] = &peerSession{
		address:      domain.PeerAddress(addr),
		peerIdentity: peerIdentity,
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
		sendCh:       sendCh,
		authOK:       true,
	}
	svc.health[domain.PeerAddress(addr)] = &peerHealth{Connected: true}
	if !peerIdentity.IsZero() {
		svc.peerIDs[domain.PeerAddress(addr)] = peerIdentity
	}
	svc.peerMu.Unlock()
	return sendCh
}

func drainForRepropagation(sendCh chan protocol.Frame, id string) bool {
	for {
		select {
		case f := <-sendCh:
			if (f.Type == "relay_message" || f.Type == "push_message") &&
				(f.ID == id || (f.Item != nil && f.Item.ID == id)) {
				return true
			}
		default:
			return false
		}
	}
}

// TestStoreIncomingMessage_OriginEchoNotRepropagated is the root-cause guard
// for the months-long zombie-DM storm: a DM this node authored (sender ==
// self) that arrives back FROM the network (Via != "") must NOT be
// re-gossiped or relayed. Re-propagating it re-injected the message with a
// fresh hop budget and revived a delivery the sender-owned engine had already
// abandoned. Re-propagation of our own messages belongs solely to the
// finite sender-owned retry engine.
func TestStoreIncomingMessage_OriginEchoNotRepropagated(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	registerSelfKey(t, svc)
	recipient, _ := identity.Generate()
	body := sealDMBody(t, svc.identity, recipient.Address, identity.BoxPublicKeyBase64(recipient.BoxPublicKey))

	sendCh := attachCapableRelayPeer(t, svc, "relay-peer-1:64646", domain.PeerIdentity{})

	echo := incomingMessage{
		ID:        "origin-echo-1",
		Topic:     "dm",
		Sender:    svc.identity.Address,
		Recipient: recipient.Address,
		Flag:      protocol.MessageFlagSenderDelete,
		CreatedAt: time.Now().UTC(),
		Body:      body,
		// Via != "" marks a network arrival — this is our own message
		// echoing back through the mesh, not a fresh local send.
		Via: domain.PeerAddress("relay-peer-1:64646"),
	}
	stored, _, errCode := svc.storeIncomingMessage(echo, false)
	if errCode != "" {
		t.Fatalf("unexpected errCode %q (stored=%v)", errCode, stored)
	}
	time.Sleep(100 * time.Millisecond)
	if drainForRepropagation(sendCh, "origin-echo-1") {
		t.Fatal("origin echo (sender==self, Via!=\"\") must NOT be re-gossiped/relayed")
	}
}

// TestHandleRelayMessage_OwnOriginEchoDropped is the relay fast-path companion
// to the store-path origin-echo guard: a relay_message whose ORIGIN sender is
// this node (recipient != self) is our own DM echoing back through the mesh and
// MUST NOT be re-forwarded — the fast path forwards before storeIncomingMessage,
// so without this guard it would bypass the store-path originEcho check and
// revive an abandoned delivery (the zombie-DM storm).
func TestHandleRelayMessage_OwnOriginEchoDropped(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	thirdParty, _ := identity.Generate()
	sendCh := attachCapableRelayPeer(t, svc, "relay-peer-1:64646", domain.PeerIdentity{})

	frame := protocol.Frame{
		Type:        "relay_message",
		ID:          "own-echo-relay-1",
		Address:     svc.Address(), // origin sender == self → echo of our own DM
		Recipient:   thirdParty.Address,
		Topic:       "dm",
		Body:        "x",
		Flag:        string(protocol.MessageFlagSenderDelete),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		HopCount:    2,
		MaxHops:     10,
		PreviousHop: "10.0.0.9:64646",
	}
	status := svc.handleRelayMessage(domain.PeerAddress("10.0.0.9:64646"), nil, frame)
	if status != "" {
		t.Fatalf("own-origin echo relay must be dropped (status %q, want empty)", status)
	}
	time.Sleep(50 * time.Millisecond)
	if drainForRepropagation(sendCh, "own-echo-relay-1") {
		t.Error("own-origin echo relay must NOT be forwarded/re-gossiped")
	}
}

// registerForeignKey publishes a third party's identity keys so
// storeIncomingMessage's VerifyEnvelope accepts a transit DM it authored.
func registerForeignKey(t *testing.T, svc *Service, id *identity.Identity) {
	t.Helper()
	svc.knowledgeMu.Lock()
	svc.pubKeys[id.Address] = identity.PublicKeyBase64(id.PublicKey)
	svc.boxKeys[id.Address] = identity.BoxPublicKeyBase64(id.BoxPublicKey)
	svc.boxSigs[id.Address] = identity.SignBoxKeyBinding(id)
	svc.known[id.Address] = struct{}{}
	svc.knowledgeMu.Unlock()
}

// TestStoreIncomingMessage_ForwardOnceTransitNotStored pins the Phase 3
// forward-once contract: a TRANSIT DM (neither party is this node) is forwarded
// in a SINGLE PASS (the usual gossip+relay frames, deduped at the receiver —
// forward-once bounds buffering/re-gossip over TIME, not the per-pass frame
// count) but is NEVER stored in s.topics nor tracked in the relay-retry contour
// — so retryRelayDeliveries has nothing to re-gossip (storm removed at the
// source). Durability is the sender's receipt-driven engine, not this hop.
func TestStoreIncomingMessage_ForwardOnceTransitNotStored(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.TransitForwardOnce = true
	sender, _ := identity.Generate()
	recipient, _ := identity.Generate()
	registerForeignKey(t, svc, sender)
	body := sealDMBody(t, sender, recipient.Address, identity.BoxPublicKeyBase64(recipient.BoxPublicKey))

	sendCh := attachCapableRelayPeer(t, svc, "relay-peer-1:64646", domain.PeerIdentity{})

	msg := incomingMessage{
		ID:        "fwd-once-1",
		Topic:     "dm",
		Sender:    sender.Address,
		Recipient: recipient.Address,
		Flag:      protocol.MessageFlagSenderDelete,
		CreatedAt: time.Now().UTC(),
		Body:      body,
	}
	_, _, errCode := svc.storeIncomingMessage(msg, false)
	if errCode != "" {
		t.Fatalf("unexpected errCode %q", errCode)
	}

	// Not stored.
	svc.gossipMu.RLock()
	n := len(svc.topics["dm"])
	svc.gossipMu.RUnlock()
	if n != 0 {
		t.Errorf("forward-once transit must not enter s.topics, got %d", n)
	}
	// No relay-retry entry.
	if svc.hasRelayRetryEntry("fwd-once-1") {
		t.Error("forward-once transit must not be tracked for relay-retry")
	}
	// But still forwarded on the wire (the single-pass send is not suppressed;
	// only the buffer + over-time re-gossip are removed).
	time.Sleep(100 * time.Millisecond)
	if !drainForRepropagation(sendCh, "fwd-once-1") {
		t.Error("forward-once transit must still be forwarded once")
	}
}

// TestStoreIncomingMessage_FirstSendIsPropagated is the companion positive
// guard: a freshly authored DM (Via == "", local send) MUST still be
// propagated, so the origin-echo gate does not suppress initial delivery.
// TestStoreIncomingMessage_FirstSendToReachableIsPropagated is the positive
// half of the reachability gate: a freshly authored DM (Via == "") to a
// recipient that IS reachable — a directed route resolves to a capable,
// connected next hop — must still be propagated immediately.
func TestStoreIncomingMessage_FirstSendToReachableIsPropagated(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true
	registerSelfKey(t, svc)
	recipient, _ := identity.Generate()
	body := sealDMBody(t, svc.identity, recipient.Address, identity.BoxPublicKeyBase64(recipient.BoxPublicKey))

	// Make the recipient reachable: a directly-connected capable peer that IS
	// the recipient, plus a 1-hop route to it. router.Route then resolves a
	// RelayNextHop, so the send is directed (not held).
	sendCh := attachCapableRelayPeer(t, svc, "relay-peer-2:64646", domain.PeerIdentityFromWire(recipient.Address))
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: domain.PeerIdentityFromWire(recipient.Address),
		Origin:   domain.PeerIdentityFromWire(recipient.Address),
		NextHop:  domain.PeerIdentityFromWire(recipient.Address),
		Hops:     1,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed route to recipient: %v", err)
	}

	fresh := incomingMessage{
		ID:        "origin-fresh-1",
		Topic:     "dm",
		Sender:    svc.identity.Address,
		Recipient: recipient.Address,
		Flag:      protocol.MessageFlagSenderDelete,
		CreatedAt: time.Now().UTC(),
		Body:      body,
	}
	stored, _, errCode := svc.storeIncomingMessage(fresh, false)
	if !stored || errCode != "" {
		t.Fatalf("first send must store, got stored=%v errCode=%q", stored, errCode)
	}
	time.Sleep(150 * time.Millisecond)
	if !drainForRepropagation(sendCh, "origin-fresh-1") {
		t.Fatal("a freshly authored DM to a REACHABLE recipient must be propagated")
	}
}

// TestStoreIncomingMessage_FirstSendToUnreachableIsHeld is the root-cure for
// the blind-gossip storm: a freshly authored DM (Via == "") to a recipient
// with NO route and NO direct session must NOT be emitted — it is held in the
// sender-owned retry set and delivered only when the recipient becomes
// reachable. A capable relay peer that is NOT a route to the recipient must
// not receive it (no blind fan-out into the void).
func TestStoreIncomingMessage_FirstSendToUnreachableIsHeld(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true
	registerSelfKey(t, svc)
	recipient, _ := identity.Generate()
	body := sealDMBody(t, svc.identity, recipient.Address, identity.BoxPublicKeyBase64(recipient.BoxPublicKey))

	// A capable relay peer exists, but it is NOT a route to the recipient and
	// is not the recipient — so the recipient is unreachable.
	sendCh := attachCapableRelayPeer(t, svc, "relay-peer-3:64646", domain.PeerIdentity{})

	fresh := incomingMessage{
		ID:        "origin-held-1",
		Topic:     "dm",
		Sender:    svc.identity.Address,
		Recipient: recipient.Address,
		Flag:      protocol.MessageFlagSenderDelete,
		CreatedAt: time.Now().UTC(),
		Body:      body,
	}
	stored, _, errCode := svc.storeIncomingMessage(fresh, false)
	if !stored || errCode != "" {
		t.Fatalf("held send must still store, got stored=%v errCode=%q", stored, errCode)
	}
	time.Sleep(150 * time.Millisecond)
	if drainForRepropagation(sendCh, "origin-held-1") {
		t.Fatal("a DM to an UNREACHABLE recipient must be held, not blind-gossiped")
	}
	// It must be parked in the sender-owned retry set for later delivery.
	svc.deliveryMu.RLock()
	_, awaiting := svc.awaitingDelivered["origin-held-1"]
	svc.deliveryMu.RUnlock()
	if !awaiting {
		t.Fatal("a held DM must be registered in awaitingDelivered for sender-owned retry")
	}
}

// TestKickDeliveryRetriesForReachable verifies the responsiveness hook: when a
// recipient becomes reachable, the held retry's NextAttemptAt is pulled
// forward to now (so the next 2s tick delivers) without spending an attempt.
func TestKickDeliveryRetriesForReachable(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true
	recipient, _ := identity.Generate()

	// The kick self-checks reachability, so make the recipient reachable:
	// a directly-connected capable peer that IS the recipient + a 1-hop route.
	attachCapableRelayPeer(t, svc, "kick-peer:64646", domain.PeerIdentityFromWire(recipient.Address))
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: domain.PeerIdentityFromWire(recipient.Address),
		Origin:   domain.PeerIdentityFromWire(recipient.Address),
		NextHop:  domain.PeerIdentityFromWire(recipient.Address),
		Hops:     1,
		SeqNo:    1,
		Source:   routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed route to recipient: %v", err)
	}

	future := time.Now().UTC().Add(10 * time.Minute)
	svc.deliveryMu.Lock()
	svc.awaitingDelivered["held-1"] = &deliveryRetryEntry{
		Envelope:      protocol.Envelope{ID: "held-1", Topic: "dm", Sender: svc.identity.Address, Recipient: recipient.Address},
		Attempts:      3,
		NextAttemptAt: future,
		Held:          true,
	}
	svc.deliveryMu.Unlock()

	svc.kickDeliveryRetriesForReachable(map[domain.PeerIdentity]struct{}{
		domain.PeerIdentityFromWire(recipient.Address): {},
	})

	svc.deliveryMu.RLock()
	entry := svc.awaitingDelivered["held-1"]
	svc.deliveryMu.RUnlock()
	if entry.NextAttemptAt.After(time.Now().UTC()) {
		t.Fatalf("kick must pull NextAttemptAt forward to now for a REACHABLE recipient, still future: %v", entry.NextAttemptAt)
	}
	if entry.Attempts != 3 {
		t.Fatalf("kick must not spend an attempt, Attempts=%d want 3", entry.Attempts)
	}
}

// TestKickDeliveryRetriesForUnreachableIsNoop verifies the self-check: a kick
// naming a recipient that is NOT actually reachable (false kick) does not
// re-arm the held retry, so no scheduled retry wastes an attempt holding again.
func TestKickDeliveryRetriesForUnreachableIsNoop(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true
	recipient, _ := identity.Generate()

	future := time.Now().UTC().Add(10 * time.Minute)
	svc.deliveryMu.Lock()
	svc.awaitingDelivered["held-2"] = &deliveryRetryEntry{
		Envelope:      protocol.Envelope{ID: "held-2", Topic: "dm", Sender: svc.identity.Address, Recipient: recipient.Address},
		Attempts:      3,
		NextAttemptAt: future,
		Held:          true,
	}
	svc.deliveryMu.Unlock()

	// No route, no session — the recipient is unreachable; the kick is a no-op.
	svc.kickDeliveryRetriesForReachable(map[domain.PeerIdentity]struct{}{
		domain.PeerIdentityFromWire(recipient.Address): {},
	})

	svc.deliveryMu.RLock()
	entry := svc.awaitingDelivered["held-2"]
	svc.deliveryMu.RUnlock()
	if !entry.NextAttemptAt.Equal(future) {
		t.Fatalf("false kick (unreachable) must NOT re-arm; NextAttemptAt moved to %v", entry.NextAttemptAt)
	}
}

// TestKickDeliveryRetriesSkipsAlreadyEmitted pins the Held gate: an entry that
// was already EMITTED and is merely awaiting its receipt (Held=false) must NOT
// be pulled forward by a kick, even when the recipient is reachable — a route
// refresh/reconfirm (RouteUnchanged) for a live conversation must not burn
// attempts or trigger early duplicate sends.
func TestKickDeliveryRetriesSkipsAlreadyEmitted(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	svc.cfg.HoldDMUntilReachable = true
	recipient, _ := identity.Generate()

	// Recipient IS reachable, so only the Held gate can keep the kick away.
	attachCapableRelayPeer(t, svc, "emit-peer:64646", domain.PeerIdentityFromWire(recipient.Address))
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: domain.PeerIdentityFromWire(recipient.Address),
		Origin:   domain.PeerIdentityFromWire(recipient.Address),
		NextHop:  domain.PeerIdentityFromWire(recipient.Address),
		Hops:     1, SeqNo: 1, Source: routing.RouteSourceAnnouncement,
	}); err != nil {
		t.Fatalf("seed route: %v", err)
	}

	future := time.Now().UTC().Add(10 * time.Minute)
	svc.deliveryMu.Lock()
	svc.awaitingDelivered["emitted-1"] = &deliveryRetryEntry{
		Envelope:      protocol.Envelope{ID: "emitted-1", Topic: "dm", Sender: svc.identity.Address, Recipient: recipient.Address},
		Attempts:      2,
		NextAttemptAt: future,
		Held:          false, // already emitted, awaiting receipt
	}
	svc.deliveryMu.Unlock()

	svc.kickDeliveryRetriesForReachable(map[domain.PeerIdentity]struct{}{
		domain.PeerIdentityFromWire(recipient.Address): {},
	})

	svc.deliveryMu.RLock()
	entry := svc.awaitingDelivered["emitted-1"]
	svc.deliveryMu.RUnlock()
	if !entry.NextAttemptAt.Equal(future) {
		t.Fatalf("kick must NOT wake an already-emitted (Held=false) entry; NextAttemptAt moved to %v", entry.NextAttemptAt)
	}
}
