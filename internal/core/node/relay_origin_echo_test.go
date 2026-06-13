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
	if peerIdentity != "" {
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

	sendCh := attachCapableRelayPeer(t, svc, "relay-peer-1:64646", "")

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
	sendCh := attachCapableRelayPeer(t, svc, "relay-peer-2:64646", domain.PeerIdentity(recipient.Address))
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: domain.PeerIdentity(recipient.Address),
		Origin:   domain.PeerIdentity(recipient.Address),
		NextHop:  domain.PeerIdentity(recipient.Address),
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
	sendCh := attachCapableRelayPeer(t, svc, "relay-peer-3:64646", "")

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
	attachCapableRelayPeer(t, svc, "kick-peer:64646", domain.PeerIdentity(recipient.Address))
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: domain.PeerIdentity(recipient.Address),
		Origin:   domain.PeerIdentity(recipient.Address),
		NextHop:  domain.PeerIdentity(recipient.Address),
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
		domain.PeerIdentity(recipient.Address): {},
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
		domain.PeerIdentity(recipient.Address): {},
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
	attachCapableRelayPeer(t, svc, "emit-peer:64646", domain.PeerIdentity(recipient.Address))
	if _, err := svc.routingTable.UpdateRoute(routing.RouteEntry{
		Identity: domain.PeerIdentity(recipient.Address),
		Origin:   domain.PeerIdentity(recipient.Address),
		NextHop:  domain.PeerIdentity(recipient.Address),
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
		domain.PeerIdentity(recipient.Address): {},
	})

	svc.deliveryMu.RLock()
	entry := svc.awaitingDelivered["emitted-1"]
	svc.deliveryMu.RUnlock()
	if !entry.NextAttemptAt.Equal(future) {
		t.Fatalf("kick must NOT wake an already-emitted (Held=false) entry; NextAttemptAt moved to %v", entry.NextAttemptAt)
	}
}
