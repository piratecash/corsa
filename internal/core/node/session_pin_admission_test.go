package node

import (
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

func seedSessionPinCapacity(store *secureSessionStore, size int) {
	store.mu.Lock()
	defer store.mu.Unlock()
	for i := range size {
		var id domain.PeerIdentity
		binary.BigEndian.PutUint64(id[:], uint64(i+1))
		store.identities[id] = identityPinEntry{pinnedAt: store.now()}
	}
}

func TestSessionPinReserveAndEphemeralAdmission(t *testing.T) {
	t.Parallel()
	store := loadSecureSessionStore("", newStoreClock().trusted())
	seedSessionPinCapacity(store, ordinarySessionPinLimit-1)
	public := sessionPinAdmission{allowEphemeral: true}
	approved := sessionPinAdmission{allowEphemeral: true, reserve: true}
	owner := domain.PeerIdentity{1}
	endpoint := domain.PeerAddress("192.0.2.55:64646")
	original, err := store.noteProvenOutbound(dialledAt(endpoint), owner, endpointProtectionNone(), public)
	if err != nil || original.pinBinding.owner != store {
		t.Fatalf("last ordinary pin: binding=%+v, err=%v", original, err)
	}
	if transient, err := store.noteProvenInboundBinding(domain.PeerIdentity{2}, public); err != nil || transient.owner != nil {
		t.Fatalf("public session should be ephemeral: binding=%+v, err=%v", transient, err)
	}
	private := sessionPinAdmission{}
	unapproved := domain.PeerIdentity{8}
	if _, err := store.noteProvenInboundBinding(unapproved, private); !errors.Is(err, errOrdinaryPinCapacity) {
		t.Fatalf("unapproved session requiring a pin at reserve boundary: %v", err)
	}
	if _, err := store.noteProvenOutbound(dialledAt("192.0.2.56:64646"), unapproved, endpointProtectionNone(), private); !errors.Is(err, errOrdinaryPinCapacity) {
		t.Fatalf("unapproved outbound session requiring a pin at reserve boundary: %v", err)
	}
	if store.identityPinned(unapproved) {
		t.Fatal("unapproved pin-required session used reserved capacity")
	}
	if existing, err := store.noteProvenInboundBinding(owner, private); err != nil || existing.owner != store {
		t.Fatalf("an existing pin remains usable on a pin-required session: binding=%+v, err=%v", existing, err)
	}
	if _, err := store.noteProvenOutbound(dialledAt(endpoint), domain.PeerIdentity{3}, endpointProtectionNone(), public); !errors.Is(err, errEphemeralPinEndpoint) {
		t.Fatalf("ephemeral session could replace endpoint ownership: %v", err)
	}
	binding, found := bindingOf(store, endpoint)
	if !found || binding.identity != owner {
		t.Fatal("previous owner was lost")
	}
	if reserved, err := store.noteProvenInboundBinding(domain.PeerIdentity{4}, approved); err != nil || reserved.owner != store {
		t.Fatalf("approved peer should use reserve: binding=%+v, err=%v", reserved, err)
	}

	full := loadSecureSessionStore("", newStoreClock().trusted())
	seedSessionPinCapacity(full, maxPinnedIdentities)
	if _, err := full.noteProvenInboundBinding(domain.PeerIdentity{5}, approved); !errors.Is(err, errPinStoreFull) {
		t.Fatalf("approved peer at hard maximum: %v", err)
	}
	if existing, err := full.noteProvenInboundBinding(domain.PeerIdentity{7: 1}, public); err != nil || existing.owner != full {
		t.Fatalf("existing pin should remain usable: binding=%+v, err=%v", existing, err)
	}
	if transient, err := full.noteProvenInboundBinding(domain.PeerIdentity{6}, public); err != nil || transient.owner != nil {
		t.Fatalf("public session at hard maximum: binding=%+v, err=%v", transient, err)
	}
	full.unreadable = true
	if _, err := full.noteProvenInboundBinding(domain.PeerIdentity{6}, public); !errors.Is(err, domain.ErrIdentityPinStoreUnavailable) {
		t.Fatalf("public inbound session over unreadable store: %v", err)
	}
	if _, err := full.noteProvenOutbound(dialledAt(endpoint), domain.PeerIdentity{6}, endpointProtectionNone(), public); !errors.Is(err, domain.ErrIdentityPinStoreUnavailable) {
		t.Fatalf("public outbound session over unreadable store: %v", err)
	}
}

func TestEphemeralSessionCarriesDirectMessageWithoutRetainedTrust(t *testing.T) {
	t.Parallel()
	seed := func(svc *Service) { seedSessionPinCapacity(svc.secureSessions.store, ordinarySessionPinLimit) }
	relay, address, stopRelay := listenerNode(t, seed)
	defer stopRelay()
	client, stopClient := dialerNode(t, address, seed)
	defer stopClient()

	clientID, relayID := peerIdentityOf(t, client), peerIdentityOf(t, relay)
	waitForConditionMsg(t, 5*time.Second, "public TLS session did not register", func() bool {
		return relay.identityHasProvenConnection(clientID) && client.identityHasProvenConnection(relayID) &&
			relay.SubscriberCount(client.Address()) > 0
	})
	if relay.secureSessions.store.identityPinned(clientID) || client.secureSessions.store.identityPinned(relayID) {
		t.Fatal("ephemeral session wrote a permanent pin")
	}
	if _, found := bindingOf(client.secureSessions.store, domain.PeerAddress(address)); found {
		t.Fatal("ephemeral session wrote an endpoint binding")
	}
	if nodeAuthenticatedIdentity(relay, clientID) || nodeAuthenticatedIdentity(client, relayID) {
		t.Fatal("ephemeral session published retained address authority")
	}

	ciphertext, err := directmsg.EncryptForParticipants(relay.identity, domain.DMRecipient{
		Address: clientID, BoxKeyBase64: identity.BoxPublicKeyBase64(client.identity.BoxPublicKey),
	}, domain.OutgoingDM{Body: "message over ephemeral TLS"})
	if err != nil {
		t.Fatal(err)
	}
	const messageID = "ephemeral-tls-dm"
	ts := time.Now().UTC().Format(time.RFC3339)
	if reply := relay.HandleLocalFrame(sendMessageFrame("dm", messageID, relay.Address(), client.Address(), "sender-delete", ts, 0, ciphertext)); reply.Type != "message_stored" {
		t.Fatalf("store ordinary DM: %+v", reply)
	}
	waitForConditionMsg(t, 10*time.Second, "ordinary DM did not traverse ephemeral TLS link", func() bool {
		reply := client.HandleLocalFrame(protocol.Frame{Type: "fetch_messages", Topic: "dm"})
		return reply.Type == "messages" && len(reply.Messages) == 1 && reply.Messages[0].ID == messageID
	})
}
