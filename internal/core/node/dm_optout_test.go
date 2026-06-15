package node

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/connauth"
	"github.com/piratecash/corsa/internal/core/directmsg"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// These tests pin the relay-only DM opt-out (config.Node.
// DisableDirectMessages, env CORSA_ACCEPT_DM on the headless binary):
//
//   - inbound DM-class messages addressed to the node itself are dropped
//     BEFORE crypto/locks with duplicate semantics (ack to the previous
//     hop, no receipt, nothing enters memory);
//   - transit DMs between other parties and broadcast/global topics are
//     untouched — the node keeps relaying;
//   - the node's own box key stays OUT of the contact plane
//     (fetch_contacts / trust-store self row) so it is never
//     redistributed, but stays IN the handshake (hello/welcome) because
//     deployed peers require all four identity fields to issue the
//     session-auth challenge (connauth.HasIdentityFields).

func startRelayOnlyTestNode(t *testing.T) (*Service, func()) {
	t.Helper()
	return startTestNode(t, config.Node{
		ListenAddress:         freeAddress(t),
		BootstrapPeers:        []string{},
		DisableDirectMessages: true,
	})
}

// TestDisableDMDropsInboundDMToSelf: a data DM addressed to the opted-out
// node is rejected with duplicate semantics — (false, 0, "") makes
// shouldAckOnStoreResult ack the previous hop so hop-level retries stop —
// and leaves no trace in topics, the dedup bloom, or known identities.
// The body is deliberately garbage: the gate must fire before envelope
// verification, so spam costs the node no crypto work.
func TestDisableDMDropsInboundDMToSelf(t *testing.T) {
	t.Parallel()

	svc, cleanup := startRelayOnlyTestNode(t)
	defer cleanup()

	sender, _ := identity.Generate()
	msgID, _ := protocol.NewMessageID()

	stored, count, errCode := svc.storeIncomingMessage(incomingMessage{
		ID:         msgID,
		Topic:      "dm",
		Sender:     sender.Address,
		Recipient:  svc.identity.Address,
		Flag:       protocol.MessageFlagImmutable,
		CreatedAt:  time.Now().UTC(),
		TTLSeconds: 86400,
		Body:       "not-even-a-valid-envelope",
	}, true)

	if stored || count != 0 || errCode != "" {
		t.Fatalf("want drop with duplicate semantics (false, 0, \"\"), got (%v, %d, %q)", stored, count, errCode)
	}
	if !shouldAckOnStoreResult(stored, errCode) {
		t.Fatal("drop must still ack the previous hop, or upstream re-push loops forever")
	}

	svc.gossipMu.RLock()
	topicLen := len(svc.topics["dm"])
	seen := svc.seen.Has(string(msgID))
	svc.gossipMu.RUnlock()
	if topicLen != 0 {
		t.Fatalf("dropped DM must not enter topics, got %d entries", topicLen)
	}
	if seen {
		t.Fatal("dropped DM must not enter the dedup bloom")
	}

	svc.knowledgeMu.RLock()
	_, senderKnown := svc.known[sender.Address]
	svc.knowledgeMu.RUnlock()
	if senderKnown {
		t.Fatal("dropped DM must not register the sender as a known identity")
	}
}

// TestDisableDMDropsControlDMToSelf: control DMs share the gate — they
// mutate chat state a relay-only node does not keep.
func TestDisableDMDropsControlDMToSelf(t *testing.T) {
	t.Parallel()

	svc, cleanup := startRelayOnlyTestNode(t)
	defer cleanup()

	sender, _ := identity.Generate()
	msgID, _ := protocol.NewMessageID()

	stored, count, errCode := svc.storeIncomingMessage(incomingMessage{
		ID:         msgID,
		Topic:      protocol.TopicControlDM,
		Sender:     sender.Address,
		Recipient:  svc.identity.Address,
		Flag:       protocol.MessageFlagImmutable,
		CreatedAt:  time.Now().UTC(),
		TTLSeconds: 86400,
		Body:       "garbage",
	}, false)

	if stored || count != 0 || errCode != "" {
		t.Fatalf("want drop with duplicate semantics (false, 0, \"\"), got (%v, %d, %q)", stored, count, errCode)
	}
}

// TestDisableDMKeepsTransitDM: a DM between two OTHER parties must keep
// flowing through the opted-out node — relay-only means exactly that.
func TestDisableDMKeepsTransitDM(t *testing.T) {
	t.Parallel()

	svc, cleanup := startRelayOnlyTestNode(t)
	defer cleanup()

	from, _ := identity.Generate()
	to, _ := identity.Generate()

	svc.HandleLocalFrame(protocol.Frame{
		Type: "import_contacts",
		Contacts: []protocol.ContactFrame{{
			Address: from.Address,
			PubKey:  identity.PublicKeyBase64(from.PublicKey),
			BoxKey:  identity.BoxPublicKeyBase64(from.BoxPublicKey),
			BoxSig:  identity.SignBoxKeyBinding(from),
		}},
	})

	sealed, err := directmsg.EncryptForParticipants(
		from,
		domain.DMRecipient{
			Address:      domain.PeerIdentityFromWire(to.Address),
			BoxKeyBase64: identity.BoxPublicKeyBase64(to.BoxPublicKey),
		},
		domain.OutgoingDM{Body: "transit"},
	)
	if err != nil {
		t.Fatalf("seal envelope: %v", err)
	}

	msgID, _ := protocol.NewMessageID()
	stored, _, errCode := svc.storeIncomingMessage(incomingMessage{
		ID:         msgID,
		Topic:      "dm",
		Sender:     from.Address,
		Recipient:  to.Address,
		Flag:       protocol.MessageFlagImmutable,
		CreatedAt:  time.Now().UTC(),
		TTLSeconds: 86400,
		Body:       sealed,
	}, false)
	if !stored || errCode != "" {
		t.Fatalf("transit DM must be stored for relay: stored=%v errCode=%q", stored, errCode)
	}
}

// TestDisableDMKeepsBroadcastTopics: non-DM topics are not personal mail —
// the opted-out node keeps participating in gossip backlogs.
func TestDisableDMKeepsBroadcastTopics(t *testing.T) {
	t.Parallel()

	svc, cleanup := startRelayOnlyTestNode(t)
	defer cleanup()

	msgID, _ := protocol.NewMessageID()
	stored, _, errCode := svc.storeIncomingMessage(incomingMessage{
		ID:         msgID,
		Topic:      "news",
		Sender:     "someone",
		Recipient:  "*",
		Flag:       protocol.MessageFlagImmutable,
		CreatedAt:  time.Now().UTC(),
		TTLSeconds: 86400,
		Body:       "broadcast body",
	}, false)
	if !stored || errCode != "" {
		t.Fatalf("broadcast must be stored: stored=%v errCode=%q", stored, errCode)
	}
}

// TestRelayOnlyKeepsBoxKeyInHandshake pins the auth-compatibility
// contract: hello/welcome carry the box key EVEN for a relay-only node.
// Deployed peers issue the session-auth challenge only when all four
// identity fields are present (connauth.HasIdentityFields); a keyless
// hello would land in the unauthenticated branch and break every
// outbound authenticated session. The DM opt-out suppresses the key on
// the contact plane only (see TestDisableDMOmitsSelfBoxKeyFromContacts).
func TestRelayOnlyKeepsBoxKeyInHandshake(t *testing.T) {
	t.Parallel()

	relayOnly, cleanup := startRelayOnlyTestNode(t)
	defer cleanup()

	var hello protocol.Frame
	if err := json.Unmarshal([]byte(relayOnly.nodeHelloJSONLine()), &hello); err != nil {
		t.Fatalf("parse hello line: %v", err)
	}
	if hello.BoxKey == "" || hello.BoxSig == "" {
		t.Fatalf("relay-only hello must keep box key for session auth, got box_key=%q box_sig=%q", hello.BoxKey, hello.BoxSig)
	}
	if !connauth.HasIdentityFields(hello) {
		t.Fatal("relay-only hello must satisfy connauth.HasIdentityFields, or peers never issue the auth challenge")
	}
	if err := identity.VerifyBoxKeyBinding(hello.Address, hello.PubKey, hello.BoxKey, hello.BoxSig); err != nil {
		t.Fatalf("relay-only hello box key binding must verify (PrepareAuth checks it): %v", err)
	}

	welcome := relayOnly.welcomeFrame("", "")
	if welcome.BoxKey == "" || welcome.BoxSig == "" {
		t.Fatalf("relay-only welcome must keep box key for session auth, got box_key=%q box_sig=%q", welcome.BoxKey, welcome.BoxSig)
	}
}

// TestDisableDMOmitsSelfBoxKeyFromContacts: fetch_contacts serves the
// knowledge cache seeded from the trust store; the self row must carry no
// box key when DMs are disabled — this is the path remote relays use for
// on-demand key sync, so a leak here would resurrect the key network-wide.
func TestDisableDMOmitsSelfBoxKeyFromContacts(t *testing.T) {
	t.Parallel()

	svc, cleanup := startRelayOnlyTestNode(t)
	defer cleanup()

	frame := svc.contactsFrame()
	for _, contact := range frame.Contacts {
		if contact.Address == svc.identity.Address && contact.BoxKey != "" {
			t.Fatalf("relay-only fetch_contacts leaked self box key: %q", contact.BoxKey)
		}
	}
}

// TestRelayOnlySelfBoxKeyEchoRejected pins the contact-plane echo loop:
// a neighbor learns our box key from the handshake (it is always there —
// session auth needs all four identity fields) and can echo it back via
// fetch_contacts sync or import_contacts. The echo carries our GENUINE,
// validly-signed binding, so signature checks cannot reject it — the
// import chokepoints (addKnownBoxKey/addKnownBoxSig/trustContact) must
// drop self key material explicitly, or contactsFrame starts
// redistributing the key network-wide again.
func TestRelayOnlySelfBoxKeyEchoRejected(t *testing.T) {
	t.Parallel()

	svc, cleanup := startRelayOnlyTestNode(t)
	defer cleanup()

	self := svc.identity.Address
	pubKey := identity.PublicKeyBase64(svc.identity.PublicKey)
	boxKey := identity.BoxPublicKeyBase64(svc.identity.BoxPublicKey)
	boxSig := identity.SignBoxKeyBinding(svc.identity)

	// Echo through every network-import chokepoint.
	svc.addKnownBoxKey(self, boxKey)
	svc.addKnownBoxSig(self, boxSig)
	svc.trustContact(self, pubKey, boxKey, boxSig, "echo-test")

	svc.knowledgeMu.RLock()
	cachedKey := svc.boxKeys[self]
	cachedSig := svc.boxSigs[self]
	svc.knowledgeMu.RUnlock()
	if cachedKey != "" || cachedSig != "" {
		t.Fatalf("echoed self box key must not re-enter knowledge maps, got key=%q sig=%q", cachedKey, cachedSig)
	}

	if selfRow, ok := svc.trust.trustedContacts()[self]; ok && (selfRow.BoxKey != "" || selfRow.BoxSignature != "") {
		t.Fatalf("echoed self box key must not re-enter trust store, got key=%q sig=%q", selfRow.BoxKey, selfRow.BoxSignature)
	}

	for _, contact := range svc.contactsFrame().Contacts {
		if contact.Address == self && contact.BoxKey != "" {
			t.Fatalf("contactsFrame redistributes echoed self box key: %q", contact.BoxKey)
		}
	}

	// Control: a foreign contact with a valid binding is still imported —
	// the guard targets ONLY self key material.
	peer, _ := identity.Generate()
	svc.addKnownBoxKey(peer.Address, identity.BoxPublicKeyBase64(peer.BoxPublicKey))
	svc.knowledgeMu.RLock()
	peerCached := svc.boxKeys[peer.Address]
	svc.knowledgeMu.RUnlock()
	if peerCached == "" {
		t.Fatal("foreign box key import must keep working")
	}
}

// TestTrustStoreRefreshesSelfRowOnPolicyFlip: a node that previously ran
// with DMs enabled has its box key persisted in the trust store. Flipping
// to relay-only must purge it on the next start — otherwise the stale key
// keeps leaking via fetch_contacts despite the new policy.
func TestTrustStoreRefreshesSelfRowOnPolicyFlip(t *testing.T) {
	t.Parallel()

	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}

	path := t.TempDir() + "/trust.json"

	withKey := trustedContact{
		Address:      id.Address,
		PubKey:       identity.PublicKeyBase64(id.PublicKey),
		BoxKey:       identity.BoxPublicKeyBase64(id.BoxPublicKey),
		BoxSignature: identity.SignBoxKeyBinding(id),
		Source:       "self",
	}
	if _, err := loadTrustStore(path, withKey); err != nil {
		t.Fatalf("seed trust store: %v", err)
	}

	withoutKey := trustedContact{
		Address: id.Address,
		PubKey:  identity.PublicKeyBase64(id.PublicKey),
		Source:  "self",
	}
	store, err := loadTrustStore(path, withoutKey)
	if err != nil {
		t.Fatalf("reload trust store: %v", err)
	}

	self, ok := store.trustedContacts()[id.Address]
	if !ok {
		t.Fatal("self contact missing after reload")
	}
	if self.BoxKey != "" || self.BoxSignature != "" {
		t.Fatalf("policy flip must purge persisted self box key, got box_key=%q box_sig=%q", self.BoxKey, self.BoxSignature)
	}
}
