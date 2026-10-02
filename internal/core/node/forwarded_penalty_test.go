package node

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/testutil/edforgery"
)

// forwarded_penalty_test.go pins owner decision №27 on the paths that are not
// signature checks: content the immediate peer FORWARDS — a non-DM message of
// some other author, a receipt for some other recipient — is refused in
// silence. What this node does not know (the author's key, a subscription that
// ended a moment ago) is this node's state, not the forwarder's violation.

// requireBanSurfaceLive proves the IP ban observable works on the routable
// fixture, so a zero read elsewhere is a measurement and not a broken probe.
func requireBanSurfaceLive(t *testing.T) {
	t.Helper()
	svc, _, connID := newRoutableDatagramInboundFixture(t, domain.CapMeshDatagramV1)
	decoy := injectDuplicateJSONKey(t, strings.TrimSuffix(mustDatagramLine(t, newNodeDatagram(t, nil)), "\n"), `"ttl":9`)
	svc.dispatchNetworkFrame(connID, decoy)
	if banScoreForIP(svc, datagramTestPeerIP) == 0 {
		t.Fatal("the ban surface is unreachable on this fixture, so a zero ban score proves nothing")
	}
}

func nonDMPush(sender, id string) protocol.Frame {
	return protocol.Frame{
		Type:  "push_message",
		Topic: "global",
		Item: &protocol.MessageFrame{
			ID:         id,
			Sender:     sender,
			Recipient:  "*",
			Flag:       string(protocol.MessageFlagImmutable),
			CreatedAt:  time.Now().UTC().Format(time.RFC3339),
			TTLSeconds: 300,
			Body:       "relayed",
		},
	}
}

// senderKeySyncScheduled reports whether a key-sync pass for sender is in
// flight or has already run.
func senderKeySyncScheduled(svc *Service, sender string) bool {
	svc.senderKeySyncMu.Lock()
	defer svc.senderKeySyncMu.Unlock()
	_, inFlight := svc.senderKeySyncInFlight[sender]
	_, ran := svc.senderKeySyncLastRun[sender]
	return inFlight || ran
}

func waitSenderKeySyncScheduled(t *testing.T, svc *Service, sender string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !senderKeySyncScheduled(svc, sender) {
		if time.Now().After(deadline) {
			t.Fatalf("no key sync was scheduled for %s", sender)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestNonDMFromUnknownAuthorThroughRelayCostsTheRelayNothing: A knows author
// S, B (this node) does not. Twenty non-DM messages of S relayed by A used to
// cost A 2000 ban points at B — a 24-hour blacklist for relaying. They are now
// refused without a ban, counted, and B starts one key-sync pass for S.
func TestNonDMFromUnknownAuthorThroughRelayCostsTheRelayNothing(t *testing.T) {
	t.Parallel()
	requireBanSurfaceLive(t)

	svc, _, connID := newRoutableDatagramInboundFixture(t)
	author, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	const messages = 20
	for i := 0; i < messages; i++ {
		svc.handleInboundPushMessage(connID, nonDMPush(author.Address, fmt.Sprintf("relayed-%d", i)))
	}

	if got := banScoreForIP(svc, datagramTestPeerIP); got != 0 {
		t.Fatalf("ban score = %d: the relay is not the author of what it forwarded", got)
	}
	if got := svc.unattributedNonDMDrops.Load(); got != messages {
		t.Fatalf("unattributed non-DM drops = %d, want %d", got, messages)
	}
	svc.gossipMu.RLock()
	stored := svc.seen.Has("relayed-0")
	svc.gossipMu.RUnlock()
	if stored {
		t.Fatal("a message of an unattributed author was stored")
	}
	// One pass for S, from the non-DM pool; the other nineteen triggers are
	// inside its budget and are skipped.
	if got := svc.nonDMKeySyncPasses.Load(); got != 1 {
		t.Fatalf("non-DM key sync passes = %d, want 1", got)
	}
}

// TestNonDMFromSmallOrderAuthorThroughOldRelayCostsNothing is the rollout
// hazard: an old relay knows the neutral key's address (it accepted the
// universal signature), this node refuses that key everywhere, so it never
// knows the author — and must not ban the old relay for forwarding `global`.
func TestNonDMFromSmallOrderAuthorThroughOldRelayCostsNothing(t *testing.T) {
	t.Parallel()
	requireBanSurfaceLive(t)

	svc, _, connID := newRoutableDatagramInboundFixture(t)
	for i := 0; i < 10; i++ {
		svc.handleInboundPushMessage(connID, nonDMPush(forgedIdentityAddress(), fmt.Sprintf("small-order-%d", i)))
	}
	if got := banScoreForIP(svc, datagramTestPeerIP); got != 0 {
		t.Fatalf("ban score = %d after relaying a small-order author's global messages", got)
	}
}

// TestPushDeliveryReceiptForUnsubscribedRecipientCostsTheRelayNothing: the
// subscriber the receipt was meant for unsubscribed between the relay's send
// and the receipt's arrival. Whether anyone is subscribed is this node's state;
// the receipt is refused without a ban.
func TestPushDeliveryReceiptForUnsubscribedRecipientCostsTheRelayNothing(t *testing.T) {
	t.Parallel()
	requireBanSurfaceLive(t)

	svc, _, connID := newRoutableDatagramInboundFixture(t)
	client, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	svc.gossipMu.Lock()
	svc.subs[client.Address] = map[string]*subscriber{"sub-1": {id: "sub-1", recipient: client.Address}}
	svc.gossipMu.Unlock()
	if !svc.hasSubscriber(client.Address) {
		t.Fatal("the subscriber was not registered")
	}
	svc.gossipMu.Lock()
	delete(svc.subs, client.Address)
	svc.gossipMu.Unlock()

	svc.handleInboundPushDeliveryReceipt(connID, protocol.Frame{
		Type: "push_delivery_receipt",
		Receipt: &protocol.ReceiptFrame{
			MessageID:   "msg-after-unsubscribe",
			Sender:      svc.identity.Address,
			Recipient:   client.Address,
			Status:      protocol.ReceiptStatusDelivered,
			DeliveredAt: time.Now().UTC().Format(time.RFC3339),
		},
	})

	if got := banScoreForIP(svc, datagramTestPeerIP); got != 0 {
		t.Fatalf("ban score = %d for a receipt whose subscriber just left", got)
	}
	svc.deliveryMu.RLock()
	stored := len(svc.receipts[client.Address])
	svc.deliveryMu.RUnlock()
	if stored != 0 {
		t.Fatalf("a receipt for nobody here was stored %d times", stored)
	}
}

// TestTrustedContactWithSmallOrderKeyIsNotLoadedNorExported: a trust file
// written before the key check holds a contact under the neutral key. Loading
// keeps the file intact but never puts the key into the knowledge maps, so it
// is neither attached to frames nor offered to peers in `contacts`, and the
// local trusted-contact list does not hand it out either. The honest contact
// next to it is the compatibility control.
func TestTrustedContactWithSmallOrderKeyIsNotLoadedNorExported(t *testing.T) {
	t.Parallel()
	honest, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	forged := forgedIdentityAddress()
	now := time.Now().UTC()
	file := trustFile{
		Version: trustFileVersion,
		Contacts: map[string]trustedContact{
			forged: {
				Address:      forged,
				PubKey:       forgedKeyBase64(),
				BoxKey:       base64.StdEncoding.EncodeToString(make([]byte, 32)),
				BoxSignature: base64.RawURLEncoding.EncodeToString(edforgery.UniversalSignature()),
				FirstSeenAt:  now,
				LastSeenAt:   now,
				Source:       "pre-check",
			},
			honest.Address: {
				Address:      honest.Address,
				PubKey:       identity.PublicKeyBase64(honest.PublicKey),
				BoxKey:       identity.BoxPublicKeyBase64(honest.BoxPublicKey),
				BoxSignature: identity.SignBoxKeyBinding(honest),
				FirstSeenAt:  now,
				LastSeenAt:   now,
				Source:       "pre-check",
			},
		},
	}
	payload, err := json.Marshal(file)
	if err != nil {
		t.Fatalf("marshal trust file: %v", err)
	}
	path := filepath.Join(t.TempDir(), "trust.json")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatalf("write trust file: %v", err)
	}

	svc := NewService(config.Node{
		ListenAddress:  "127.0.0.1:0",
		Type:           config.NodeTypeFull,
		TrustStorePath: path,
	}, testIdentityForNetworkConsumerTest(t), nil)
	t.Cleanup(svc.WaitBackground)

	svc.knowledgeMu.RLock()
	_, forgedKnown := svc.pubKeys[forged]
	_, forgedBox := svc.boxKeys[forged]
	_, honestKnown := svc.pubKeys[honest.Address]
	svc.knowledgeMu.RUnlock()
	if forgedKnown || forgedBox {
		t.Fatal("a small-order contact key entered the knowledge maps")
	}
	if !honestKnown {
		t.Fatal("the honest contact was not loaded")
	}
	if got := svc.keyHygieneStats().RefusedTrustedContacts; got != 1 {
		t.Fatalf("refused trusted contacts = %d, want 1", got)
	}

	frame := protocol.Frame{}
	svc.attachKnownSenderKeys(&frame, "dm", forged)
	if frame.PubKey != "" {
		t.Fatal("the small-order key was attached to an outgoing frame")
	}

	assertContacts := func(name string, reply protocol.Frame) {
		t.Helper()
		var sawHonest bool
		for _, contact := range reply.Contacts {
			if contact.Address == forged {
				t.Fatalf("%s offers the small-order contact", name)
			}
			sawHonest = sawHonest || contact.Address == honest.Address
		}
		if !sawHonest {
			t.Fatalf("%s lost the honest contact", name)
		}
	}
	assertContacts("contacts (wire)", svc.contactsFrameForNetwork())
	assertContacts("fetch_trusted_contacts", svc.trustedContactsFrame())

	if !svc.trust.isTrustedContact(domain.PeerIdentityFromWire(forged)) {
		t.Fatal("the refused contact was dropped from the trust store; the file must keep it")
	}
}
