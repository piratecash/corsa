package node

// attached_sender_keys_test.go locks the v27 self-certifying sender-key
// contract (config.ProtocolVersionDMSenderKeys): DM transport frames
// carry the origin sender's PUBLIC key triple, the final recipient
// validates it against the sender address (the address IS the signing
// key's fingerprint) and imports it, and transit hops copy the triple
// through — or backfill it from local knowledge for keyless frames from
// pre-v27 origins. This is what makes a FIRST-CONTACT DM deliverable
// over relay hops that never met the sender, where the on-demand
// fetch_contacts recovery (previous hop only) has nothing to serve.

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// senderKeyTriple returns the wire form of id's public key material as
// attached to DM transport frames.
func senderKeyTriple(id *identity.Identity) (pubKey, boxKey, boxSig string) {
	return identity.PublicKeyBase64(id.PublicKey),
		identity.BoxPublicKeyBase64(id.BoxPublicKey),
		identity.SignBoxKeyBinding(id)
}

// TestRelayDeliverAttachedSenderKeysFirstContact is the core first-contact
// regression: the recipient has NEVER seen the sender (no handshake, no
// contact sync, empty knowledge maps for that address), the previous hop
// is not dialable — exactly the state that used to end in the
// unknown-sender-key reject loop. With the attached triple the envelope
// must verify and deliver locally, and the keys must land in the
// knowledge maps so the recipient can verify follow-ups and reply.
func TestRelayDeliverAttachedSenderKeysFirstContact(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	// Deliberately NOT registered on svc — first contact.
	body := sealDMBody(t, sender, svc.Address(), identity.BoxPublicKeyBase64(svc.identity.BoxPublicKey))
	pubKey, boxKey, boxSig := senderKeyTriple(sender)

	frame := protocol.Frame{
		Type:        "relay_message",
		ID:          "attached-keys-first-contact-1",
		Address:     sender.Address,
		Recipient:   svc.Address(),
		Topic:       "dm",
		Body:        body,
		Flag:        string(protocol.MessageFlagImmutable),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		HopCount:    2,
		MaxHops:     10,
		PreviousHop: "transit-hop-fingerprint",
		PubKey:      pubKey,
		BoxKey:      boxKey,
		BoxSig:      boxSig,
	}

	// Previous hop transport address is intentionally undialable-fast
	// (connection refused): a fallback sync attempt would fail, so a
	// "delivered" status proves the attached keys did the work.
	status := svc.handleRelayMessage(domain.PeerAddress("127.0.0.1:1"), nil, frame)
	if status != "delivered" {
		t.Fatalf("first-contact relay with attached keys: got status %q, want \"delivered\"", status)
	}

	svc.knowledgeMu.RLock()
	gotPub := svc.pubKeys[sender.Address]
	gotBox := svc.boxKeys[sender.Address]
	gotSig := svc.boxSigs[sender.Address]
	svc.knowledgeMu.RUnlock()
	if gotPub != pubKey {
		t.Errorf("sender pubkey not imported: got %q", gotPub)
	}
	if gotBox != boxKey || gotSig != boxSig {
		t.Errorf("sender box key pair not imported: box=%q sig=%q", gotBox, gotSig)
	}
}

// TestRelayDeliverAttachedKeysFingerprintMismatchRejected verifies the
// self-certification gate: a triple whose signing key does not
// fingerprint-match the sender address must be ignored — nothing enters
// the knowledge maps and the message stays rejected (unknown sender),
// exactly as if no keys were attached.
func TestRelayDeliverAttachedKeysFingerprintMismatchRejected(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate (sender): %v", err)
	}
	imposter, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate (imposter): %v", err)
	}
	body := sealDMBody(t, sender, svc.Address(), identity.BoxPublicKeyBase64(svc.identity.BoxPublicKey))
	// Attach the IMPOSTER's key material under the sender's address.
	pubKey, boxKey, boxSig := senderKeyTriple(imposter)

	frame := protocol.Frame{
		Type:        "relay_message",
		ID:          "attached-keys-mismatch-1",
		Address:     sender.Address,
		Recipient:   svc.Address(),
		Topic:       "dm",
		Body:        body,
		Flag:        string(protocol.MessageFlagImmutable),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		HopCount:    2,
		MaxHops:     10,
		PreviousHop: "transit-hop-fingerprint",
		PubKey:      pubKey,
		BoxKey:      boxKey,
		BoxSig:      boxSig,
	}

	// 127.0.0.1:1 → the fallback key sync dial fails fast (refused).
	status := svc.handleRelayMessage(domain.PeerAddress("127.0.0.1:1"), nil, frame)
	if status != "" {
		t.Fatalf("mismatched attached keys: got status %q, want \"\" (rejected)", status)
	}

	svc.knowledgeMu.RLock()
	gotPub := svc.pubKeys[sender.Address]
	svc.knowledgeMu.RUnlock()
	if gotPub != "" {
		t.Errorf("mismatched pubkey must not be imported, got %q", gotPub)
	}
}

// TestRelayForwardPreservesAttachedSenderKeys verifies the transit
// contract: a forwarding hop copies the attached triple through
// verbatim, even when it knows nothing about the origin sender —
// stripping it would break first-contact verification on the final
// recipient.
func TestRelayForwardPreservesAttachedSenderKeys(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate (sender): %v", err)
	}
	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate (recipient): %v", err)
	}
	pubKey, boxKey, boxSig := senderKeyTriple(sender)

	// Direct session to the recipient so the forward takes the
	// direct-peer path and the frame is capturable from sendCh.
	forwarded := make(chan protocol.Frame, 1)
	svc.peerMu.Lock()
	svc.sessions[domain.PeerAddress("addr-fwd")] = &peerSession{
		address:      "addr-fwd",
		peerIdentity: domain.PeerIdentityFromWire(recipientID.Address),
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
		sendCh:       forwarded,
	}
	svc.health[domain.PeerAddress("addr-fwd")] = &peerHealth{Connected: true}
	svc.peerMu.Unlock()

	frame := protocol.Frame{
		Type:        "relay_message",
		ID:          "attached-keys-forward-1",
		Address:     sender.Address,
		Recipient:   recipientID.Address,
		Topic:       "dm",
		Body:        "opaque-transit-ciphertext",
		Flag:        string(protocol.MessageFlagImmutable),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		HopCount:    1,
		MaxHops:     10,
		TTLSeconds:  300,
		PreviousHop: "origin-fingerprint",
		PubKey:      pubKey,
		BoxKey:      boxKey,
		BoxSig:      boxSig,
	}

	status := svc.handleRelayMessage(domain.PeerAddress("10.0.0.1:64646"), nil, frame)
	if status != "forwarded" {
		t.Fatalf("expected \"forwarded\", got %q", status)
	}
	select {
	case out := <-forwarded:
		if out.PubKey != pubKey || out.BoxKey != boxKey || out.BoxSig != boxSig {
			t.Errorf("attached keys not preserved in forward: pub=%q box=%q sig=%q", out.PubKey, out.BoxKey, out.BoxSig)
		}
	default:
		t.Fatalf("no frame captured on the forward session")
	}
}

// TestRelayForwardBackfillsKnownSenderKeys covers the mixed-network
// bridge: a pre-v27 origin sends the frame keyless, but THIS hop knows
// the origin's keys (e.g. cached from a direct handshake) — the forward
// must attach them so a downstream first-contact recipient still gets
// the fast path.
func TestRelayForwardBackfillsKnownSenderKeys(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	sender := registerSenderKey(t, svc)
	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate (recipient): %v", err)
	}

	forwarded := make(chan protocol.Frame, 1)
	svc.peerMu.Lock()
	svc.sessions[domain.PeerAddress("addr-backfill")] = &peerSession{
		address:      "addr-backfill",
		peerIdentity: domain.PeerIdentityFromWire(recipientID.Address),
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
		sendCh:       forwarded,
	}
	svc.health[domain.PeerAddress("addr-backfill")] = &peerHealth{Connected: true}
	svc.peerMu.Unlock()

	frame := protocol.Frame{
		Type:        "relay_message",
		ID:          "attached-keys-backfill-1",
		Address:     sender.Address,
		Recipient:   recipientID.Address,
		Topic:       "dm",
		Body:        "opaque-transit-ciphertext",
		Flag:        string(protocol.MessageFlagImmutable),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		HopCount:    1,
		MaxHops:     10,
		TTLSeconds:  300,
		PreviousHop: "origin-fingerprint",
		// No key fields — legacy keyless origin.
	}

	status := svc.handleRelayMessage(domain.PeerAddress("10.0.0.1:64646"), nil, frame)
	if status != "forwarded" {
		t.Fatalf("expected \"forwarded\", got %q", status)
	}
	select {
	case out := <-forwarded:
		if out.PubKey != identity.PublicKeyBase64(sender.PublicKey) {
			t.Errorf("pubkey not backfilled: got %q", out.PubKey)
		}
		if out.BoxKey == "" || out.BoxSig == "" {
			t.Errorf("box pair not backfilled: box=%q sig=%q", out.BoxKey, out.BoxSig)
		}
	default:
		t.Fatalf("no frame captured on the forward session")
	}
}

// TestAttachKnownSenderKeysOptOutSelfOmitsBoxKey pins the relay-only DM
// opt-out interaction: an opt-out node attaches ONLY its signing key to
// its own outgoing DMs — recipients can verify its envelopes, but the
// box key needed to compose a DM back stays unpublished, preserving the
// opt-out contract (see Service.selfBoxKey).
func TestAttachKnownSenderKeysOptOutSelfOmitsBoxKey(t *testing.T) {
	t.Parallel()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	svc := NewService(config.Node{
		ListenAddress:         "127.0.0.1:64646",
		TrustStorePath:        t.TempDir() + "/trust.json",
		Type:                  config.NodeTypeFull,
		AllowPrivatePeers:     true,
		DisableDirectMessages: true,
	}, id, nil)
	t.Cleanup(svc.WaitBackground)

	frame := protocol.Frame{Type: "relay_message", Topic: "dm"}
	svc.attachKnownSenderKeys(&frame, "dm", svc.Address())
	if frame.PubKey == "" {
		t.Errorf("opt-out self attach: signing key must still be attached")
	}
	if frame.BoxKey != "" || frame.BoxSig != "" {
		t.Errorf("opt-out self attach must omit the box pair: box=%q sig=%q", frame.BoxKey, frame.BoxSig)
	}
}

// TestAttachKnownSenderKeysNonDMTopicNoop pins the topic gate: broadcast
// gossip frames must not carry key material (nothing to verify there,
// pure wire bloat).
func TestAttachKnownSenderKeysNonDMTopicNoop(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)
	sender := registerSenderKey(t, svc)

	frame := protocol.Frame{Type: "push_message", Topic: "global"}
	svc.attachKnownSenderKeys(&frame, "global", sender.Address)
	if frame.PubKey != "" || frame.BoxKey != "" || frame.BoxSig != "" {
		t.Errorf("non-DM topic must not carry keys: pub=%q box=%q sig=%q", frame.PubKey, frame.BoxKey, frame.BoxSig)
	}
}

// TestAttachedKeysNotImportedOnForgedEnvelope pins the
// verify-before-import ordering: a frame whose attached triple is
// perfectly valid (correct fingerprint, correct binding) but whose
// envelope signature does NOT verify must leave NO trace in the
// knowledge maps — no keys, no known-set entry, no IdentityAdded churn.
// Without that ordering an attacker generating throwaway identities
// could flood valid-fingerprint/forged-envelope frames and evict real
// cached contacts from the shared bounded LRU.
func TestAttachedKeysNotImportedOnForgedEnvelope(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	pubKey, boxKey, boxSig := senderKeyTriple(sender)

	frame := protocol.Frame{
		Type:        "relay_message",
		ID:          "attached-keys-forged-envelope-1",
		Address:     sender.Address,
		Recipient:   svc.Address(),
		Topic:       "dm",
		Body:        "definitely-not-a-valid-sealed-envelope",
		Flag:        string(protocol.MessageFlagImmutable),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		HopCount:    2,
		MaxHops:     10,
		PreviousHop: "transit-hop-fingerprint",
		PubKey:      pubKey,
		BoxKey:      boxKey,
		BoxSig:      boxSig,
	}

	status := svc.handleRelayMessage(domain.PeerAddress("127.0.0.1:1"), nil, frame)
	if status != "" {
		t.Fatalf("forged envelope must be rejected, got status %q", status)
	}

	svc.knowledgeMu.RLock()
	gotPub := svc.pubKeys[sender.Address]
	gotBox := svc.boxKeys[sender.Address]
	known := svc.known.Has(sender.Address)
	svc.knowledgeMu.RUnlock()
	if gotPub != "" || gotBox != "" {
		t.Errorf("keys must not be imported before envelope verification: pub=%q box=%q", gotPub, gotBox)
	}
	if known {
		t.Errorf("sender must not enter the known set on a forged envelope")
	}
}

// TestAttachedKeysBackfillMissingBoxPair pins the fill-only import for a
// PARTIALLY known sender: the pubkey arrived earlier (so the
// unknown-sender fallback sync never triggers again), but the box pair
// never did — the recipient could read but not reply. A delivered DM
// carrying the full triple must fill the missing box pair.
func TestAttachedKeysBackfillMissingBoxPair(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	pubKey, boxKey, boxSig := senderKeyTriple(sender)
	// Pre-register ONLY the signing key.
	svc.addKnownPubKey(sender.Address, pubKey)

	body := sealDMBody(t, sender, svc.Address(), identity.BoxPublicKeyBase64(svc.identity.BoxPublicKey))
	frame := protocol.Frame{
		Type:        "relay_message",
		ID:          "attached-keys-boxfill-1",
		Address:     sender.Address,
		Recipient:   svc.Address(),
		Topic:       "dm",
		Body:        body,
		Flag:        string(protocol.MessageFlagImmutable),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		HopCount:    2,
		MaxHops:     10,
		PreviousHop: "transit-hop-fingerprint",
		PubKey:      pubKey,
		BoxKey:      boxKey,
		BoxSig:      boxSig,
	}

	status := svc.handleRelayMessage(domain.PeerAddress("127.0.0.1:1"), nil, frame)
	if status != "delivered" {
		t.Fatalf("expected \"delivered\", got %q", status)
	}

	svc.knowledgeMu.RLock()
	gotBox := svc.boxKeys[sender.Address]
	gotSig := svc.boxSigs[sender.Address]
	svc.knowledgeMu.RUnlock()
	if gotBox != boxKey || gotSig != boxSig {
		t.Errorf("missing box pair must be backfilled from the delivered frame: box=%q sig=%q", gotBox, gotSig)
	}
}

// TestRelayForwardDropsOversizedAttachedKeys pins the transit-side
// length cap: a hostile origin stuffing megabytes into the key fields
// (which bypass the Body admission cap) must not have them amplified
// across the mesh — the forward strips fields exceeding
// maxAttachedKeyFieldLen. Oversized fields can never validate on the
// recipient anyway.
func TestRelayForwardDropsOversizedAttachedKeys(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	recipientID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate (recipient): %v", err)
	}

	forwarded := make(chan protocol.Frame, 1)
	svc.peerMu.Lock()
	svc.sessions[domain.PeerAddress("addr-oversize")] = &peerSession{
		address:      "addr-oversize",
		peerIdentity: domain.PeerIdentityFromWire(recipientID.Address),
		capabilities: []domain.Capability{domain.CapMeshRelayV1},
		sendCh:       forwarded,
	}
	svc.health[domain.PeerAddress("addr-oversize")] = &peerHealth{Connected: true}
	svc.peerMu.Unlock()

	huge := strings.Repeat("A", 64*1024)
	frame := protocol.Frame{
		Type:        "relay_message",
		ID:          "attached-keys-oversize-1",
		Address:     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Recipient:   recipientID.Address,
		Topic:       "dm",
		Body:        "opaque-transit-ciphertext",
		Flag:        string(protocol.MessageFlagImmutable),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		HopCount:    1,
		MaxHops:     10,
		TTLSeconds:  300,
		PreviousHop: "origin-fingerprint",
		PubKey:      huge,
		BoxKey:      huge,
		BoxSig:      huge,
	}

	status := svc.handleRelayMessage(domain.PeerAddress("10.0.0.1:64646"), nil, frame)
	if status != "forwarded" {
		t.Fatalf("expected \"forwarded\", got %q", status)
	}
	select {
	case out := <-forwarded:
		if out.PubKey != "" || out.BoxKey != "" || out.BoxSig != "" {
			t.Errorf("oversized key fields must be stripped on forward: pub=%d box=%d sig=%d bytes",
				len(out.PubKey), len(out.BoxKey), len(out.BoxSig))
		}
	default:
		t.Fatalf("no frame captured on the forward session")
	}
}

// TestTriggerSenderKeySyncAsyncSingleFlight verifies that a burst of
// recovery triggers for the same previous-hop address (the exact wedge
// scenario: many undeliverable messages from one hop) produces exactly
// one background dial. The mock listener accepts and holds connections
// without responding; the sync goroutine times out on its idle deadline.
func TestTriggerSenderKeySyncAsyncSingleFlight(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	var accepts atomic.Int64
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepts.Add(1)
			// Hold the connection open, never respond — the dialer
			// times out on its idle deadline.
			go func(c net.Conn) {
				time.Sleep(3 * time.Second)
				_ = c.Close()
			}(c)
		}
	}()

	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	addr := domain.PeerAddress(ln.Addr().String())
	for i := 0; i < 10; i++ {
		svc.triggerSenderKeySyncAsync(addr, sender.Address, nil)
	}

	// Give the (single) goroutine time to dial; a second dial would also
	// have landed well within this window if single-flight were broken.
	time.Sleep(500 * time.Millisecond)
	if got := accepts.Load(); got != 1 {
		t.Fatalf("expected exactly 1 recovery dial for a trigger burst, got %d", got)
	}
}

// TestSenderKeySyncFanoutRecoversFromOtherPeer covers the mixed-network
// / undialable-previous-hop recovery: the previous hop cannot serve the
// sender's contact (here: not even dialable), but another peer with a
// live outbound session can. The background recovery pass must fan out
// past the previous hop and import the sender's keys from the second
// candidate.
func TestSenderKeySyncFanoutRecoversFromOtherPeer(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	contact := protocol.ContactFrame{
		Address: sender.Address,
		PubKey:  identity.PublicKeyBase64(sender.PublicKey),
		BoxKey:  identity.BoxPublicKeyBase64(sender.BoxPublicKey),
		BoxSig:  identity.SignBoxKeyBinding(sender),
	}

	// The fan-out candidate: a mock peer that serves the sender's
	// contact over the standard sync handshake.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	defer func() { _ = ln.Close() }()
	go interleavedSyncPeerMockServer(t, ln, 0, nil, nil, []protocol.ContactFrame{contact})

	candidateAddr := domain.PeerAddress(ln.Addr().String())
	svc.peerMu.Lock()
	svc.sessions[candidateAddr] = &peerSession{address: candidateAddr}
	svc.health[candidateAddr] = &peerHealth{Connected: true}
	svc.peerMu.Unlock()

	// Previous hop is undialable-fast (connection refused): the pass
	// must move on to the session-backed candidate.
	svc.triggerSenderKeySyncAsync(domain.PeerAddress("127.0.0.1:1"), sender.Address, nil)

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if svc.hasSenderPubKey(sender.Address) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("sender keys not recovered via fan-out candidate")
}

// TestRequestOwnedContactSyncUnavailable pins the failure modes of the
// owner-serialised sync request: nil session, a manually-built session
// without the channel, and a live channel nobody owns (dead loop) must
// all report "not executed" instead of blocking the recovery pass.
func TestRequestOwnedContactSyncUnavailable(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	if _, ok := svc.requestOwnedContactSync(context.Background(), nil, 50*time.Millisecond); ok {
		t.Errorf("nil session must not execute")
	}
	if _, ok := svc.requestOwnedContactSync(context.Background(), &peerSession{address: "no-channel"}, 50*time.Millisecond); ok {
		t.Errorf("session without contactSyncCh must not execute")
	}
	dead := &peerSession{address: "dead-owner", contactSyncCh: make(chan chan int)}
	start := time.Now()
	if _, ok := svc.requestOwnedContactSync(context.Background(), dead, 50*time.Millisecond); ok {
		t.Errorf("ownerless session must time out, not execute")
	}
	if time.Since(start) > 2*time.Second {
		t.Errorf("ownerless request took too long: %v", time.Since(start))
	}
}

// TestTriggerSenderKeySyncOwnedSessionFirst pins the recovery order for
// a NATed / undialable previous hop: the pass must run the
// owner-serialised sync over the provided live session FIRST, and a
// successful import there terminates the pass without any fresh dials.
// The stub owner stands in for the servePeerSession contactSyncCh arm.
func TestTriggerSenderKeySyncOwnedSessionFirst(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	owned := &peerSession{address: "owned-natted-hop", contactSyncCh: make(chan chan int)}
	served := make(chan struct{})
	go func() {
		reply := <-owned.contactSyncCh
		// Emulate the serve-loop arm outcome: the previous hop's
		// contacts contained the sender.
		svc.addKnownIdentity(domain.PeerIdentityFromWire(sender.Address))
		svc.addKnownPubKey(sender.Address, identity.PublicKeyBase64(sender.PublicKey))
		reply <- 1
		close(served)
	}()

	// Previous hop transport address is undialable-fast: if the owned
	// path were skipped, the pass could only fail (no other sessions).
	svc.triggerSenderKeySyncAsync(domain.PeerAddress("127.0.0.1:1"), sender.Address, owned)

	select {
	case <-served:
	case <-time.After(5 * time.Second):
		t.Fatalf("owned-session sync request never reached the owner")
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if svc.hasSenderPubKey(sender.Address) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("sender key not recovered via owned session")
}

// TestOwnedContactSyncOverLiveServeLoop is the end-to-end smoke for the
// servePeerSession contactSyncCh arm: a REAL outbound session between
// two nodes executes an owner-serialised fetch_contacts on behalf of an
// external requester.
func TestOwnedContactSyncOverLiveServeLoop(t *testing.T) {
	t.Parallel()

	addressA := freeAddress(t)
	addressB := freeAddress(t)

	nodeA, stopA := startTestNode(t, config.Node{
		ListenAddress:  addressA,
		BootstrapPeers: []string{},
		Type:           domain.NodeTypeFull,
	})
	defer stopA()

	nodeB, stopB := startTestNode(t, config.Node{
		ListenAddress:  addressB,
		BootstrapPeers: []string{normalizeAddress(addressA)},
		Type:           domain.NodeTypeFull,
	})
	defer stopB()

	// Wait for nodeB's outbound session to nodeA to be live.
	var session *peerSession
	waitForCondition(t, 8*time.Second, func() bool {
		nodeB.peerMu.RLock()
		defer nodeB.peerMu.RUnlock()
		for _, sess := range nodeB.sessions {
			if sess != nil && sess.contactSyncCh != nil {
				session = sess
				return true
			}
		}
		return false
	})

	// Register a contact on nodeA AFTER session setup so only an
	// on-demand owned sync can deliver it to nodeB.
	senderID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	nodeA.knowledgeMu.Lock()
	nodeA.pubKeys[senderID.Address] = identity.PublicKeyBase64(senderID.PublicKey)
	nodeA.boxKeys[senderID.Address] = identity.BoxPublicKeyBase64(senderID.BoxPublicKey)
	nodeA.boxSigs[senderID.Address] = identity.SignBoxKeyBinding(senderID)
	nodeA.known.Add(senderID.Address)
	nodeA.knowledgeMu.Unlock()

	imported, ok := nodeB.requestOwnedContactSync(context.Background(), session, 5*time.Second)
	if !ok {
		t.Fatalf("owned contact sync was not executed by the live serve loop")
	}
	if imported < 1 {
		t.Fatalf("expected at least 1 imported contact via owned sync, got %d", imported)
	}
	if !nodeB.hasSenderPubKey(senderID.Address) {
		t.Fatalf("sender key not present on nodeB after owned sync")
	}
}

// TestSenderKeySyncGlobalCap pins the global concurrency bound: frames
// with DISTINCT fabricated senders must not spawn unbounded recovery
// passes — beyond maxConcurrentSenderKeySyncPasses the trigger drops
// the request outright.
func TestSenderKeySyncGlobalCap(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	// Occupy every slot with synthetic in-flight passes.
	svc.senderKeySyncMu.Lock()
	if svc.senderKeySyncInFlight == nil {
		svc.senderKeySyncInFlight = make(map[string]struct{})
	}
	for i := 0; i < maxConcurrentSenderKeySyncPasses; i++ {
		svc.senderKeySyncInFlight[fmt.Sprintf("occupied-%d", i)] = struct{}{}
	}
	svc.senderKeySyncMu.Unlock()

	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	svc.triggerSenderKeySyncAsync(domain.PeerAddress("127.0.0.1:1"), sender.Address, nil)

	svc.senderKeySyncMu.Lock()
	_, spawned := svc.senderKeySyncInFlight[sender.Address]
	total := len(svc.senderKeySyncInFlight)
	svc.senderKeySyncMu.Unlock()
	if spawned {
		t.Errorf("trigger above the global cap must be dropped, not spawned")
	}
	if total != maxConcurrentSenderKeySyncPasses {
		t.Errorf("in-flight set grew past the cap: %d", total)
	}
}

// TestRequestOwnedContactSyncHonoursCtx pins the lifecycle contract: a
// cancelled context aborts both waits immediately instead of holding
// the recovery goroutine (and WaitBackground on shutdown) hostage to
// the request timers.
func TestRequestOwnedContactSyncHonoursCtx(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ownerless := &peerSession{address: "ctx-owner", contactSyncCh: make(chan chan int)}
	start := time.Now()
	if _, ok := svc.requestOwnedContactSync(ctx, ownerless, 10*time.Second); ok {
		t.Errorf("cancelled ctx must not execute")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("cancelled ctx must abort immediately, took %v", elapsed)
	}
}

// TestOwnedContactSyncFailureTearsDownSession pins the no-zombie
// contract of the serve-loop arm DETERMINISTICALLY: the mock peer keeps
// the connection healthy through session setup, ACCEPTS the
// fetch_contacts request, and then closes instead of answering — so the
// error surfaces strictly INSIDE the contactSyncCh arm's
// peerSessionRequest (the generic errCh arm cannot fire first: no
// error exists before the request is read). The arm must answer the
// requester and TEAR DOWN the session; a swallowed error would leave a
// session whose reader goroutine already exited, and the next
// heartbeat would block forever.
func TestOwnedContactSyncFailureTearsDownSession(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	local, remote := net.Pipe()
	defer func() { _ = local.Close() }()

	peerAddr := domain.PeerAddress("10.0.0.60:64646")
	session := &peerSession{
		address:       peerAddr,
		conn:          local,
		sendCh:        make(chan protocol.Frame, 4),
		inboxCh:       make(chan protocol.Frame, 4),
		errCh:         make(chan error, 1),
		contactSyncCh: make(chan chan int),
	}
	attachTestNetCore(svc, session)

	svc.peerMu.Lock()
	svc.sessions[peerAddr] = session
	svc.health[peerAddr] = &peerHealth{Connected: true}
	svc.peerMu.Unlock()

	// Mock peer: read exactly the fetch_contacts request, then close
	// without replying — readPeerSession then reports EOF while the arm
	// is inside its peerSessionRequest wait.
	go func() {
		r := bufio.NewReader(remote)
		if _, err := r.ReadString('\n'); err == nil {
			_ = remote.Close()
		}
	}()

	go svc.readPeerSession(bufio.NewReader(local), session)
	done := make(chan error, 1)
	go func() {
		done <- svc.servePeerSession(context.Background(), session)
	}()

	imported, executed := svc.requestOwnedContactSync(context.Background(), session, 5*time.Second)
	if !executed {
		t.Fatalf("owned sync request was not executed by the serve loop")
	}
	if imported != 0 {
		t.Fatalf("failing owned sync must report 0 imported, got %d", imported)
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("servePeerSession must return the owned-sync error, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("zombie session: serve loop did not tear down after failing owned sync")
	}

	svc.peerMu.RLock()
	health := svc.health[peerAddr]
	svc.peerMu.RUnlock()
	if health != nil && health.Connected {
		t.Errorf("peer must be marked disconnected after owned-sync teardown")
	}
}

// TestSenderKeySyncRejectsNonCanonicalSender pins the format gate on
// the wire-supplied sender: an arbitrary (here: megabyte-sized) string
// must never become a key in the recovery maps — it is demoted to the
// address-keyed pass instead.
func TestSenderKeySyncRejectsNonCanonicalSender(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	giant := strings.Repeat("x", 1<<20)
	svc.triggerSenderKeySyncAsync(domain.PeerAddress("127.0.0.1:1"), giant, nil)

	svc.senderKeySyncMu.Lock()
	for k := range svc.senderKeySyncInFlight {
		if len(k) > 128 {
			t.Errorf("oversized sender leaked into in-flight map (len=%d)", len(k))
		}
	}
	for k := range svc.senderKeySyncLastRun {
		if len(k) > 128 {
			t.Errorf("oversized sender leaked into cooldown map (len=%d)", len(k))
		}
	}
	svc.senderKeySyncMu.Unlock()
}

// TestSenderKeySyncCooldownMapBounded pins the hard cap on the
// cooldown-stamp map: even a flood of unique young stamps must be
// evicted down to maxSenderKeySyncLastRunEntries at pass completion.
func TestSenderKeySyncCooldownMapBounded(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	svc.senderKeySyncMu.Lock()
	if svc.senderKeySyncLastRun == nil {
		svc.senderKeySyncLastRun = make(map[string]time.Time)
	}
	now := time.Now()
	for i := 0; i < maxSenderKeySyncLastRunEntries+500; i++ {
		// All stamps YOUNG (inside the cooldown) — the expired-first
		// sweep removes nothing, forcing the arbitrary eviction path.
		svc.senderKeySyncLastRun[fmt.Sprintf("young-%d", i)] = now
	}
	svc.pruneSenderKeySyncLastRunLocked()
	got := len(svc.senderKeySyncLastRun)
	svc.senderKeySyncMu.Unlock()
	if got > maxSenderKeySyncLastRunEntries {
		t.Fatalf("cooldown map exceeds hard cap after prune: %d > %d", got, maxSenderKeySyncLastRunEntries)
	}
}

// TestRelayMessageDispatchedDuringOwnedSync pins the data-plane
// no-loss contract of peerSessionRequest's wait loop: a relay_message
// arriving while the serve loop waits for a contacts reply (owner
// contact sync — a window of up to peerRequestTimeout) must be
// DISPATCHED, not discarded as an unexpected reply. The mock peer
// receives fetch_contacts, first pushes a fully-valid first-contact DM
// relay frame (attached v27 keys), then answers with contacts.
func TestRelayMessageDispatchedDuringOwnedSync(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	sender, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	body := sealDMBody(t, sender, svc.Address(), identity.BoxPublicKeyBase64(svc.identity.BoxPublicKey))
	pubKey, boxKey, boxSig := senderKeyTriple(sender)

	local, remote := net.Pipe()
	defer func() { _ = local.Close() }()
	defer func() { _ = remote.Close() }()

	peerAddr := domain.PeerAddress("10.0.0.61:64646")
	session := &peerSession{
		address:       peerAddr,
		conn:          local,
		sendCh:        make(chan protocol.Frame, 4),
		inboxCh:       make(chan protocol.Frame, 4),
		errCh:         make(chan error, 1),
		contactSyncCh: make(chan chan int),
		capabilities:  []domain.Capability{domain.CapMeshRelayV1},
	}
	attachTestNetCore(svc, session)

	svc.peerMu.Lock()
	svc.sessions[peerAddr] = session
	svc.health[peerAddr] = &peerHealth{Connected: true}
	svc.peerMu.Unlock()

	// Mock peer: on fetch_contacts, interleave a relay_message BEFORE
	// the contacts reply.
	go func() {
		r := bufio.NewReader(remote)
		if _, err := r.ReadString('\n'); err != nil {
			return
		}
		relayLine, _ := protocol.MarshalFrameLine(protocol.Frame{
			Type:        "relay_message",
			ID:          "owned-sync-interleaved-dm-1",
			Address:     sender.Address,
			Recipient:   svc.Address(),
			Topic:       "dm",
			Body:        body,
			Flag:        string(protocol.MessageFlagImmutable),
			CreatedAt:   time.Now().UTC().Format(time.RFC3339),
			HopCount:    2,
			MaxHops:     10,
			PreviousHop: "transit-hop-fingerprint",
			PubKey:      pubKey,
			BoxKey:      boxKey,
			BoxSig:      boxSig,
		})
		_, _ = remote.Write([]byte(relayLine))
		contactsLine, _ := protocol.MarshalFrameLine(protocol.Frame{Type: "contacts"})
		_, _ = remote.Write([]byte(contactsLine))
	}()

	go svc.readPeerSession(bufio.NewReader(local), session)
	go func() { _ = svc.servePeerSession(context.Background(), session) }()

	if _, executed := svc.requestOwnedContactSync(context.Background(), session, 5*time.Second); !executed {
		t.Fatalf("owned sync was not executed")
	}

	// The interleaved relay DM must have been delivered, not dropped:
	// its attached keys land in the knowledge maps on delivery.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if svc.hasSenderPubKey(sender.Address) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("relay_message interleaved during owned sync was dropped")
}

// TestSenderKeySyncPerHopFairness pins the anti-starvation slot: one
// previous hop may hold at most ONE concurrent recovery pass, so a
// hostile hop flooding unique well-formed senders cannot occupy the
// whole global cap — a trigger for a different hop must still pass.
func TestSenderKeySyncPerHopFairness(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	hostileHop := domain.PeerAddress("10.9.9.9:64646")
	otherHop := domain.PeerAddress("127.0.0.1:1")

	// Simulate an in-flight pass for the hostile hop (address-keyed:
	// no session/via identity is resolvable for it in this fixture).
	svc.senderKeySyncMu.Lock()
	if svc.senderKeySyncHopInFlight == nil {
		svc.senderKeySyncHopInFlight = make(map[string]struct{})
	}
	if svc.senderKeySyncInFlight == nil {
		svc.senderKeySyncInFlight = make(map[string]struct{})
	}
	svc.senderKeySyncHopInFlight["addr:"+string(hostileHop)] = struct{}{}
	svc.senderKeySyncInFlight["occupied-sender"] = struct{}{}
	svc.senderKeySyncMu.Unlock()

	senderA, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}
	senderB, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate: %v", err)
	}

	// Second sender via the SAME hostile hop: hop slot busy → dropped.
	svc.triggerSenderKeySyncAsync(hostileHop, senderA.Address, nil)
	svc.senderKeySyncMu.Lock()
	_, spawnedA := svc.senderKeySyncInFlight[senderA.Address]
	svc.senderKeySyncMu.Unlock()
	if spawnedA {
		t.Errorf("second pass for a busy hop must be dropped")
	}

	// A sender via a DIFFERENT hop must still get a slot.
	svc.triggerSenderKeySyncAsync(otherHop, senderB.Address, nil)
	deadline := time.Now().Add(2 * time.Second)
	seen := false
	for time.Now().Before(deadline) {
		svc.senderKeySyncMu.Lock()
		_, inFlight := svc.senderKeySyncInFlight[senderB.Address]
		_, done := svc.senderKeySyncLastRun[senderB.Address]
		svc.senderKeySyncMu.Unlock()
		if inFlight || done {
			seen = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !seen {
		t.Errorf("pass via a different hop was starved by the hostile hop")
	}
}

// TestSenderKeySyncHopFairnessKeyedByIdentity pins the alias bypass:
// one authenticated identity holding connections under DIFFERENT
// transport addresses must still occupy a single fairness slot — the
// slot keys on the session's peer identity, not the address string.
func TestSenderKeySyncHopFairnessKeyedByIdentity(t *testing.T) {
	t.Parallel()
	svc := newTestService(t, config.NodeTypeFull)

	hopID, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate (hop): %v", err)
	}
	senderA, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate (senderA): %v", err)
	}
	senderB, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity.Generate (senderB): %v", err)
	}

	// Same authenticated identity behind two different addresses.
	hopIdentity := domain.PeerIdentityFromWire(hopID.Address)
	sessionAlias1 := &peerSession{address: "10.7.7.1:64646", peerIdentity: hopIdentity}
	sessionAlias2 := &peerSession{address: "10.7.7.2:64647", peerIdentity: hopIdentity}

	// Occupy the identity-keyed slot as if a pass via alias 1 runs.
	svc.senderKeySyncMu.Lock()
	if svc.senderKeySyncHopInFlight == nil {
		svc.senderKeySyncHopInFlight = make(map[string]struct{})
	}
	if svc.senderKeySyncInFlight == nil {
		svc.senderKeySyncInFlight = make(map[string]struct{})
	}
	svc.senderKeySyncHopInFlight["id:"+hopIdentity.String()] = struct{}{}
	svc.senderKeySyncMu.Unlock()
	_ = sessionAlias1

	// Trigger via alias 2 — SAME identity, different address: must be
	// dropped by the identity-keyed slot.
	svc.triggerSenderKeySyncAsync(sessionAlias2.address, senderA.Address, sessionAlias2)
	svc.senderKeySyncMu.Lock()
	_, spawned := svc.senderKeySyncInFlight[senderA.Address]
	svc.senderKeySyncMu.Unlock()
	if spawned {
		t.Errorf("alias address must not bypass the identity-keyed fairness slot")
	}

	// A pass via an UNRELATED hop still goes through.
	svc.triggerSenderKeySyncAsync(domain.PeerAddress("127.0.0.1:1"), senderB.Address, nil)
	deadline := time.Now().Add(2 * time.Second)
	seen := false
	for time.Now().Before(deadline) {
		svc.senderKeySyncMu.Lock()
		_, inFlight := svc.senderKeySyncInFlight[senderB.Address]
		_, done := svc.senderKeySyncLastRun[senderB.Address]
		svc.senderKeySyncMu.Unlock()
		if inFlight || done {
			seen = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !seen {
		t.Errorf("unrelated hop was starved by the identity slot")
	}
}
