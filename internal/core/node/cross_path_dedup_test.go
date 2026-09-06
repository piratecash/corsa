package node

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/chatlog"
	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/storage"
	"github.com/piratecash/corsa/internal/core/storage/migrations"
)

// One logical message, several delivery paths, one row and one notification.
//
// # WHAT THIS FILE DOES AND DOES NOT ESTABLISH
//
// It exercises the paths that exist TODAY: a directed relay frame, a pushed
// frame, and the retry that sends both in one tick. Today those are all mesh —
// the datagram plane carries identity discovery and dm_control (reactions), and
// no user DM body travels on it — so **this is NOT a demonstration of
// cross-plane deduplication**. That claim belongs to the step where a second
// plane actually delivers a DM, and it is carried as an exit criterion of
// steps 09/10 rather than assumed here.
//
// What it does establish is the property those steps will depend on: a message
// that arrives twice by different routes is ONE message to the node — one
// stored row, one message.new — and two DIFFERENT messages are not folded into
// one by the same machinery.
//
// The sink is a REAL chatlog store, not a stub returning a canned verdict:
// "one row" is a statement about the database's primary key, and a fake that
// answers StoreDuplicate on the second call would pass whether or not the row
// is actually keyed on the identifier. What the real production adapter adds on
// top — the deferred-write and deleted-id gates of MessageStoreAdapter — is NOT
// in this loop, and that limit is stated rather than implied.
//
// Reference: docs/refactoring/dht/20-cross-plane-dedup.md.

// chatlogBackedStore is the node-side MessageStore backed by a real chatlog.
//
// It mirrors the production mapping — inserted ⇒ StoreInserted, ignored ⇒
// StoreDuplicate — because that mapping is what turns the database's primary
// key into "the UI is told once". The mapping lives in
// service.MessageStoreAdapter, which this package cannot import (service
// imports node), so it is reproduced here and its divergence is a known limit
// of the fixture, named in the file header.
type chatlogBackedStore struct {
	t     *testing.T
	store *chatlog.Store
	self  domain.PeerIdentity

	mu     sync.Mutex
	stored []protocol.Envelope
}

func newChatlogBackedStore(t *testing.T, self domain.PeerIdentity) *chatlogBackedStore {
	t.Helper()
	database, err := storage.Open(context.Background(), storage.Config{
		ExplicitPath: filepath.Join(t.TempDir(), "state.db"),
		Owner:        self,
		Catalog:      migrations.Catalog(),
	})
	if err != nil {
		t.Fatalf("open state database: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return &chatlogBackedStore{
		t:     t,
		store: chatlog.NewStore(database.Executor(), self),
		self:  self,
	}
}

// StoreMessage writes through to the real chatlog and reports whether the row
// was new.
func (s *chatlogBackedStore) StoreMessage(envelope protocol.Envelope, _ bool) StoreResult {
	entry := chatlog.Entry{
		ID:        string(envelope.ID),
		Sender:    string(envelope.Sender),
		Recipient: string(envelope.Recipient),
		Body:      string(envelope.Payload),
		Flag:      string(envelope.Flag),
		CreatedAt: envelope.CreatedAt.UTC().Format(time.RFC3339Nano),
	}
	inserted, err := s.store.AppendReportNew(context.Background(), envelope.Topic, s.self, entry)
	if err != nil {
		s.t.Errorf("append to the chatlog: %v", err)
		return StoreDeferred
	}
	s.mu.Lock()
	s.stored = append(s.stored, envelope)
	s.mu.Unlock()
	if !inserted {
		return StoreDuplicate
	}
	return StoreInserted
}

// UpdateDeliveryStatus is not the subject here; receipts have their own tests.
func (s *chatlogBackedStore) UpdateDeliveryStatus(protocol.DeliveryReceipt) bool { return true }

// rows reads back what the database actually holds for one conversation.
func (s *chatlogBackedStore) rows(topic string, peer domain.PeerIdentity) []chatlog.Entry {
	s.t.Helper()
	entries, err := s.store.Read(context.Background(), topic, peer)
	if err != nil {
		s.t.Fatalf("read the conversation back: %v", err)
	}
	return entries
}

// crossPathFixture is one receiving node with a real chatlog sink and an
// observable event bus.
type crossPathFixture struct {
	svc    *Service
	store  *chatlogBackedStore
	sender *identity.Identity
	bus    *ebus.Bus

	mu       sync.Mutex
	observed []string
	sentinel chan struct{}
}

func newCrossPathFixture(t *testing.T) *crossPathFixture {
	t.Helper()
	svc := newTestService(t, config.NodeTypeFull)
	sender := registerSenderKey(t, svc)

	store := newChatlogBackedStore(t, domain.PeerIdentityFromWire(svc.Address()))
	svc.messageStore = store

	// newTestService passes a nil bus; a real one makes the UI notification
	// observable, and "one notification" is half of what this file pins.
	bus := ebus.New()
	t.Cleanup(bus.Shutdown)
	svc.eventBus = bus

	f := &crossPathFixture{
		svc:      svc,
		store:    store,
		sender:   sender,
		bus:      bus,
		sentinel: make(chan struct{}),
	}
	bus.Subscribe(ebus.TopicMessageNew, func(event protocol.LocalChangeEvent) {
		if event.MessageID == crossPathSentinelID {
			close(f.sentinel)
			return
		}
		f.mu.Lock()
		f.observed = append(f.observed, event.MessageID)
		f.mu.Unlock()
	})
	return f
}

// crossPathSentinelID marks the barrier event. It is not a message id any
// delivery can produce, so a real notification can never be mistaken for the
// barrier.
const crossPathSentinelID = "cross-path-sentinel"

// announcements returns the message ids the UI was told about, after waiting
// for every publication made so far to be HANDLED.
//
// The wait is a barrier, not a sleep. ebus gives each subscriber its own
// goroutine and an inbox it drains IN ORDER, so a sentinel published after the
// deliveries is handled after them: seeing it proves the handler has already
// run for everything before it.
//
// The sleep this replaces could not prove anything in either direction. A
// second, erroneous notification arriving late would be counted after the
// assertion had passed, and a slow handler would fail a correct build — the
// test would be reporting scheduler luck instead of behaviour.
func (f *crossPathFixture) announcements(t *testing.T) []string {
	t.Helper()
	f.svc.WaitBackground()
	f.bus.Publish(ebus.TopicMessageNew, protocol.LocalChangeEvent{
		Type:      protocol.LocalChangeNewMessage,
		MessageID: crossPathSentinelID,
	})
	select {
	case <-f.sentinel:
	case <-time.After(10 * time.Second):
		t.Fatal("the event handler never reached the barrier: no conclusion about the number of notifications is possible")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.observed...)
}

// relayArrival delivers the message the way a neighbour relays it.
func (f *crossPathFixture) relayArrival(t *testing.T, id, body, previousHop string) string {
	t.Helper()
	frame := protocol.Frame{
		Type:        "relay_message",
		ID:          id,
		Address:     f.sender.Address,
		Recipient:   f.svc.Address(),
		Topic:       "dm",
		Body:        body,
		Flag:        string(protocol.MessageFlagImmutable),
		CreatedAt:   time.Now().UTC().Format(time.RFC3339),
		HopCount:    2,
		MaxHops:     10,
		PreviousHop: previousHop,
	}
	return f.svc.handleRelayMessage(domain.PeerAddress(previousHop), nil, frame)
}

// pushArrival delivers the same message the way a direct session pushes it.
//
// It goes through storeIncomingMessage — the ONE door every mesh path uses —
// which is the point: the two arrivals meet where the node decides whether it
// has seen this message, not in the test.
func (f *crossPathFixture) pushArrival(t *testing.T, id, body string) (bool, string) {
	t.Helper()
	stored, _, errCode := f.svc.storeIncomingMessage(incomingMessage{
		ID:        protocol.MessageID(id),
		Topic:     "dm",
		Sender:    f.sender.Address,
		Recipient: f.svc.Address(),
		Flag:      protocol.MessageFlagImmutable,
		CreatedAt: time.Now().UTC(),
		Body:      body,
	}, true)
	return stored, errCode
}

// TestOneMessageDeliveredByTwoPathsIsStoredOnce pins the property in both
// delivery orders.
//
// Both orders, because the two paths are not symmetric: the relay arm runs the
// transit bookkeeping (relayStateStore, hop accounting) before reaching the
// common door, and the push arm does not. A test fixing one order would leave
// the other free to grow a second row.
func TestOneMessageDeliveredByTwoPathsIsStoredOnce(t *testing.T) {
	t.Parallel()

	orders := map[string]func(t *testing.T, f *crossPathFixture, id, body string){
		"relay first, then push": func(t *testing.T, f *crossPathFixture, id, body string) {
			if status := f.relayArrival(t, id, body, "10.0.0.1:9000"); status != "delivered" {
				t.Fatalf("relay arrival = %q, want delivered", status)
			}
			if stored, errCode := f.pushArrival(t, id, body); errCode != "" {
				t.Fatalf("push arrival after relay: stored=%v errCode=%q", stored, errCode)
			}
		},
		"push first, then relay": func(t *testing.T, f *crossPathFixture, id, body string) {
			if stored, errCode := f.pushArrival(t, id, body); !stored || errCode != "" {
				t.Fatalf("push arrival = stored:%v errCode:%q, want stored", stored, errCode)
			}
			// The relay arm ACKS a message it already has, and that is
			// deliberate: silence would leave the previous hop retrying a
			// message that has in fact arrived. The duplicate is refused at
			// the store, not at the ack — which is exactly why the row count
			// below, and not the status, is what this test is about.
			if status := f.relayArrival(t, id, body, "10.0.0.2:9000"); status == "" {
				t.Fatal("relay arrival after push returned no status: the previous hop would retry a delivered message")
			}
		},
	}

	for name, deliver := range orders {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newCrossPathFixture(t)
			body := sealDMBody(t, f.sender, f.svc.Address(), identity.BoxPublicKeyBase64(f.svc.identity.BoxPublicKey))

			deliver(t, f, "two-paths-1", body)
			announced := f.announcements(t)

			rows := f.store.rows("dm", domain.PeerIdentityFromWire(f.sender.Address))
			if len(rows) != 1 {
				t.Fatalf("the conversation holds %d rows, want 1 — one message delivered twice is one message", len(rows))
			}
			if rows[0].ID != "two-paths-1" {
				t.Fatalf("stored row id = %q, want the identifier both paths carried", rows[0].ID)
			}
			if len(announced) != 1 || announced[0] != "two-paths-1" {
				t.Fatalf("the UI was told %v, want exactly [two-paths-1] — the second arrival must be silent", announced)
			}
		})
	}
}

// TestConcurrentArrivalsOfOneMessageStoreOnce pins the same property when the
// two paths land at the same time.
//
// Sequential delivery is the easy half: the first arrival has already written
// its dedup mark before the second begins. Concurrency is where a check-then-act
// gap would show, and it is also the shape the network actually produces —
// dispatchEnvelopeRetry pushes, gossips and relays one message in a single tick.
func TestConcurrentArrivalsOfOneMessageStoreOnce(t *testing.T) {
	t.Parallel()

	f := newCrossPathFixture(t)
	body := sealDMBody(t, f.sender, f.svc.Address(), identity.BoxPublicKeyBase64(f.svc.identity.BoxPublicKey))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		f.relayArrival(t, "concurrent-1", body, "10.0.0.1:9000")
	}()
	go func() {
		defer wg.Done()
		f.pushArrival(t, "concurrent-1", body)
	}()
	wg.Wait()
	announced := f.announcements(t)

	rows := f.store.rows("dm", domain.PeerIdentityFromWire(f.sender.Address))
	if len(rows) != 1 {
		t.Fatalf("the conversation holds %d rows, want 1 under concurrent arrival", len(rows))
	}
	if len(announced) != 1 || announced[0] != "concurrent-1" {
		t.Fatalf("the UI was told %v, want exactly [concurrent-1] under concurrent arrival", announced)
	}
}

// TestTwoDifferentMessagesAreNotMergedByDeduplication is the other half of the
// claim, and it is not decoration.
//
// A dedup that folds everything together satisfies "one row per message"
// perfectly and loses conversations. The two assertions have to travel
// together: nothing merged, nothing duplicated.
func TestTwoDifferentMessagesAreNotMergedByDeduplication(t *testing.T) {
	t.Parallel()

	f := newCrossPathFixture(t)
	first := sealDMBody(t, f.sender, f.svc.Address(), identity.BoxPublicKeyBase64(f.svc.identity.BoxPublicKey))
	second := sealDMBody(t, f.sender, f.svc.Address(), identity.BoxPublicKeyBase64(f.svc.identity.BoxPublicKey))

	if status := f.relayArrival(t, "distinct-1", first, "10.0.0.1:9000"); status != "delivered" {
		t.Fatalf("first message = %q, want delivered", status)
	}
	if stored, errCode := f.pushArrival(t, "distinct-2", second); !stored || errCode != "" {
		t.Fatalf("second message = stored:%v errCode:%q, want stored", stored, errCode)
	}
	announced := f.announcements(t)

	rows := f.store.rows("dm", domain.PeerIdentityFromWire(f.sender.Address))
	if len(rows) != 2 {
		t.Fatalf("the conversation holds %d rows, want 2 — two different messages must stay two", len(rows))
	}
	seen := map[string]bool{}
	for _, row := range rows {
		seen[row.ID] = true
	}
	if !seen["distinct-1"] || !seen["distinct-2"] {
		t.Fatalf("stored ids = %v, want both distinct-1 and distinct-2", seen)
	}
	if len(announced) != 2 {
		t.Fatalf("the UI was told %v, want both messages announced", announced)
	}
	if announced[0] == announced[1] {
		t.Fatalf("the UI was told about %q twice instead of two distinct messages", announced[0])
	}
}

// TestSameIdentifierWithDifferentBodyDoesNotOverwriteTheAcceptedMessage pins
// what happens in the case the identifier alone cannot settle.
//
// Two arrivals share an id and disagree about the body. Whatever else is
// decided later — whether such a conflict deserves a counter, and which fields
// are even required to match — ONE thing must hold now: the message already
// accepted is not rewritten by the later one. Silent overwrite would let
// anybody who learns an identifier replace what a conversation says.
//
// The test deliberately does NOT assert that the conflict is reported: no
// counter exists yet, and the decision about one waits on defining which
// content must match (step 20 §3).
func TestSameIdentifierWithDifferentBodyDoesNotOverwriteTheAcceptedMessage(t *testing.T) {
	t.Parallel()

	f := newCrossPathFixture(t)
	accepted := sealDMBody(t, f.sender, f.svc.Address(), identity.BoxPublicKeyBase64(f.svc.identity.BoxPublicKey))
	conflicting := sealDMBody(t, f.sender, f.svc.Address(), identity.BoxPublicKeyBase64(f.svc.identity.BoxPublicKey))
	if accepted == conflicting {
		t.Fatal("the fixture produced identical ciphertexts; the conflict would be invisible")
	}

	if stored, errCode := f.pushArrival(t, "conflict-1", accepted); !stored || errCode != "" {
		t.Fatalf("the first arrival must be accepted, got stored=%v errCode=%q", stored, errCode)
	}
	// Same identifier, different content, arriving by the other path.
	f.relayArrival(t, "conflict-1", conflicting, "10.0.0.1:9000")
	announced := f.announcements(t)

	rows := f.store.rows("dm", domain.PeerIdentityFromWire(f.sender.Address))
	if len(rows) != 1 {
		t.Fatalf("the conversation holds %d rows, want 1", len(rows))
	}
	if rows[0].Body != accepted {
		t.Fatal("the later arrival overwrote the accepted message: an identifier is not permission to rewrite what was said")
	}
	if len(announced) != 1 || announced[0] != "conflict-1" {
		t.Fatalf("the UI was told %v, want exactly [conflict-1]", announced)
	}
}

// TestSendSideRebuildsPreserveTheMessageIdentifier covers what the arrival
// tests above CANNOT.
//
// Those tests hand both arrivals the same identifier, so they establish that
// two inputs carrying one id become one message — and nothing about whether the
// send side still produces one id. A regression in which the push frame, the
// gossip frame or the retry restamp minted a fresh identifier would leave every
// one of them green, because the divergence would happen before the point they
// start.
//
// So the transformations are exercised directly, on one envelope, and each
// result is checked against the identifier it started with. These are the ones
// callable as functions; the rest of the twelve rebuild points named in
// docs/refactoring/dht/20-cross-plane-dedup.md §3.1 are frame literals inside
// send paths that need a network, and for those the audit is a READING of the
// code, not an execution of it. The step says so rather than letting "audited"
// be mistaken for "tested".
func TestSendSideRebuildsPreserveTheMessageIdentifier(t *testing.T) {
	t.Parallel()

	const id = protocol.MessageID("rebuild-keeps-1")
	envelope := protocol.Envelope{
		ID:         id,
		Topic:      "dm",
		Sender:     "sender-address",
		Recipient:  "recipient-address",
		Flag:       protocol.MessageFlagImmutable,
		Payload:    []byte("ciphertext"),
		CreatedAt:  time.Now().UTC().Add(-time.Minute),
		TTLSeconds: 3600,
		Hops:       4,
	}

	rebuilds := map[string]func(t *testing.T) protocol.MessageID{
		"push to a subscriber (messageFrame)": func(t *testing.T) protocol.MessageID {
			return protocol.MessageID(messageFrame(envelope).ID)
		},
		"gossip fan-out (gossipPushFrame)": func(t *testing.T) protocol.MessageID {
			return protocol.MessageID(gossipPushFrame(envelope).Item.ID)
		},
		"sender retry restamp (legacyTransitRestamp)": func(t *testing.T) protocol.MessageID {
			return legacyTransitRestamp(envelope, time.Now().UTC()).ID
		},
		"receive from the wire (incomingMessageFromFrame)": func(t *testing.T) protocol.MessageID {
			msg, err := incomingMessageFromFrame(protocol.Frame{
				ID:        string(id),
				Topic:     envelope.Topic,
				Address:   envelope.Sender,
				Recipient: envelope.Recipient,
				Flag:      string(envelope.Flag),
				CreatedAt: envelope.CreatedAt.Format(time.RFC3339),
				Body:      string(envelope.Payload),
			})
			if err != nil {
				t.Fatalf("rebuild the incoming message: %v", err)
			}
			return msg.ID
		},
	}

	for name, rebuild := range rebuilds {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := rebuild(t); got != id {
				t.Fatalf("the rebuild produced id %q, want %q — a rebuild that renames the message "+
					"makes one message two everywhere downstream", got, id)
			}
		})
	}
}
