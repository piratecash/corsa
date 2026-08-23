package node

import (
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// newPresenceTestService builds a node whose trust store already holds one
// contact, so recordLastOnlineAt has somewhere to write.
func newPresenceTestService(t *testing.T, contact domain.PeerIdentity, clock func() time.Time) *Service {
	t.Helper()
	selfID, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	cfg := config.Node{
		TrustStorePath:    filepath.Join(t.TempDir(), "trust.json"),
		Type:              config.NodeTypeFull,
		AllowPrivatePeers: true,
	}
	svc := NewService(cfg, selfID, nil)
	t.Cleanup(svc.WaitBackground)
	svc.presenceClock = clock
	if stored, err := svc.trust.remember(trustedContact{Address: contact.String(), PubKey: "pk-peer"}); err != nil || !stored {
		t.Fatalf("remember contact: stored=%v err=%v", stored, err)
	}
	return svc
}

func lastOnlineOf(t *testing.T, svc *Service, contact domain.PeerIdentity) time.Time {
	t.Helper()
	svc.WaitBackground()
	return svc.trust.trustedContacts()[contact.String()].LastOnlineAt
}

// TestDirectDMArrivalRecordsPresence covers the live half of "last online":
// a message the peer handed us over their own session proves their node was
// up, and that observation is ours — stamped with our clock, not with the
// CreatedAt they chose.
func TestDirectDMArrivalRecordsPresence(t *testing.T) {
	peer := domaintest.ID("direct-sender")
	observed := time.Date(2026, time.August, 21, 10, 0, 0, 0, time.UTC)
	svc := newPresenceTestService(t, peer, func() time.Time { return observed })

	svc.recordDirectArrivalPresence(incomingMessage{
		ID:        "m-direct",
		Topic:     "dm",
		Sender:    peer.String(),
		Recipient: svc.identity.Address,
		// The sender's own clock claims tomorrow; the durable field must
		// not inherit it.
		CreatedAt:   observed.Add(24 * time.Hour),
		ViaIdentity: peer,
	})

	if got := lastOnlineOf(t, svc, peer); !got.Equal(observed) {
		t.Fatalf("last_online_at = %v, want our observation %v", got, observed)
	}
}

// TestRelayedDMArrivalDoesNotRecordPresence is the other half: an envelope
// that reached us through a relay says only that the relay is up. The message
// may have waited in transit for days, so stamping the sender online now would
// invent an observation this node never made.
func TestRelayedDMArrivalDoesNotRecordPresence(t *testing.T) {
	peer := domaintest.ID("relayed-sender")
	relay := domaintest.ID("some-relay")
	observed := time.Date(2026, time.August, 21, 10, 0, 0, 0, time.UTC)
	svc := newPresenceTestService(t, peer, func() time.Time { return observed })

	svc.recordDirectArrivalPresence(incomingMessage{
		ID:          "m-relayed",
		Topic:       "dm",
		Sender:      peer.String(),
		Recipient:   svc.identity.Address,
		CreatedAt:   observed,
		ViaIdentity: relay,
	})

	if got := lastOnlineOf(t, svc, peer); !got.IsZero() {
		t.Fatalf("relayed message recorded presence: %v", got)
	}
}

// TestTransitDMDoesNotRecordPresence guards the relay case from the other
// side: traffic we merely carry for two other identities is not evidence
// about either of them, and must not touch our contacts at all.
func TestTransitDMDoesNotRecordPresence(t *testing.T) {
	peer := domaintest.ID("transit-sender")
	stranger := domaintest.ID("transit-recipient")
	observed := time.Date(2026, time.August, 21, 10, 0, 0, 0, time.UTC)
	svc := newPresenceTestService(t, peer, func() time.Time { return observed })

	svc.recordDirectArrivalPresence(incomingMessage{
		ID:          "m-transit",
		Topic:       "dm",
		Sender:      peer.String(),
		Recipient:   stranger.String(),
		CreatedAt:   observed,
		ViaIdentity: peer,
	})

	if got := lastOnlineOf(t, svc, peer); !got.IsZero() {
		t.Fatalf("transit traffic recorded presence: %v", got)
	}
}

// TestStoreIncomingMessageRecordsDirectArrivalPresence drives the real
// ingest path with a genuinely signed envelope. The unit tests above pin the
// rules; this one pins that storeIncomingMessage actually applies them —
// without it, deleting the call site would leave every rule passing and the
// feature dead.
func TestStoreIncomingMessageRecordsDirectArrivalPresence(t *testing.T) {
	svc := newTestService(t, config.NodeTypeFull)
	sender := registerSenderKey(t, svc)
	senderID := domain.PeerIdentityFromWire(sender.Address)
	if stored, err := svc.trust.remember(trustedContact{
		Address: sender.Address,
		PubKey:  identity.PublicKeyBase64(sender.PublicKey),
	}); err != nil || !stored {
		t.Fatalf("remember sender: stored=%v err=%v", stored, err)
	}

	observed := time.Date(2026, time.August, 21, 12, 30, 0, 0, time.UTC)
	svc.presenceClock = func() time.Time { return observed }

	body := sealDMBody(t, sender, svc.Address(), identity.BoxPublicKeyBase64(svc.identity.BoxPublicKey))
	stored, _, errCode := svc.storeIncomingMessage(incomingMessage{
		ID:          "presence-ingest-1",
		Topic:       "dm",
		Sender:      sender.Address,
		Recipient:   svc.Address(),
		CreatedAt:   time.Now().UTC(),
		Body:        body,
		ViaIdentity: senderID,
	}, true)
	if !stored || errCode != "" {
		t.Fatalf("message must be stored: stored=%v errCode=%q", stored, errCode)
	}

	if got := lastOnlineOf(t, svc, senderID); !got.Equal(observed) {
		t.Fatalf("ingest path did not record presence: got %v, want %v", got, observed)
	}
}

// TestDirectDMArrivalPublishesPresenceObservation covers the delivery half of
// the observation. The desktop probes the node once at startup and lives on
// events afterwards, so a durable write made while it is running reaches the
// sidebar only through the bus.
func TestDirectDMArrivalPublishesPresenceObservation(t *testing.T) {
	peer := domaintest.ID("published-sender")
	observed := time.Date(2026, time.August, 21, 10, 0, 0, 0, time.UTC)

	bus := ebus.New()
	t.Cleanup(bus.Shutdown)
	received := make(chan ebus.IdentityPresenceChange, 4)
	bus.Subscribe(ebus.TopicIdentityPresenceObserved, func(change ebus.IdentityPresenceChange) {
		received <- change
	})

	svc := newPresenceTestService(t, peer, func() time.Time { return observed })
	svc.eventBus = bus

	svc.recordDirectArrivalPresence(incomingMessage{
		ID:          "m-published",
		Topic:       "dm",
		Sender:      peer.String(),
		Recipient:   svc.identity.Address,
		CreatedAt:   observed,
		ViaIdentity: peer,
	})

	select {
	case change := <-received:
		if change.Source != domain.PeerIdentityFromWire(svc.identity.Address) {
			t.Fatalf("observation source = %s, want the observing node", change.Source)
		}
		if len(change.Identities) != 1 || change.Identities[0] != peer {
			t.Fatalf("observation identities = %v, want [%s]", change.Identities, peer)
		}
		if !change.ChangedAt.Equal(observed) {
			t.Fatalf("observation time = %v, want %v", change.ChangedAt, observed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a direct DM arrival published no presence observation")
	}
}

// TestArrivalPresenceIgnoresNonContacts pins the gate. The trust store
// creates no contacts on this path, so an observation about a stranger has
// nowhere to be stored — and announcing it would hand the desktop monitor an
// identity it must hold aside for a contact-added event that is never coming,
// out of a 512-slot budget fed by remote parties.
func TestArrivalPresenceIgnoresNonContacts(t *testing.T) {
	contact := domaintest.ID("a-real-contact")
	stranger := domaintest.ID("never-trusted")
	observed := time.Date(2026, time.August, 21, 10, 0, 0, 0, time.UTC)
	svc := newPresenceTestService(t, contact, func() time.Time { return observed })

	bus := ebus.New()
	t.Cleanup(bus.Shutdown)
	var announced atomic.Int32
	bus.Subscribe(ebus.TopicIdentityPresenceObserved, func(ebus.IdentityPresenceChange) {
		announced.Add(1)
	}, ebus.WithSync())
	svc.eventBus = bus

	svc.recordDirectArrivalPresence(incomingMessage{
		ID: "m-stranger", Topic: "dm",
		Sender: stranger.String(), Recipient: svc.identity.Address,
		CreatedAt: observed, ViaIdentity: stranger,
	})
	svc.WaitBackground()

	if n := announced.Load(); n != 0 {
		t.Fatalf("announcements = %d, want none: the sender is not a contact", n)
	}
	if _, stored := svc.trust.trustedContacts()[stranger.String()]; stored {
		t.Fatal("a DM arrival created a contact")
	}
}

// TestArrivalPresenceWriteIsThrottled pins the cost of the DM-arrival stamp.
// Persisting means marshalling every contact and rewriting the file, and this
// runs on every inbound DM — including the retries and re-gossips that arrive
// before the dedup gate. The durable field only has to survive a restart, so
// it is written at most once per contact per interval; the running UI still
// hears about every arrival through the event.
func TestArrivalPresenceWriteIsThrottled(t *testing.T) {
	peer := domaintest.ID("chatty-contact")
	base := time.Date(2026, time.August, 21, 10, 0, 0, 0, time.UTC)
	clock := base
	svc := newPresenceTestService(t, peer, func() time.Time { return clock })

	bus := ebus.New()
	t.Cleanup(bus.Shutdown)
	var announced atomic.Int32
	// Synchronous delivery: the assertion counts announcements, and the bus
	// would otherwise still be carrying the last one when the test looks.
	bus.Subscribe(ebus.TopicIdentityPresenceObserved, func(ebus.IdentityPresenceChange) {
		announced.Add(1)
	}, ebus.WithSync())
	svc.eventBus = bus

	arrive := func(id string) {
		svc.recordDirectArrivalPresence(incomingMessage{
			ID: protocol.MessageID(id), Topic: "dm",
			Sender: peer.String(), Recipient: svc.identity.Address,
			CreatedAt: clock, ViaIdentity: peer,
		})
		svc.WaitBackground()
	}

	writes := func() uint64 {
		svc.trust.mu.RLock()
		defer svc.trust.mu.RUnlock()
		// One snapshot generation is one rewrite of the trust file.
		return svc.trust.snapshotGen
	}

	arrive("m-1")
	if got := lastOnlineOf(t, svc, peer); !got.Equal(base) {
		t.Fatalf("first arrival did not persist: %v", got)
	}

	// A burst inside the interval: the stamp stays put, the UI still hears.
	clock = base.Add(5 * time.Second)
	arrive("m-2")
	clock = base.Add(10 * time.Second)
	arrive("m-3")
	if got := lastOnlineOf(t, svc, peer); !got.Equal(base) {
		t.Fatalf("a burst rewrote the trust store: stamp = %v, want %v", got, base)
	}

	// Past the interval it moves again.
	clock = base.Add(arrivalPresencePersistInterval + time.Second)
	arrive("m-4")
	if got := lastOnlineOf(t, svc, peer); !got.Equal(clock) {
		t.Fatalf("stamp after the interval = %v, want %v", got, clock)
	}

	if n := announced.Load(); n != 4 {
		t.Fatalf("announcements = %d, want one per arrival (4) — the throttle is about the disk, not the UI", n)
	}

	// The interval is only respected if the comparison and the update share a
	// lock. Deliver a burst without letting any of the writes finish first:
	// every message reads the same stored stamp, so a check made outside the
	// store buys a full rewrite for each one.
	clock = clock.Add(arrivalPresencePersistInterval + time.Second)
	burstAt := clock
	before := writes()
	for i, id := range []string{"burst-1", "burst-2", "burst-3", "burst-4", "burst-5"} {
		// Real arrivals do not share a timestamp, and equal ones would be
		// absorbed by monotonicity rather than by the throttle.
		clock = burstAt.Add(time.Duration(i) * time.Second)
		svc.recordDirectArrivalPresence(incomingMessage{
			ID: protocol.MessageID(id), Topic: "dm",
			Sender: peer.String(), Recipient: svc.identity.Address,
			CreatedAt: clock, ViaIdentity: peer,
		})
	}
	svc.WaitBackground()
	if got := writes() - before; got != 1 {
		t.Fatalf("a burst of 5 arrivals rewrote the trust file %d times, want 1", got)
	}
	// Which arrival of the burst won the single write is a scheduling detail
	// — the stamp is coarse by design — but it has to be one of them.
	last := burstAt.Add(4 * time.Second)
	if got := lastOnlineOf(t, svc, peer); got.Before(burstAt) || got.After(last) {
		t.Fatalf("stamp after the burst = %v, want one of the burst's own moments [%v, %v]", got, burstAt, last)
	}
}
