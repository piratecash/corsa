package service

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/ebus"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/node"
)

// newPresenceMonitor wires a monitor over a real bus with one seeded contact,
// and reports domain notifications so a test can wait for the apply instead of
// sleeping.
func newPresenceMonitor(t *testing.T, contact domain.PeerIdentity) (*NodeStatusMonitor, *ebus.Bus, domain.PeerIdentity, chan NodeStatusDomain) {
	t.Helper()
	dir := t.TempDir()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	bus := ebus.New()
	t.Cleanup(bus.Shutdown)
	svc := node.NewService(config.Node{
		ListenAddress:  ":0",
		TrustStorePath: filepath.Join(dir, "trust.json"),
		PeersStatePath: filepath.Join(dir, "peers.json"),
	}, id, bus)
	t.Cleanup(func() { svc.WaitBackground() })

	client := &DesktopClient{
		id:        id,
		appCfg:    config.App{Version: "test"},
		localNode: svc,
		chatLog:   newTestChatlogStore(t, domain.PeerIdentityFromWire(id.Address)),
	}
	client.wireSubServices()

	notified := make(chan NodeStatusDomain, 8)
	monitor := NewNodeStatusMonitor(NodeStatusMonitorOpts{
		EventBus:         bus,
		Client:           client,
		OnPartialChanged: func(d NodeStatusDomain) { notified <- d },
	})
	monitor.Start()

	monitor.mu.Lock()
	if monitor.status.Contacts == nil {
		monitor.status.Contacts = make(map[string]Contact)
	}
	monitor.status.Contacts[contact.String()] = Contact{PubKey: "pk"}
	monitor.mu.Unlock()

	return monitor, bus, domain.PeerIdentityFromWire(id.Address), notified
}

func awaitPresenceNotification(t *testing.T, notified chan NodeStatusDomain) bool {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case d := <-notified:
			if d == NodeStatusDomainPresence {
				return true
			}
		case <-deadline:
			return false
		}
	}
}

// TestPresenceObservedReachesRunningStatus is the reason the observation is
// published at all. The desktop probes the node ONCE at startup and lives on
// events afterwards, so a durable write the node makes while running reaches
// NodeStatus only through this event — without it the sidebar would keep the
// pre-arrival timestamp until the next launch.
func TestPresenceObservedReachesRunningStatus(t *testing.T) {
	peer := domaintest.ID("observed-peer")
	monitor, bus, self, notified := newPresenceMonitor(t, peer)

	observedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	ebus.PublishIdentityPresenceObserved(bus, ebus.IdentityPresenceChange{
		Source:     self,
		Identities: []domain.PeerIdentity{peer},
		ChangedAt:  observedAt,
	})
	if !awaitPresenceNotification(t, notified) {
		t.Fatal("presence observation never reached the monitor")
	}

	got := monitor.NodeStatus().Contacts[peer.String()].LastOnlineAt
	if !got.Valid() || !got.Time().Equal(observedAt) {
		t.Fatalf("LastOnlineAt = %v, want %v", got, observedAt)
	}
}

// TestPresenceObservedNeverRegresses pins the monotone rule on the receiving
// side too. Independent subscriber goroutines deliver in no guaranteed order,
// so "last received" is not "last observed": an older observation arriving
// late must not walk the timestamp backwards.
func TestPresenceObservedNeverRegresses(t *testing.T) {
	peer := domaintest.ID("out-of-order-observed")
	monitor, bus, self, notified := newPresenceMonitor(t, peer)

	newest := time.Now().UTC().Truncate(time.Second)
	ebus.PublishIdentityPresenceObserved(bus, ebus.IdentityPresenceChange{
		Source: self, Identities: []domain.PeerIdentity{peer}, ChangedAt: newest,
	})
	if !awaitPresenceNotification(t, notified) {
		t.Fatal("first observation never reached the monitor")
	}

	// An older observation from this node, on the route-loss topic that
	// shares the handler. It must not walk the timestamp back.
	ebus.PublishIdentityPresenceChanged(bus, ebus.IdentityPresenceChange{
		Source: self, Identities: []domain.PeerIdentity{peer}, ChangedAt: newest.Add(-time.Hour),
	})
	// A barrier on the SAME topic and inbox, for a DIFFERENT identity: its
	// effect proves the older one above was already processed, without
	// overwriting what that one may have wrongly written. A barrier for the
	// same peer would repair the regression it is meant to detect.
	barrier := domaintest.ID("barrier-contact")
	monitor.mu.Lock()
	monitor.status.Contacts[barrier.String()] = Contact{PubKey: "pk"}
	monitor.mu.Unlock()
	barrierAt := newest.Add(time.Minute)
	ebus.PublishIdentityPresenceChanged(bus, ebus.IdentityPresenceChange{
		Source: self, Identities: []domain.PeerIdentity{barrier}, ChangedAt: barrierAt,
	})
	if !awaitPresenceNotification(t, notified) {
		t.Fatal("the barrier observation never reached the monitor")
	}

	if got := monitor.NodeStatus().Contacts[peer.String()].LastOnlineAt; !got.Valid() || !got.Time().Equal(newest) {
		t.Fatalf("LastOnlineAt = %v, want it held at %v — an older observation walked it backwards", got, newest)
	}

	// A foreign observer's, newer, on the other topic: not ours to apply.
	ebus.PublishIdentityPresenceObserved(bus, ebus.IdentityPresenceChange{
		Source:     domaintest.ID("someone-else"),
		Identities: []domain.PeerIdentity{peer},
		ChangedAt:  newest.Add(time.Hour),
	})
	// And a newer one of our own, which must be applied — ordering behind
	// the foreign event on the same inbox.
	settled := newest.Add(2 * time.Minute)
	ebus.PublishIdentityPresenceObserved(bus, ebus.IdentityPresenceChange{
		Source: self, Identities: []domain.PeerIdentity{peer}, ChangedAt: settled,
	})
	if !awaitPresenceNotification(t, notified) {
		t.Fatal("the trailing observation never reached the monitor")
	}

	got := monitor.NodeStatus().Contacts[peer.String()].LastOnlineAt
	if !got.Valid() || !got.Time().Equal(settled) {
		t.Fatalf("LastOnlineAt = %v, want %v", got, settled)
	}
}

// TestPresenceObservedSurvivesContactAddedRace covers the ordering the two
// topics do not guarantee. TopicContactAdded and the presence topics run on
// independent subscriber goroutines, so a DM from a contact that is being
// added right now can be applied FIRST — against a snapshot that has no row
// for it yet. Dropping the observation there used to be permanent: the
// contact then arrived with an empty timestamp, and with the periodic full
// probe gone nothing would ever fill it in.
//
// The observation is applied SYNCHRONOUSLY rather than published: two
// Publish calls land on independent goroutines, so the test would not
// actually pin the order it is about, and the pre-fix code could pass it by
// winning the race.
func TestPresenceObservedSurvivesContactAddedRace(t *testing.T) {
	peer := domaintest.ID("added-after-the-dm")
	monitor, bus, self, _ := newPresenceMonitor(t, domaintest.ID("someone-else"))

	observedAt := time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Second)
	monitor.applyIdentityPresence(ebus.IdentityPresenceChange{
		Source:     self,
		Identities: []domain.PeerIdentity{peer},
		ChangedAt:  observedAt,
	}, presenceFromDirectMessage)

	monitor.mu.RLock()
	_, held := monitor.heldPresence[peer.String()]
	monitor.mu.RUnlock()
	if !held {
		t.Fatal("an observation about an identity with no contact row was dropped instead of held")
	}

	// The contact arrives second and must claim it.
	ebus.PublishContactAdded(bus, ebus.ContactAddedEvent{
		Address: peer,
		PubKey:  domain.PeerPublicKey("pk"),
	})

	got := awaitLastOnline(t, monitor, peer)
	if !got.Equal(observedAt) {
		t.Fatalf("LastOnlineAt = %v, want the held observation %v", got, observedAt)
	}

	monitor.mu.RLock()
	_, stillHeld := monitor.heldPresence[peer.String()]
	monitor.mu.RUnlock()
	if stillHeld {
		t.Fatal("the claimed observation was left in the hold")
	}
}

// TestPresenceObservedClaimedByStartupProbe covers the other way a contact
// first appears: the startup probe, which introduces them in bulk. Without
// this the startup race would simply have moved — an observation that landed
// before the first probe would wait for a contact-added event that already
// happened.
func TestPresenceObservedClaimedByStartupProbe(t *testing.T) {
	peer := domaintest.ID("introduced-by-probe")
	monitor, _, self, _ := newPresenceMonitor(t, domaintest.ID("someone-else"))

	observedAt := time.Now().UTC().Add(-3 * time.Minute).Truncate(time.Second)
	monitor.applyIdentityPresence(ebus.IdentityPresenceChange{
		Source:     self,
		Identities: []domain.PeerIdentity{peer},
		ChangedAt:  observedAt,
	}, presenceFromDirectMessage)

	monitor.SeedFromProbe(NodeStatus{
		Connected: true,
		Contacts:  map[string]Contact{peer.String(): {PubKey: "pk"}},
	})

	got := monitor.NodeStatus().Contacts[peer.String()].LastOnlineAt
	if !got.Valid() || !got.Time().Equal(observedAt) {
		t.Fatalf("LastOnlineAt after the probe = %v, want the held observation %v", got, observedAt)
	}
}

// TestPresenceHoldIsClearedOnReset pins the hold to the identity that made
// the observations. Kept across a reset, a held value would be claimed by
// whatever contact the NEXT session adds under the same address.
func TestPresenceHoldIsClearedOnReset(t *testing.T) {
	peer := domaintest.ID("previous-session-peer")
	monitor, bus, self, _ := newPresenceMonitor(t, domaintest.ID("someone-else"))

	monitor.applyIdentityPresence(ebus.IdentityPresenceChange{
		Source:     self,
		Identities: []domain.PeerIdentity{peer},
		ChangedAt:  time.Now().UTC().Add(-time.Minute),
	}, presenceFromDirectMessage)
	monitor.Reset()

	monitor.mu.RLock()
	remaining := len(monitor.heldPresence)
	monitor.mu.RUnlock()
	if remaining != 0 {
		t.Fatalf("held observations survived the reset: %d", remaining)
	}

	// A contact added in the new session must not inherit it.
	ebus.PublishContactAdded(bus, ebus.ContactAddedEvent{
		Address: peer,
		PubKey:  domain.PeerPublicKey("pk"),
	})
	deadline := time.After(2 * time.Second)
	for {
		if got := monitor.NodeStatus().Contacts[peer.String()]; got.PubKey != "" {
			if got.LastOnlineAt.Valid() {
				t.Fatalf("a new-session contact inherited the previous session's observation: %v", got.LastOnlineAt.Time())
			}
			return
		}
		select {
		case <-deadline:
			t.Fatal("the contact-added event never reached the monitor")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// TestPresenceHoldExpiresStaleEntries covers the cap. Entries wait for an
// event that claims them, and one that never comes must not pin its slot for
// the life of the process — otherwise a burst of identities whose contacts
// never arrive would keep the observation of a contact that IS being added.
func TestPresenceHoldExpiresStaleEntries(t *testing.T) {
	monitor, _, self, _ := newPresenceMonitor(t, domaintest.ID("someone-else"))

	old := time.Now().UTC().Add(-time.Hour)
	identities := make([]domain.PeerIdentity, 0, maxHeldPresenceObservations)
	for i := 0; i < maxHeldPresenceObservations; i++ {
		identities = append(identities, domaintest.ID(fmt.Sprintf("stale-%d", i)))
	}
	monitor.applyIdentityPresence(ebus.IdentityPresenceChange{
		Source: self, Identities: identities, ChangedAt: old,
	}, presenceFromRouting)

	monitor.mu.RLock()
	filled := len(monitor.heldPresence)
	monitor.mu.RUnlock()
	if filled != maxHeldPresenceObservations {
		t.Fatalf("hold size = %d, want the cap %d", filled, maxHeldPresenceObservations)
	}

	// A fresh observation arriving after the TTL must find room.
	fresh := domaintest.ID("the-contact-being-added")
	monitor.applyIdentityPresence(ebus.IdentityPresenceChange{
		Source: self, Identities: []domain.PeerIdentity{fresh}, ChangedAt: time.Now().UTC(),
	}, presenceFromDirectMessage)

	monitor.mu.RLock()
	_, kept := monitor.heldPresence[fresh.String()]
	remaining := len(monitor.heldPresence)
	monitor.mu.RUnlock()
	if !kept {
		t.Fatal("a full hold of stale entries refused the observation of a contact being added")
	}
	// The room came from the TTL sweep, not from evicting one entry to make
	// space: an eviction alone frees exactly one slot and leaves the cap full
	// of observations whose contact-added event is never coming.
	if remaining != 1 {
		t.Fatalf("hold size = %d, want only the fresh observation — the stale entries were evicted one by one instead of expiring", remaining)
	}
}

// TestPresenceHoldEvictsRouteChurnBeforeContacts covers the cap when nothing
// is stale enough to expire. The routing topic reports identities from the
// routing table — peers, most of which will never be contacts — so a churning
// mesh fills the hold with entries nobody will ever claim. Evicting purely by
// age hands those entries the slots and throws out the DM observation for the
// contact that IS being added, which is the only reason the hold exists.
func TestPresenceHoldEvictsRouteChurnBeforeContacts(t *testing.T) {
	monitor, _, self, _ := newPresenceMonitor(t, domaintest.ID("someone-else"))

	// The DM observation arrives first, so it is also the oldest.
	waiting := domaintest.ID("contact-being-added")
	waitingAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	monitor.applyIdentityPresence(ebus.IdentityPresenceChange{
		Source: self, Identities: []domain.PeerIdentity{waiting}, ChangedAt: waitingAt,
	}, presenceFromDirectMessage)

	// Route churn fills the rest of the map and then overflows it. Every
	// entry is recent, so the TTL sweep frees nothing.
	churn := make([]domain.PeerIdentity, 0, maxHeldPresenceObservations)
	for i := 0; i < maxHeldPresenceObservations; i++ {
		churn = append(churn, domaintest.ID(fmt.Sprintf("churn-%d", i)))
	}
	monitor.applyIdentityPresence(ebus.IdentityPresenceChange{
		Source: self, Identities: churn, ChangedAt: time.Now().UTC(),
	}, presenceFromRouting)

	monitor.mu.RLock()
	entry, kept := monitor.heldPresence[waiting.String()]
	size := len(monitor.heldPresence)
	monitor.mu.RUnlock()

	if size > maxHeldPresenceObservations {
		t.Fatalf("hold size = %d, want at most the cap %d", size, maxHeldPresenceObservations)
	}
	if !kept {
		t.Fatal("route churn evicted the observation of the contact being added")
	}
	if !entry.at.Equal(waitingAt) {
		t.Fatalf("held observation = %v, want %v", entry.at, waitingAt)
	}
}

// awaitLastOnline polls until the contact carries a timestamp. The
// contact-added handler reports a whole-status change rather than a
// presence-domain one, so there is no domain notification to wait on.
func awaitLastOnline(t *testing.T, monitor *NodeStatusMonitor, peer domain.PeerIdentity) time.Time {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		if got := monitor.NodeStatus().Contacts[peer.String()].LastOnlineAt; got.Valid() {
			return got.Time()
		}
		select {
		case <-deadline:
			t.Fatal("the observation never reached the contact")
		case <-time.After(10 * time.Millisecond):
		}
	}
}
