package service

import (
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

// TestReachableIDsReconcileOnlyOnSnapshotReason is the monitor half of the
// §1.2 regression (docs/refactoring/identity-discovery-lookup.md): the
// ReachableIDs rebuild runs on the snapshot-published reason and on nothing
// else. The mutation-time reasons fire while the cached routing snapshot is
// still the previous generation, so a rebuild on them wrote stale data —
// and could overwrite a fresh set with a stale one.
func TestReachableIDsReconcileOnlyOnSnapshotReason(t *testing.T) {
	t.Parallel()

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
	store := newTestChatlogStore(t, domain.PeerIdentityFromWire(id.Address))

	client := &DesktopClient{
		id:        id,
		appCfg:    config.App{Version: "test"},
		localNode: svc,
		chatLog:   store,
	}
	client.wireSubServices()

	notified := make(chan NodeStatusDomain, 8)
	monitor := NewNodeStatusMonitor(NodeStatusMonitorOpts{
		EventBus: bus,
		Client:   client,
		OnPartialChanged: func(domain NodeStatusDomain) {
			notified <- domain
		},
	})
	monitor.Start()

	if monitor.ReachableIDsSnapshot() != nil {
		t.Fatal("test setup: ReachableIDs must start unknown (nil)")
	}

	// A mutation-time reason must NOT trigger a rebuild: the map stays nil
	// and no reachability notification fires.
	bus.Publish(ebus.TopicRouteTableChanged, ebus.RouteTableChange{
		Reason: domain.RouteChangeAnnouncement, Accepted: 1,
	})
	// The snapshot reason is ordered behind it on the same subscriber inbox,
	// so observing its effect proves the earlier event was filtered.
	bus.Publish(ebus.TopicRouteTableChanged, ebus.RouteTableChange{
		Reason: domain.RouteChangeSnapshot,
	})

	deadline := time.After(5 * time.Second)
	for {
		select {
		case changed := <-notified:
			if changed != NodeStatusDomainReachableIDs {
				continue
			}
			if monitor.ReachableIDsSnapshot() == nil {
				t.Fatal("snapshot reason fired the notification but left ReachableIDs unknown")
			}
			// Exactly one reachability notification: the mutation-reason
			// event, processed first on the same inbox, produced none.
			select {
			case extra := <-notified:
				if extra == NodeStatusDomainReachableIDs {
					t.Fatal("the mutation-time reason also triggered a rebuild")
				}
			default:
			}
			goto reachabilityVerified
		case <-deadline:
			t.Fatal("the snapshot reason never triggered the reachability rebuild")
		}
	}

reachabilityVerified:
	// The dedicated presence event carries the exact timestamp persisted by
	// the node for online→offline transitions. The monitor must apply it to the
	// trusted contact in the same status update, without waiting for a probe.
	peer := domaintest.ID("monitor-last-online-peer")
	observedAt := time.Date(2026, time.August, 21, 7, 6, 16, 0, time.UTC)
	foreignObservedAt := observedAt.Add(time.Hour)
	monitor.mu.Lock()
	monitor.status.Contacts = map[string]Contact{peer.String(): {PubKey: "pk-peer"}}
	monitor.status.ReachableIDs = map[domain.PeerIdentity]bool{peer: true}
	monitor.mu.Unlock()

	// A shared bus may carry presence observations from another embedded node.
	// Its newer timestamp must not contaminate this monitor, and presence must
	// never rewrite ReachableIDs (the snapshot route event is the sole owner).
	ebus.PublishIdentityPresenceChanged(bus, ebus.IdentityPresenceChange{
		Source:     domaintest.ID("foreign-node"),
		Identities: []domain.PeerIdentity{peer},
		ChangedAt:  foreignObservedAt,
	})
	ebus.PublishIdentityPresenceChanged(bus, ebus.IdentityPresenceChange{
		Source:     client.Address(),
		Identities: []domain.PeerIdentity{peer},
		ChangedAt:  observedAt,
	})
	select {
	case changed := <-notified:
		if changed != NodeStatusDomainPresence {
			t.Fatalf("last-online transition notified domain %v, want presence", changed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("last-online transition did not notify the partial-only subscriber")
	}
	contact := monitor.Contacts()[peer.String()]
	if !contact.LastOnlineAt.Valid() || !contact.LastOnlineAt.Time().Equal(observedAt) {
		t.Fatalf("contact last online = %v, want %v", contact.LastOnlineAt, observedAt)
	}
	if reachable := monitor.ReachableIDsSnapshot(); !reachable[peer] {
		t.Fatalf("presence event rewrote ReachableIDs: %v", reachable)
	}
}

func presenceOnlineProof() domain.Presence {
	return domain.OnlinePresence(domain.PresenceSourceProof)
}

func presenceOfflineProbeTimeout() domain.Presence {
	return domain.OfflinePresence(domain.PresenceSourceProbeTimeout)
}

func domainPresenceFixture(wire string, presence domain.Presence) domain.PresenceSet {
	return domain.PresenceSet{domain.PeerIdentityFromWire(wire): presence}
}

func onlyPresenceKey(t *testing.T, set domain.PresenceSet) domain.PeerIdentity {
	t.Helper()
	for identity := range set {
		return identity
	}
	t.Fatal("presence fixture is empty")
	return domain.PeerIdentity{}
}

// TestTheLaterProjectionWins covers what happens when two readers of the same
// projection race, and it replaces a test that pinned the opposite.
//
// Presence is a WHOLE-SET answer: every pass of the node covers every contact.
// So the presence event and a full probe never hold complementary halves — they
// hold one picture read at two moments. The old rule said "events win per key,
// they are newer by construction", and that construction does not exist: both
// readers fetch the snapshot pointer independently, so a probe that read it
// LATER could be overwritten by an event that read it earlier. On a starting
// node that showed a stale status, and a dropped best-effort event left it
// stale until the one-minute heartbeat.
func TestTheLaterProjectionWins(t *testing.T) {
	key := "1111111111111111111111111111111111111111"
	early := domainPresenceFixture(key, presenceOfflineProbeTimeout())
	late := domainPresenceFixture(key, presenceOnlineProof())
	identity := onlyPresenceKey(t, late)

	m := &NodeStatusMonitor{}

	// The probe read generation 2; the event read generation 1 and applies
	// second. It must not undo the newer picture.
	if !m.applyPresenceLocked(late, 2) {
		t.Fatal("the first projection was refused")
	}
	if m.applyPresenceLocked(early, 1) {
		t.Fatal("an OLDER projection was applied over a newer one: whichever reader " +
			"happens to finish last would decide the status")
	}
	if got := m.status.Presence.Get(identity); got != late.Get(identity) {
		t.Fatalf("presence rolled back to %s", got)
	}

	// The later one still wins when it arrives second, which is the ordinary case.
	if !m.applyPresenceLocked(early, 3) {
		t.Fatal("a newer projection was refused")
	}
	if got := m.status.Presence.Get(identity); got != early.Get(identity) {
		t.Fatalf("the newer projection did not land: got %s", got)
	}
}

// TestTheSameGenerationIsNotReapplied: two readers that fetched the SAME
// projection have nothing to tell each other. Applying it twice would announce
// a change that did not happen.
func TestTheSameGenerationIsNotReapplied(t *testing.T) {
	set := domainPresenceFixture("4444444444444444444444444444444444444444", presenceOnlineProof())
	m := &NodeStatusMonitor{}
	if !m.applyPresenceLocked(set, 7) {
		t.Fatal("the first projection was refused")
	}
	if m.applyPresenceLocked(set, 7) {
		t.Fatal("the same generation was applied twice")
	}
}

// TestTheGenerationAlwaysMatchesTheSet: NodeStatus hands both out together, and
// a reader comparing them against another node's answer needs them to describe
// each other. A private counter beside the published one is two places to
// forget.
func TestTheGenerationAlwaysMatchesTheSet(t *testing.T) {
	set := domainPresenceFixture("5555555555555555555555555555555555555555", presenceOnlineProof())
	m := &NodeStatusMonitor{}
	if !m.applyPresenceLocked(set, 11) {
		t.Fatal("projection refused")
	}
	if m.status.PresenceGeneration != 11 {
		t.Fatalf("published generation is %d while the set came from 11: NodeStatus "+
			"would hand out a set with somebody else's number", m.status.PresenceGeneration)
	}
}

// TestAnUngeneratedProjectionIsNotAnEmptyOne: a node that has not projected yet
// answers with generation zero. Treating that as a real (empty) projection would
// replace "nothing is known" with "nobody is present" — and every contact would
// drop to the routing fallback for as long as it stuck.
func TestAnUngeneratedProjectionIsNotAnEmptyOne(t *testing.T) {
	key := "2222222222222222222222222222222222222222"
	known := domainPresenceFixture(key, presenceOnlineProof())
	identity := onlyPresenceKey(t, known)

	m := &NodeStatusMonitor{}
	if !m.applyPresenceLocked(known, 1) {
		t.Fatal("a numbered projection was refused")
	}
	if m.applyPresenceLocked(nil, 0) {
		t.Fatal("an unnumbered answer was applied: 'the node has not projected yet' " +
			"was turned into 'nobody is present'")
	}
	if got := m.status.Presence.Get(identity); got != known.Get(identity) {
		t.Fatalf("a known projection was wiped by an unnumbered one: got %s", got)
	}
}

// TestProbePresenceStillSeedsAnAttachedMonitor keeps the property the removed
// per-key merge was protecting: attaching to an already-running node must get
// the probe's full projection, because a quiet node emits no events at all and
// the contact list would otherwise sit on the reachability fallback forever.
func TestProbePresenceStillSeedsAnAttachedMonitor(t *testing.T) {
	fromProbe := domainPresenceFixture("3333333333333333333333333333333333333333", presenceOnlineProof())
	m := &NodeStatusMonitor{}
	if !m.applyPresenceLocked(fromProbe, 1) {
		t.Fatal("a monitor that has seen no event refused the probe's projection: " +
			"attaching to a running node would show no presence at all")
	}
	if len(m.status.Presence) != 1 {
		t.Fatalf("probe presence: got %d entries, want 1", len(m.status.Presence))
	}
}
