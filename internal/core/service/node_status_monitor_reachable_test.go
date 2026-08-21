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
