package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/transport"
)

// evictOrphanedPeerMetadata regression suite (memory-leak audit,
// 2026-06). The per-address metadata maps (peerTypes / peerIDs /
// peerVersions / peerBuilds / observedIPHistoryByPeer) gain entries
// on paths that never create a health row — version-incompatible
// hello rejections, handshake failures, CM dial failures. Inbound
// addresses carry an ephemeral source port, so every reconnect mints
// a fresh key and nothing ever deleted the entries: a long-lived
// node accumulated one metadata set per connection forever. The
// sweep deletes metadata whose address has no lifecycle anchor for a
// full orphanedHealthEvictWindow (two-phase mark via
// metaOrphanFirstSeen — protects the in-flight handshake window
// where metadata lands before the health row exists).

// newMetadataFixture builds the minimal Service the sweep touches.
func newMetadataFixture() *Service {
	return &Service{
		peerTypes:               make(map[domain.PeerAddress]domain.NodeType),
		peerIDs:                 make(map[domain.PeerAddress]domain.PeerIdentity),
		peerVersions:            make(map[domain.PeerAddress]string),
		peerBuilds:              make(map[domain.PeerAddress]int),
		observedIPHistoryByPeer: make(map[domain.PeerAddress][]domain.PeerIP),
		health:                  make(map[domain.PeerAddress]*peerHealth),
		inboundHealthRefs:       make(map[domain.PeerAddress]int),
		persistedMeta:           make(map[domain.PeerAddress]*peerEntry),
		sessions:                make(map[domain.PeerAddress]*peerSession),
	}
}

func seedMetadata(svc *Service, addr domain.PeerAddress) {
	svc.peerTypes[addr] = domain.NodeTypeUnknown
	svc.peerIDs[addr] = "aa00000000000000000000000000000000000001"
	svc.peerVersions[addr] = "1.0.47"
	svc.peerBuilds[addr] = 1860000
	svc.observedIPHistoryByPeer[addr] = []domain.PeerIP{"203.0.113.7"}
}

// sweepNow resets the once-a-minute sweep throttle and runs one
// pass — tests drive consecutive passes back-to-back.
func sweepNow(svc *Service) {
	svc.peerMu.Lock()
	svc.lastMetaOrphanSweep = time.Time{}
	svc.peerMu.Unlock()
	svc.evictOrphanedPeerMetadata()
}

func metadataPresent(svc *Service, addr domain.PeerAddress) bool {
	svc.peerMu.RLock()
	_, ids := svc.peerIDs[addr]
	_, vers := svc.peerVersions[addr]
	_, builds := svc.peerBuilds[addr]
	_, types := svc.peerTypes[addr]
	svc.peerMu.RUnlock()
	svc.ipStateMu.Lock()
	_, hist := svc.observedIPHistoryByPeer[addr]
	svc.ipStateMu.Unlock()
	return ids || vers || builds || types || hist
}

// TestEvictOrphanedPeerMetadata_RemovesAfterGrace pins the core
// lifecycle: pass 1 only MARKS an orphaned address (in-flight
// handshake protection), and deletion happens only after the address
// stays orphaned past orphanedHealthEvictWindow.
func TestEvictOrphanedPeerMetadata_RemovesAfterGrace(t *testing.T) {
	t.Parallel()

	svc := newMetadataFixture()
	addr := domain.PeerAddress("203.0.113.7:51234")
	seedMetadata(svc, addr)

	// Pass 1: orphan is marked, nothing deleted.
	sweepNow(svc)
	if !metadataPresent(svc, addr) {
		t.Fatal("first sweep pass must only mark the orphan, not delete it (handshake-window grace)")
	}
	svc.peerMu.RLock()
	_, marked := svc.metaOrphanFirstSeen[addr]
	svc.peerMu.RUnlock()
	if !marked {
		t.Fatal("first sweep pass must record the orphan mark")
	}

	// Pass 2 inside the grace window: still nothing deleted.
	sweepNow(svc)
	if !metadataPresent(svc, addr) {
		t.Fatal("sweep inside the grace window must not delete")
	}

	// Backdate the mark past the window: pass 3 deletes everything.
	svc.peerMu.Lock()
	svc.metaOrphanFirstSeen[addr] = time.Now().UTC().Add(-orphanedHealthEvictWindow - time.Minute)
	svc.peerMu.Unlock()
	sweepNow(svc)
	if metadataPresent(svc, addr) {
		t.Fatal("orphaned metadata must be deleted after a full grace window")
	}
	svc.peerMu.RLock()
	_, marked = svc.metaOrphanFirstSeen[addr]
	svc.peerMu.RUnlock()
	if marked {
		t.Fatal("orphan mark must be dropped together with the metadata")
	}
}

// TestEvictOrphanedPeerMetadata_AnchorsProtect pins every lifecycle
// anchor the sweep must respect: s.peers row, health row, active
// inbound refs, outbound session, persisted row. Anchored addresses
// keep their metadata even with a backdated orphan mark, and the
// stale mark is cleared.
func TestEvictOrphanedPeerMetadata_AnchorsProtect(t *testing.T) {
	t.Parallel()

	svc := newMetadataFixture()
	expired := time.Now().UTC().Add(-orphanedHealthEvictWindow - time.Minute)

	anchored := []struct {
		name string
		addr domain.PeerAddress
		hook func(domain.PeerAddress)
	}{
		{"peers_row", "198.51.100.1:64646", func(a domain.PeerAddress) {
			svc.peers = append(svc.peers, transport.Peer{Address: a})
		}},
		{"health_row", "198.51.100.2:64646", func(a domain.PeerAddress) {
			svc.health[a] = &peerHealth{Address: a}
		}},
		{"inbound_refs", "198.51.100.3:64646", func(a domain.PeerAddress) {
			svc.inboundHealthRefs[a] = 1
		}},
		{"session", "198.51.100.4:64646", func(a domain.PeerAddress) {
			svc.sessions[a] = &peerSession{address: a}
		}},
		{"persisted_meta", "198.51.100.5:64646", func(a domain.PeerAddress) {
			svc.persistedMeta[a] = &peerEntry{Address: a}
		}},
	}

	svc.peerMu.Lock()
	svc.metaOrphanFirstSeen = make(map[domain.PeerAddress]time.Time)
	for _, tc := range anchored {
		seedMetadata(svc, tc.addr)
		tc.hook(tc.addr)
		// Even a poisoned (expired) mark must not delete an anchored
		// address — the anchor check runs first.
		svc.metaOrphanFirstSeen[tc.addr] = expired
	}
	svc.peerMu.Unlock()

	sweepNow(svc)

	for _, tc := range anchored {
		if !metadataPresent(svc, tc.addr) {
			t.Fatalf("anchor %q failed to protect metadata for %s", tc.name, tc.addr)
		}
		svc.peerMu.RLock()
		_, marked := svc.metaOrphanFirstSeen[tc.addr]
		svc.peerMu.RUnlock()
		if marked {
			t.Fatalf("anchor %q must clear the stale orphan mark for %s", tc.name, tc.addr)
		}
	}
}

// TestEvictOrphanedPeerMetadata_StaleMarksPruned pins the tracker's
// own hygiene: a mark whose metadata was already removed by another
// sweep (health-driven eviction) must not linger in
// metaOrphanFirstSeen forever.
func TestEvictOrphanedPeerMetadata_StaleMarksPruned(t *testing.T) {
	t.Parallel()

	svc := newMetadataFixture()
	ghost := domain.PeerAddress("198.51.100.9:60000")
	svc.peerMu.Lock()
	svc.metaOrphanFirstSeen = map[domain.PeerAddress]time.Time{
		ghost: time.Now().UTC(),
	}
	svc.peerMu.Unlock()

	sweepNow(svc)

	svc.peerMu.RLock()
	_, marked := svc.metaOrphanFirstSeen[ghost]
	svc.peerMu.RUnlock()
	if marked {
		t.Fatal("mark without any metadata key must be pruned — the tracker must not leak its own entries")
	}
}
