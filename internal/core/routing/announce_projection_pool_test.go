package routing

import (
	"fmt"
	"sync"
	"testing"

	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// announce_projection_pool_test.go pins the safety of the pooled AnnounceTo
// projection buffer (announceEntryBufPool, released via
// Table.ReleaseAnnounceEntries): a buffer returned to the pool and reused by a
// later projection must not leak stale entries, and concurrent
// AnnounceTo+Release callers must not corrupt each other.

func seedTransitRoutes(t *testing.T, tbl *Table, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		mustUpdate(t, tbl, RouteEntry{
			Identity: domaintest.ID(fmt.Sprintf("dest-%d", i)),
			Origin:   domaintest.ID(fmt.Sprintf("orig-%d", i)),
			NextHop:  domaintest.ID(fmt.Sprintf("up-%d", i)),
			Hops:     2,
			SeqNo:    1,
			Source:   RouteSourceAnnouncement,
		})
	}
}

func TestAnnounceToPoolReuseNoStaleEntries(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")))
	seedTransitRoutes(t, tbl, 20)

	// First projection to a peer that is not any uplink → all 20 destinations.
	full := tbl.AnnounceTo(domaintest.ID("stranger"))
	if len(full) != 20 {
		t.Fatalf("expected 20 projected entries, got %d", len(full))
	}
	tbl.ReleaseAnnounceEntries(full) // hand the buffer back to the pool

	// Second projection reuses the pooled backing array: exclude via up-5, so
	// dest-5 (reachable only through up-5) must be absent and the count must be
	// 19. A buffer-reset bug would leak the stale dest-5 from the prior
	// 20-entry build.
	excl := tbl.AnnounceTo(domaintest.ID("up-5"))
	if len(excl) != 19 {
		t.Fatalf("expected 19 entries after excludeVia up-5, got %d", len(excl))
	}
	for _, e := range excl {
		if e.Identity == domaintest.ID("dest-5") {
			t.Fatal("dest-5 (reachable only via excluded up-5) leaked into the projection — pool reuse corruption")
		}
	}
	tbl.ReleaseAnnounceEntries(excl)
}

func TestAnnounceToPoolConcurrent(t *testing.T) {
	tbl := NewTable(WithLocalOrigin(domaintest.ID("self")))
	const dests = 16
	seedTransitRoutes(t, tbl, dests)

	const workers = 16
	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				// peer-0..3 are not uplinks, so every destination projects.
				peer := domaintest.ID(fmt.Sprintf("peer-%d", i%4))
				ents := tbl.AnnounceTo(peer)
				if len(ents) != dests {
					t.Errorf("projection len=%d, want %d", len(ents), dests)
					tbl.ReleaseAnnounceEntries(ents)
					return
				}
				tbl.ReleaseAnnounceEntries(ents)
			}
		}()
	}
	wg.Wait()
}
