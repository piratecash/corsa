package node

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
)

// datagram_replay_sweep_test.go covers the one schedule the anti-replay cache
// cannot run for itself.
//
// The cache frees records on its own RECEIVE path: the bounded pass lives inside
// Reserve. A plane that stops receiving — an idle node, or one whose routed plane
// is refusing everything — therefore holds every record it has until traffic
// returns, and five minutes of authenticated frames from every neighbour is what
// that costs. The cache's own comment has always said the full pass is run
// "periodically" by its caller; nothing in the node ran it.

// nodeReplayKey builds a distinct replay key for this file's fixtures.
func nodeReplayKey(seed byte) domain.ReplayKey {
	var key domain.ReplayKey
	for i := range key {
		key[i] = seed + byte(i)
	}
	return key
}

// TestTheBackgroundPassSweepsTheBaseReplayCache drives the REAL schedules of a
// started plane and waits for the expired record to go.
//
// It is deliberately not a check that some function calls another: the finding
// was that a pass every layer of documentation described as periodic was started
// nowhere, so the only assertion worth making is that a node which is merely
// RUNNING reclaims the record.
//
// The live record beside it is the positive control. Without it the test would
// pass against a pass that empties the cache, which would be an anti-replay hole
// rather than a fix: every record dropped early is a frame this node will accept
// a second time.
//
// The mutations this kills: dropping the sweep out of runDatagramMaintenancePass,
// moving it into a helper nothing calls, or wiring it to a schedule
// startDatagramSchedules does not start.
func TestTheBackgroundPassSweepsTheBaseReplayCache(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerService(t, true)
	requireDatagramPlane(t, svc)
	layer := svc.datagramLayer()

	cache := layer.replayCache

	// The layer's clock is the node's own wall clock, so the deadlines are
	// expressed against it rather than against an injected one: this test drives
	// real goroutines and cannot pretend about time.
	now := time.Now().UTC()
	settle := func(key domain.ReplayKey, until time.Time) {
		t.Helper()
		token, held := cache.Reserve(context.Background(), key, datagram.LocalIngress(), until).Reservation()
		if !held {
			t.Fatalf("the fixture could not reserve %s", until)
		}
		if applied := cache.Commit(context.Background(), token); !applied.IsApplied() {
			t.Fatalf("the fixture could not commit: %v", applied.Err())
		}
	}
	expired, live := nodeReplayKey(1), nodeReplayKey(50)
	settle(expired, now.Add(-time.Hour))
	settle(live, now.Add(time.Hour))
	if cache.Len() != 2 {
		t.Fatalf("the fixture holds %d records, want the expired one and the live one", cache.Len())
	}

	// The cadence is a field precisely so this needs no ten-second tick.
	layer.maintenancePace = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		svc.WaitBackground()
	})
	svc.startDatagramSchedules(ctx, layer)

	deadline := time.Now().Add(5 * time.Second)
	for cache.Len() > 1 {
		if time.Now().After(deadline) {
			t.Fatal("a running node never reclaimed a record that was expired before it started: " +
				"the anti-replay cache only sweeps on its own receive path, so nothing else ever " +
				"hands those records back")
		}
		time.Sleep(time.Millisecond)
	}

	if got := cache.Has(context.Background(), live).Outcome(); got != datagram.HasHit {
		t.Fatalf("the live record was swept as well (Has = %s): a record dropped before its "+
			"deadline is a frame this node will accept a second time", got)
	}
	if got := cache.Has(context.Background(), expired).Outcome(); got != datagram.HasMiss {
		t.Fatalf("the expired record is still readable: Has = %s", got)
	}
}

// TestTheSummaryPublishesTheReplayCacheCounters is the observability half of
// the same component.
//
// The base cache is the one part of the plane whose OVERFLOW is a fairness
// decision rather than a queue depth: it refuses and evicts records under
// pressure (§5) and its watchdog reclaims pipeline branches that never reached
// commit or release. Every one of those counters used to have no reader outside
// the cache's own tests — a rule that fires invisibly is a rule an operator
// cannot act on — so the diagnostic now carries them beside the occupancy they
// have to be read against.
//
// The mutation this kills: dropping the replay cache out of CollectDiagnostics
// or the `replay` block out of the summary, which would take the §5 refusals
// off every surface the node publishes.
func TestTheSummaryPublishesTheReplayCacheCounters(t *testing.T) {
	t.Parallel()

	svc := newDatagramLayerService(t, true)
	requireDatagramPlane(t, svc)
	cache := svc.datagramLayer().replayCache

	// A settled record, so both halves of the block are non-zero: a counter
	// that moved and an occupancy to read it against. A block that only ever
	// reported zeroes would pass against a hard-coded one.
	key := nodeReplayKey(90)
	token, held := cache.Reserve(
		context.Background(), key, datagram.LocalIngress(), time.Now().UTC().Add(time.Hour),
	).Reservation()
	if !held {
		t.Fatal("the fixture could not reserve a replay key")
	}
	if applied := cache.Commit(context.Background(), token); !applied.IsApplied() {
		t.Fatalf("the fixture could not commit: %v", applied.Err())
	}

	data, err := svc.FetchDatagramSummary()
	if err != nil {
		t.Fatalf("FetchDatagramSummary: %v", err)
	}
	var summary struct {
		Replay *struct {
			Counters struct {
				Reserved              uint64
				Committed             uint64
				RejectedNoisyPeer     uint64
				EvictedNoisyPeer      uint64
				AbandonedReservations uint64
			}
			Held int
		} `json:"replay"`
	}
	if err := json.Unmarshal(data, &summary); err != nil {
		t.Fatalf("decode the datagram summary: %v", err)
	}
	if summary.Replay == nil {
		t.Fatal("the summary carries no replay block: the §5 fairness refusals of the " +
			"anti-replay cache reach no operator surface at all")
	}
	if summary.Replay.Held != cache.Len() {
		t.Fatalf("replay.Held = %d, want the cache's own %d", summary.Replay.Held, cache.Len())
	}
	if summary.Replay.Counters.Reserved != 1 || summary.Replay.Counters.Committed != 1 {
		t.Fatalf("replay counters are not the cache's own: %+v", summary.Replay.Counters)
	}
}
