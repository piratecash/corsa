package node

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/ebus"
)

// ---------------------------------------------------------------------------
// Storm dedup — publisher-side suppression of no-op events.
//
// Background: after an i/o timeout cascade (cm_session_setup_failed →
// ActiveSessionLost → markPeerDisconnected → recomputeVersionPolicyLocked),
// two ebus topics fire in quick succession. When the underlying payload
// did not actually change, every redundant Publish forces the Desktop layer
// to rebuild a full NodeStatus snapshot and invalidate every subscribed
// window. Under a burst this looks like a frozen UI.
//
// The tests below pin the contract at the publishers:
//
//  1. TopicVersionPolicyChanged fires when VersionPolicySnapshot content
//     changes OR when versionPolicyHeartbeatInterval has elapsed since
//     the last publish. The heartbeat is mandatory because ebus is
//     lossy (Publish drops events when a subscriber inbox is full), so
//     pure content-based dedup would strand subscribers whose initial
//     publish was dropped.
//
//  2. TopicAggregateStatusChanged fires when AggregateStatusSnapshot
//     content (excluding the ComputedAt heartbeat) changes OR when
//     aggregateStatusHeartbeatInterval has elapsed. The heartbeat also
//     serves the user-visible "last checked" timestamp the Desktop UI
//     derives from ComputedAt.
//
// Slot-state emissions are intentionally NOT deduplicated: they are
// distinct lifecycle events and publisher-side dedup combined with lossy
// delivery would permanently mask dropped transitions.
// ---------------------------------------------------------------------------

// newStormBus builds a synchronous ebus. Sync subscriptions guarantee the
// handler has run by the time Publish returns, so tests can read the
// counter without sleeping.
func newStormBus(t *testing.T) *ebus.Bus {
	t.Helper()

	bus := ebus.New()
	t.Cleanup(func() { bus.Shutdown() })

	return bus
}

// TestRecomputeVersionPolicyLocked_SuppressesNoOpPublish exercises the
// scenario triggered by a peer-disconnect storm: the version policy is
// recomputed on every markPeerDisconnected, yet the inputs (reporter set,
// peer build map) are unchanged across attempts. The recompute must then
// produce a byte-identical snapshot and must NOT publish — as long as
// the heartbeat interval has not yet elapsed.
func TestRecomputeVersionPolicyLocked_SuppressesNoOpPublish(t *testing.T) {
	t.Parallel()

	bus := newStormBus(t)
	var count atomic.Int32
	bus.Subscribe(ebus.TopicVersionPolicyChanged, func(_ domain.VersionPolicySnapshot) {
		count.Add(1)
	}, ebus.WithSync())

	svc := &Service{
		eventBus:     bus,
		peerIDs:      make(map[domain.PeerAddress]domain.PeerIdentity),
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
	}

	now := time.Now().UTC()

	// First recompute — empty input, first-publish bootstrap fires once
	// so subscribers can seed their initial view.
	svc.peerMu.Lock()
	svc.recomputeVersionPolicyLocked(now)
	svc.peerMu.Unlock()

	baseline := count.Load()

	// Simulate a disconnect storm: 10 sequential recomputes with the exact
	// same inputs within the heartbeat window. The snapshot is bit-identical
	// on every pass, so no additional publish should occur.
	for i := 0; i < 10; i++ {
		svc.peerMu.Lock()
		// Advance well below versionPolicyHeartbeatInterval so the
		// heartbeat path cannot accidentally mask a broken dedup.
		svc.recomputeVersionPolicyLocked(now.Add(time.Duration(i+1) * time.Second))
		svc.peerMu.Unlock()
	}

	if got := count.Load(); got != baseline {
		t.Fatalf("duplicate version-policy publishes during storm: got %d extra, want 0", got-baseline)
	}
}

// TestRecomputeVersionPolicyLocked_HeartbeatRepublishesUnchangedSnapshot
// guards the recovery path for lossy ebus delivery. If the initial publish
// is dropped because a subscriber inbox was full, dedup must not strand
// the subscriber — the heartbeat interval must trigger a retransmit even
// when the snapshot content is byte-identical to the last publish.
func TestRecomputeVersionPolicyLocked_HeartbeatRepublishesUnchangedSnapshot(t *testing.T) {
	t.Parallel()

	bus := newStormBus(t)
	var count atomic.Int32
	bus.Subscribe(ebus.TopicVersionPolicyChanged, func(_ domain.VersionPolicySnapshot) {
		count.Add(1)
	}, ebus.WithSync())

	svc := &Service{
		eventBus:     bus,
		peerIDs:      make(map[domain.PeerAddress]domain.PeerIdentity),
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
	}

	base := time.Now().UTC()

	svc.peerMu.Lock()
	svc.recomputeVersionPolicyLocked(base)
	svc.peerMu.Unlock()

	if got := count.Load(); got != 1 {
		t.Fatalf("expected 1 publish on first recompute, got %d", got)
	}

	// Recompute one heartbeat interval later with identical inputs — the
	// content has not changed but the heartbeat must fire so a dropped
	// initial delivery cannot leave subscribers permanently stale.
	svc.peerMu.Lock()
	svc.recomputeVersionPolicyLocked(base.Add(versionPolicyHeartbeatInterval))
	svc.peerMu.Unlock()

	if got := count.Load(); got != 2 {
		t.Fatalf("expected heartbeat republish on unchanged snapshot, got %d publishes total", got)
	}
}

// TestRecomputeVersionPolicyLocked_PublishesOnRealChange guards against an
// over-aggressive dedup hiding legitimate transitions. Crossing the
// incompatibleVersionReporterThreshold must propagate to subscribers.
func TestRecomputeVersionPolicyLocked_PublishesOnRealChange(t *testing.T) {
	t.Parallel()

	bus := newStormBus(t)
	var count atomic.Int32
	bus.Subscribe(ebus.TopicVersionPolicyChanged, func(_ domain.VersionPolicySnapshot) {
		count.Add(1)
	}, ebus.WithSync())

	svc := &Service{
		eventBus:     bus,
		peerIDs:      make(map[domain.PeerAddress]domain.PeerIdentity),
		peerBuilds:   make(map[domain.PeerAddress]int),
		peerVersions: make(map[domain.PeerAddress]string),
	}

	now := time.Now().UTC()

	// Prime the state — baseline empty snapshot.
	svc.peerMu.Lock()
	svc.recomputeVersionPolicyLocked(now)
	svc.peerMu.Unlock()

	baseline := count.Load()

	// Cross the threshold (3 distinct reporters) to flip UpdateAvailable.
	svc.peerMu.Lock()
	svc.recordIncompatibleObservationLocked("peer-aaa", 10, 10, now)
	svc.recordIncompatibleObservationLocked("peer-bbb", 10, 10, now)
	svc.recordIncompatibleObservationLocked("peer-ccc", 10, 10, now)
	svc.peerMu.Unlock()

	if got := count.Load(); got <= baseline {
		t.Fatalf("expected at least one publish when threshold is crossed, got %d (baseline %d)", got, baseline)
	}
}

// TestPublishAggregateStatusChangedLocked_SuppressesNoOpPublishOnSameContent
// pins the aggregate-status publisher contract. The helper compares ignoring
// ComputedAt — the wall-clock heartbeat is not part of the aggregate
// semantics — and the heartbeat interval guards against dropped deliveries
// leaving subscribers stale.
func TestPublishAggregateStatusChangedLocked_SuppressesNoOpPublishOnSameContent(t *testing.T) {
	t.Parallel()

	bus := newStormBus(t)
	var count atomic.Int32
	bus.Subscribe(ebus.TopicAggregateStatusChanged, func(_ domain.AggregateStatusSnapshot) {
		count.Add(1)
	}, ebus.WithSync())

	svc := &Service{
		eventBus:        bus,
		health:          make(map[domain.PeerAddress]*peerHealth),
		pending:         make(map[domain.PeerAddress][]pendingFrame),
		peerIDs:         make(map[domain.PeerAddress]domain.PeerIdentity),
		peerBuilds:      make(map[domain.PeerAddress]int),
		peerVersions:    make(map[domain.PeerAddress]string),
		aggregateStatus: domain.AggregateStatusSnapshot{Status: domain.NetworkStatusOffline},
	}

	// Seed some health so computeAggregateStatusLocked produces a stable,
	// non-zero snapshot. Two reconnecting peers — no usable, status stays
	// "reconnecting" regardless of wall-clock drift.
	svc.health["10.0.0.1:1000"] = &peerHealth{
		Address: "10.0.0.1:1000", Connected: false, State: peerStateReconnecting,
	}
	svc.health["10.0.0.2:1000"] = &peerHealth{
		Address: "10.0.0.2:1000", Connected: false, State: peerStateReconnecting,
	}
	svc.pending["10.0.0.1:1000"] = nil
	svc.pending["10.0.0.2:1000"] = nil

	// First publication via the guarded helper — content moves from offline
	// to reconnecting, so exactly one publish is expected.
	svc.peerMu.Lock()
	svc.publishAggregateStatusChangedLocked()
	svc.peerMu.Unlock()

	if got := count.Load(); got != 1 {
		t.Fatalf("expected 1 publish on initial content change, got %d", got)
	}

	// Storm: repeat the helper without mutating any input. Content is
	// identical and the real-wall-clock delta per iteration is microscopic,
	// so the heartbeat cannot fire either — no extra publishes must hit
	// subscribers.
	for i := 0; i < 10; i++ {
		svc.peerMu.Lock()
		svc.publishAggregateStatusChangedLocked()
		svc.peerMu.Unlock()
	}

	if got := count.Load(); got != 1 {
		t.Fatalf("duplicate aggregate-status publishes on identical content: got %d, want 1", got)
	}

	// Flip one peer to healthy+connected — content changes, exactly one
	// more publish must reach subscribers.
	svc.health["10.0.0.1:1000"].Connected = true
	svc.health["10.0.0.1:1000"].State = peerStateHealthy
	svc.health["10.0.0.1:1000"].LastUsefulReceiveAt = time.Now().UTC()

	svc.peerMu.Lock()
	svc.publishAggregateStatusChangedLocked()
	svc.peerMu.Unlock()

	if got := count.Load(); got != 2 {
		t.Fatalf("expected 2 publishes after real change, got %d", got)
	}
}

// TestPublishAggregateStatusChangedLocked_HeartbeatRepublishesUnchangedSnapshot
// guards the recovery path for lossy ebus delivery on the aggregate-status
// topic. If the first publish is dropped because a subscriber inbox was
// full, dedup must not strand the subscriber: once the heartbeat interval
// elapses (observed via the ComputedAt wall-clock), the helper must
// re-publish even though content is byte-identical. This is also what
// keeps the Desktop UI's "last checked" timestamp advancing on a quiet
// but healthy node, since NodeStatusMonitor mirrors ComputedAt into
// status.CheckedAt.
func TestPublishAggregateStatusChangedLocked_HeartbeatRepublishesUnchangedSnapshot(t *testing.T) {
	t.Parallel()

	bus := newStormBus(t)
	var count atomic.Int32
	bus.Subscribe(ebus.TopicAggregateStatusChanged, func(_ domain.AggregateStatusSnapshot) {
		count.Add(1)
	}, ebus.WithSync())

	// Inject a controllable clock so the test can advance wall-clock time
	// deterministically — computeAggregateStatusLocked stamps ComputedAt
	// via time.Now().UTC() directly, so we drive the heartbeat by
	// manipulating lastAggregateStatusPublishAt instead of clock injection.
	svc := &Service{
		eventBus:        bus,
		health:          make(map[domain.PeerAddress]*peerHealth),
		pending:         make(map[domain.PeerAddress][]pendingFrame),
		peerIDs:         make(map[domain.PeerAddress]domain.PeerIdentity),
		peerBuilds:      make(map[domain.PeerAddress]int),
		peerVersions:    make(map[domain.PeerAddress]string),
		aggregateStatus: domain.AggregateStatusSnapshot{Status: domain.NetworkStatusOffline},
	}

	svc.health["10.0.0.1:1000"] = &peerHealth{
		Address: "10.0.0.1:1000", Connected: false, State: peerStateReconnecting,
	}
	svc.pending["10.0.0.1:1000"] = nil

	// First publish — content moves to reconnecting.
	svc.peerMu.Lock()
	svc.publishAggregateStatusChangedLocked()
	svc.peerMu.Unlock()

	if got := count.Load(); got != 1 {
		t.Fatalf("expected 1 publish on first call, got %d", got)
	}

	// Back-date the last-publish anchor so the next recompute observes
	// an elapsed heartbeat interval. This simulates the real-world case
	// where the previous publish was delivered (or dropped) aggregateStatusHeartbeatInterval ago
	// and the content is still byte-identical.
	svc.peerMu.Lock()
	svc.lastAggregateStatusPublishAt = svc.lastAggregateStatusPublishAt.Add(-2 * aggregateStatusHeartbeatInterval)
	svc.peerMu.Unlock()

	svc.peerMu.Lock()
	svc.publishAggregateStatusChangedLocked()
	svc.peerMu.Unlock()

	if got := count.Load(); got != 2 {
		t.Fatalf("expected heartbeat republish on unchanged snapshot, got %d publishes total", got)
	}
}
