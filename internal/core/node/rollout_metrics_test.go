package node

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/netcore"
)

// newCompositionFixture builds the smallest Service the census walk needs: it
// reads s.sessions and the inbound connection registry and nothing else, so a
// full node is not required to pin what the census counts.
func newCompositionFixture(t *testing.T) *Service {
	t.Helper()
	return &Service{}
}

// peerIdentityFromLabel makes a distinct identity out of a readable label, so
// a failing assertion names the peer instead of twenty hex bytes.
func peerIdentityFromLabel(label string) domain.PeerIdentity {
	var id domain.PeerIdentity
	copy(id[:], label)
	return id
}

func capsRoutingV3() []domain.Capability {
	return []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3, domain.CapMeshRelayV1}
}

func capsRoutingOnly() []domain.Capability {
	return []domain.Capability{domain.CapMeshRoutingV1, domain.CapMeshRoutingV3}
}

func capsRelayOnly() []domain.Capability {
	return []domain.Capability{domain.CapMeshRelayV1}
}

// declaringSession builds an outbound session whose RAW handshake
// self-description names caps.
//
// The census must read THIS, not session.capabilities: the latter is the
// NEGOTIATED set — the intersection with our own — so a locally disabled
// capability is absent from it for every neighbour alive.
func declaringSession(id domain.PeerIdentity, advertised ...domain.Capability) *peerSession {
	names := make([]domain.CapabilityName, 0, len(advertised))
	for _, capability := range advertised {
		names = append(names, domain.CapabilityName(capability))
	}
	return &peerSession{
		peerIdentity: id,
		declarations: netcore.HandshakeDeclarations{AdvertisedNames: names},
	}
}

// Rollout telemetry (docs/refactoring/dht/05-rollout-metrics.md).
//
// The property under test: an outbound attempt is classified by the TYPE of
// its error, and the four outcomes always sum to the attempts. Both halves
// matter — the first is what keeps the metric from depending on wording nobody
// treats as a contract, the second is what makes a missing case visible
// instead of quietly absorbed.

// TestSessionOutcomeClassificationUsesErrorTypes pins the classification and,
// with it, the rule that a wrapped error is still recognised: the dial path
// wraps the transport failure, and the CM path wraps the version refusal, so a
// classifier comparing against unwrapped values would put both in "other".
func TestSessionOutcomeClassificationUsesErrorTypes(t *testing.T) {
	counters := &sessionOutcomeCounters{startedAt: time.Unix(0, 0).UTC()}

	counters.record(nil)
	counters.record(fmt.Errorf("%w: %w", errPeerDialTransport, &net.OpError{Op: "dial"}))
	counters.record(fmt.Errorf("dialing peer: %w", fmt.Errorf("%w: version 3 < 29", errIncompatibleProtocol)))
	counters.record(errors.New("something else entirely"))

	stats := counters.snapshot()
	if stats.Succeeded != 1 {
		t.Fatalf("succeeded = %d, want 1", stats.Succeeded)
	}
	if stats.ErrorsConnect != 1 {
		t.Fatalf("errors_connect = %d, want 1 — a wrapped transport failure must still be recognised", stats.ErrorsConnect)
	}
	if stats.ErrorsCompat != 1 {
		t.Fatalf("errors_compat = %d, want 1 — a doubly wrapped version refusal must still be recognised", stats.ErrorsCompat)
	}
	if stats.ErrorsOther != 1 {
		t.Fatalf("errors_other = %d, want 1", stats.ErrorsOther)
	}

	// The invariant that makes the numbers readable: every attempt landed in
	// exactly one bucket. A future failure mode nobody classified will show up
	// as errors_other rising, never as a gap between the sum and the total.
	sum := stats.Succeeded + stats.ErrorsConnect + stats.ErrorsCompat + stats.ErrorsOther
	if sum != stats.Attempts {
		t.Fatalf("outcomes sum to %d but attempts = %d", sum, stats.Attempts)
	}
	if stats.Attempts != 4 {
		t.Fatalf("attempts = %d, want 4", stats.Attempts)
	}
}

// TestSessionOutcomeCountersAreNilSafe pins that a Service assembled without
// the counters (partial fixtures do exist in this package) keeps dialling.
func TestSessionOutcomeCountersAreNilSafe(t *testing.T) {
	var counters *sessionOutcomeCounters
	counters.record(nil)
	counters.record(errors.New("boom"))
	if stats := counters.snapshot(); stats.Attempts != 0 {
		t.Fatalf("nil counters recorded %d attempts", stats.Attempts)
	}
}

// TestNeighbourCompositionReportsNotReadyBeforeFirstRefresh pins the
// difference between "we have not looked yet" and "we looked and there is
// nobody". Both are all-zero rows, and a reader that cannot tell them apart
// will eventually report the first as the second.
func TestNeighbourCompositionReportsNotReadyBeforeFirstRefresh(t *testing.T) {
	svc := &Service{}
	composition := svc.NeighbourComposition()
	if composition.Ready {
		t.Fatal("a census that never ran must not report Ready")
	}
	if !composition.UpdatedAt.IsZero() {
		t.Fatalf("updated_at = %v, want zero before the first refresh", composition.UpdatedAt)
	}
}

// TestNeighbourCompositionCountsConnectionsAndPeersApart pins the two counts
// that diverge exactly when it matters.
//
// Two sockets of ONE neighbour are two connections and one peer. Reporting
// only connections would let a single reconnecting peer look like a rollout
// wave; reporting only peers would hide that half our sockets still speak the
// old format.
func TestNeighbourCompositionCountsConnectionsAndPeersApart(t *testing.T) {
	svc := newCompositionFixture(t)

	one := peerIdentityFromLabel("peer-one")
	svc.sessions = map[domain.PeerAddress]*peerSession{
		"one:1": declaringSession(one, capsRoutingV3()...),
		"one:2": declaringSession(one, capsRoutingV3()...),
	}

	composition := svc.collectNeighbourComposition()
	if composition.Connections != 2 {
		t.Fatalf("connections = %d, want 2", composition.Connections)
	}
	if composition.Peers != 1 {
		t.Fatalf("peers = %d, want 1 — two sockets of one neighbour are one neighbour", composition.Peers)
	}
	if composition.RoutingV3Triplet.Connections != 2 || composition.RoutingV3Triplet.Peers != 1 {
		t.Fatalf("triplet = %+v, want 2 connections / 1 peer", composition.RoutingV3Triplet)
	}
}

// TestNeighbourCompositionNeverMergesCapabilitiesAcrossConnections pins the
// rule that makes the triplet row trustworthy: a peer whose two sockets
// advertise different halves of the triplet supports it on NEITHER, and
// claiming otherwise would send v3 frames down a connection that refuses them.
func TestNeighbourCompositionNeverMergesCapabilitiesAcrossConnections(t *testing.T) {
	svc := newCompositionFixture(t)

	split := peerIdentityFromLabel("split-peer")
	svc.sessions = map[domain.PeerAddress]*peerSession{
		"split:1": declaringSession(split, capsRoutingOnly()...),
		"split:2": declaringSession(split, capsRelayOnly()...),
	}

	composition := svc.collectNeighbourComposition()
	if composition.RoutingV3Triplet.Connections != 0 {
		t.Fatalf("triplet connections = %d, want 0 — the halves live on different sockets",
			composition.RoutingV3Triplet.Connections)
	}
	if composition.RoutingV3Triplet.Peers != 0 {
		t.Fatalf("triplet peers = %d, want 0", composition.RoutingV3Triplet.Peers)
	}
	if composition.Connections != 2 || composition.Peers != 1 {
		t.Fatalf("population = %d connections / %d peers, want 2 / 1",
			composition.Connections, composition.Peers)
	}
}

// TestNeighbourCompositionCountsUnprovenIdentitiesApart pins that an
// advertisement on a session WE dialled is reported as what it is — a claim by
// somebody who has not authenticated to us. The handshake proves the dialler
// to the listener, so on an outbound session the welcome address is a name the
// remote picked.
func TestNeighbourCompositionCountsUnprovenIdentitiesApart(t *testing.T) {
	svc := newCompositionFixture(t)
	svc.sessions = map[domain.PeerAddress]*peerSession{
		"claimed:1": declaringSession(peerIdentityFromLabel("claimed"), capsRoutingV3()...),
	}

	composition := svc.collectNeighbourComposition()
	if composition.IdentityUnproven != 1 {
		t.Fatalf("identity_unproven = %d, want 1 for an outbound session", composition.IdentityUnproven)
	}
	if composition.IdentityUnknown != 0 {
		t.Fatalf("identity_unknown = %d, want 0", composition.IdentityUnknown)
	}
}

// TestNeighbourCompositionCountsPeersWithoutKnownCapabilities pins that the
// census counts the neighbours a rollout is WAITING for. Filtering them out
// would answer "how many of the upgraded are upgraded", which is always
// everybody.
func TestNeighbourCompositionCountsPeersWithoutKnownCapabilities(t *testing.T) {
	svc := newCompositionFixture(t)
	svc.sessions = map[domain.PeerAddress]*peerSession{
		"legacy:1": declaringSession(peerIdentityFromLabel("legacy")),
	}

	composition := svc.collectNeighbourComposition()
	if composition.Connections != 1 || composition.Peers != 1 {
		t.Fatalf("population = %d / %d, want 1 / 1 — a neighbour advertising nothing is still a neighbour",
			composition.Connections, composition.Peers)
	}
	// Every known capability still gets a row, and the row is zero. A missing
	// row and a zero row are different answers early in a rollout.
	if len(composition.Capabilities) != len(rolloutCapabilities) {
		t.Fatalf("published %d capability rows, want %d — zero rows are the interesting ones",
			len(composition.Capabilities), len(rolloutCapabilities))
	}
	for _, usage := range composition.Capabilities {
		if usage.Connections != 0 || usage.Peers != 0 {
			t.Fatalf("row %+v is non-zero for a neighbour that advertised nothing", usage)
		}
	}
}

// TestNeighbourCompositionReadsTheRawAdvertisementNotTheIntersection is the
// regression guard for a defect that pointed operators at the wrong thing.
//
// session.capabilities is the NEGOTIATED set. With mesh_routing_v3 disabled
// LOCALLY it is empty of v3 for every neighbour, including the ones that
// advertised it — so a census built on it reports a fleet that never upgraded
// whenever the local config turns something off.
func TestNeighbourCompositionReadsTheRawAdvertisementNotTheIntersection(t *testing.T) {
	svc := newCompositionFixture(t)

	upgraded := peerIdentityFromLabel("upgraded")
	session := declaringSession(upgraded, capsRoutingV3()...)
	// Exactly the shape a v3-disabled build produces: the peer advertised the
	// triplet, the negotiated set kept nothing of it.
	session.capabilities = nil
	svc.sessions = map[domain.PeerAddress]*peerSession{"upgraded:1": session}

	composition := svc.collectNeighbourComposition()
	if composition.RoutingV3Triplet.Connections != 1 {
		t.Fatalf("triplet connections = %d, want 1 — the neighbour DID advertise it; "+
			"reading the negotiated set would blame the fleet for our own config",
			composition.RoutingV3Triplet.Connections)
	}
	for _, usage := range composition.Capabilities {
		if usage.Capability != domain.CapMeshRoutingV3 {
			continue
		}
		if usage.Connections != 1 {
			t.Fatalf("mesh_routing_v3 row = %+v, want 1 connection", usage)
		}
	}
}

// TestSessionOutcomeSnapshotAlwaysAddsUpUnderConcurrency pins the invariant
// against the race that made it a hope rather than a guarantee.
//
// Attempts used to be its own atomic, incremented next to the outcome. A
// reader landing between the two increments — or loading attempts before a new
// attempt finished and an outcome after — published a total that did not equal
// its parts. Deriving the total from the loaded outcomes makes that impossible.
func TestSessionOutcomeSnapshotAlwaysAddsUpUnderConcurrency(t *testing.T) {
	counters := &sessionOutcomeCounters{startedAt: time.Unix(0, 0).UTC()}

	const writers = 4
	const perWriter = 2000
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(writers)
	for w := 0; w < writers; w++ {
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				switch (w + i) % 4 {
				case 0:
					counters.record(nil)
				case 1:
					counters.record(fmt.Errorf("%w: refused", errPeerDialTransport))
				case 2:
					counters.record(fmt.Errorf("%w: too old", errIncompatibleProtocol))
				default:
					counters.record(errors.New("other"))
				}
			}
		}(w)
	}

	// Read continuously WHILE the writers run: the defect only shows up in a
	// snapshot taken mid-flight, so a test that reads after the writers are
	// done would have stayed green through it.
	go func() {
		defer close(done)
		for i := 0; i < 20000; i++ {
			stats := counters.snapshot()
			sum := stats.Succeeded + stats.ErrorsConnect + stats.ErrorsCompat + stats.ErrorsOther
			if sum != stats.Attempts {
				t.Errorf("mid-flight snapshot does not add up: outcomes %d, attempts %d", sum, stats.Attempts)
				return
			}
		}
	}()

	wg.Wait()
	<-done

	final := counters.snapshot()
	if final.Attempts != writers*perWriter {
		t.Fatalf("attempts = %d, want %d", final.Attempts, writers*perWriter)
	}
}

// TestSessionOutcomeSnapshotStampsItsOwnReadTime pins the closing edge of the
// period. See the mode-selection twin for why snapshot_at cannot serve.
func TestSessionOutcomeSnapshotStampsItsOwnReadTime(t *testing.T) {
	started := time.Now().UTC().Add(-time.Hour) // a period that really has started
	counters := &sessionOutcomeCounters{startedAt: started}

	stats := counters.snapshot()
	if stats.ReadAt.IsZero() {
		t.Fatal("read_at must be stamped even when no attempt was recorded")
	}
	if stats.ReadAt.Before(started) {
		t.Fatalf("read_at %v precedes started_at %v", stats.ReadAt, started)
	}
	if !stats.StartedAt.Equal(started) {
		t.Fatalf("started_at = %v, want %v", stats.StartedAt, started)
	}
}
