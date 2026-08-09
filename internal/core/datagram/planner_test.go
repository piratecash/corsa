package datagram

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// planner_test.go covers the scheduling half of §4.3: the gated
// direct-first branch, the walk to the first successful enqueue, the four
// synchronous outcomes and avoid_next_hop.

// TestDirectSessionIsTriedFirst pins step 1: a live, gated direct session
// beats every routing-table candidate, however well the relay ranks.
func TestDirectSessionIsTriedFirst(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	relay := domaintest.ID("relay")

	fixture.direct.set(dst, fixture.datagramPeer(dst, time.Minute, schedDType.String()))
	fixture.datagramPeer(relay, 10*time.Hour)
	fixture.routes.set(dst, fixture.route(relay, 1))

	outcome := fixture.send(t, dst)
	hop, queued := outcome.NextHop()
	if !queued || hop != dst {
		t.Fatalf("outcome = %s, want queued(dst)", outcome)
	}
	requireHops(t, fixture.sender.tried(), dst)
}

// TestDirectSessionRequiresTheRoleGate is §4.3 item 1 and §9: a live
// session is NOT enough. The peer must pass the same role check as any
// candidate — mesh_datagram_v1 — and the check cannot be walked around by the
// session being up.
func TestDirectSessionRequiresTheRoleGate(t *testing.T) {
	t.Parallel()

	relay := domaintest.ID("relay")
	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	fixture.direct.set(dst, PeerConnection{
		ConnectedAt:             fixture.clock().Add(-time.Hour),
		Advertised:              advertising(CapabilityDatagramTransitV1),
		DTypes:                  declaredDTypesOf([]string{schedDType.String()}),
		ReportedProtocolVersion: schedLocalVersion,
	})
	fixture.peers.set(relay, PeerConnection{
		ConnectedAt:             fixture.clock().Add(-time.Minute),
		Advertised:              advertising(CapabilityDatagramV1, CapabilityDatagramTransitV1),
		ReportedProtocolVersion: schedLocalVersion,
	})
	fixture.routes.set(dst, fixture.route(relay, 2))

	outcome := fixture.send(t, dst)
	hop, queued := outcome.NextHop()
	if !queued || hop != relay {
		t.Fatalf("outcome = %s, want the relay to take the frame", outcome)
	}
	requireHops(t, fixture.sender.tried(), relay)
}

// TestLastHopDTypeGateRejectsInsteadOfDroppingSilently is the core of §4.3
// item 1 and §9: sending to a dst that never declared this dtype does not
// happen, and the caller gets a REFUSAL rather than the silent drop it would
// have suffered at the destination.
//
// The relay route is present on purpose: the gate is a hard stop, not a
// reason to take the long way to the same dead end — otherwise the
// reachability probe, which must call this destination unreachable, would
// disagree with the send.
func TestLastHopDTypeGateRejectsInsteadOfDroppingSilently(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	relay := domaintest.ID("relay")

	// The destination rolled back to a build without the handler: its
	// handshake set no longer carries the type, though a lookup a moment
	// ago said it did.
	fixture.direct.set(dst, PeerConnection{
		ConnectedAt:             fixture.clock().Add(-time.Hour),
		Advertised:              advertising(CapabilityDatagramV1, CapabilityDatagramTransitV1),
		DTypes:                  declaredDTypesOf([]string{"some_other_type"}),
		ReportedProtocolVersion: schedLocalVersion,
	})
	fixture.datagramPeer(relay, time.Hour)
	fixture.routes.set(dst, fixture.route(relay, 2))

	outcome := fixture.send(t, dst)
	if outcome.Kind() != SendRejected {
		t.Fatalf("outcome = %s, want rejected", outcome)
	}
	reason, rejected := outcome.Rejection()
	if !rejected || reason != RejectionUnsupportedDType {
		t.Fatalf("rejection reason = %s, want unsupported_dtype", reason)
	}
	if tried := fixture.sender.tried(); len(tried) != 0 {
		t.Fatalf("nothing may be enqueued after the last-hop gate, tried %v", tried)
	}
	if mustReachable(t, fixture.scheduler, mustReachabilityQuery(t, ReachabilityQueryOpts{Dst: dst, DType: schedDType})).Reachable() {
		t.Fatal("the probe must call this destination unreachable for this type")
	}
}

// TestLastHopGateRefusesADestinationThatDeclaredNoType is the send-side half of
// §6.1. A destination that named no type is an endpoint for none, and it
// reaches that state two ways which the wire tells apart and the gate does not:
// a node with an empty type registry says `"dtypes": []`, while a node that
// never sent the field says nothing at all.
//
// Both are asserted with a type from the WITHDRAWN baseline, because that is
// the set an absent field used to be credited with: it is the case a
// regression would wave through. And the refusal has to be a caller-visible
// `rejected`, never the silent drop at the destination the gate exists to
// prevent.
func TestLastHopGateRefusesADestinationThatDeclaredNoType(t *testing.T) {
	t.Parallel()

	cases := map[string]struct {
		declared DeclaredDTypes
		dtype    domain.DType
	}{
		"explicitly empty, withdrawn baseline type": {NewDeclaredDTypes(domain.ParseDeclaredDTypes(nil)), "get_identity"},
		"explicitly empty, unrelated type":          {NewDeclaredDTypes(domain.ParseDeclaredDTypes(nil)), schedDType},
		"field never sent, withdrawn baseline type": {NewDeclaredDTypes(domain.AbsentDTypes()), "get_identity"},
		"field never sent, unrelated type":          {NewDeclaredDTypes(domain.AbsentDTypes()), schedDType},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			dtype := tc.dtype
			fixture := newSchedFixture(t, schedFixtureOpts{})
			dst := domaintest.ID("dst")
			relay := domaintest.ID("relay")

			fixture.direct.set(dst, PeerConnection{
				ConnectedAt:             fixture.clock().Add(-time.Hour),
				Advertised:              advertising(CapabilityDatagramV1, CapabilityDatagramTransitV1),
				DTypes:                  tc.declared,
				ReportedProtocolVersion: schedLocalVersion,
			})
			// A live relay route exists on purpose: the gate is a hard stop,
			// not a reason to take the long way to the same dead end.
			fixture.datagramPeer(relay, time.Hour)
			fixture.routes.set(dst, fixture.route(relay, 2))

			outcome := fixture.send(t, dst, withDType(dtype))
			if outcome.Kind() != SendRejected {
				t.Fatalf("outcome = %s, want rejected", outcome)
			}
			reason, rejected := outcome.Rejection()
			if !rejected || reason != RejectionUnsupportedDType {
				t.Fatalf("rejection reason = %s, want unsupported_dtype", reason)
			}
			if tried := fixture.sender.tried(); len(tried) != 0 {
				t.Fatalf("nothing may be enqueued after the last-hop gate, tried %v", tried)
			}

			// The probe must answer the same, with the same reason: its
			// guarantee is that an unreachable destination would not have
			// been queued either.
			probe := mustReachable(t, fixture.scheduler, mustReachabilityQuery(t, ReachabilityQueryOpts{Dst: dst, DType: dtype}))
			if probe.Reachable() {
				t.Fatal("the probe called a destination reachable that the send refuses")
			}
			if !probe.UnsupportedDType() {
				t.Fatal("the probe refused for the wrong reason: unsupported_dtype is what a rolled-back or handler-less destination must report")
			}
		})
	}
}

// TestLastHopGateLetsADeclaredTypeThrough pins the positive half of §6.1: the
// ONE thing that opens the gate is the destination having listed the type.
//
// The second half of the test is what makes the first mean something — the
// same destination, the same connection, a type it did not list, refused. A
// gate that let everything through would pass the first assertion alone.
func TestLastHopGateLetsADeclaredTypeThrough(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	fixture.direct.set(dst, fixture.datagramPeer(dst, time.Hour, "get_identity"))

	outcome := fixture.send(t, dst, withDType("get_identity"))
	if hop, queued := outcome.NextHop(); !queued || hop != dst {
		t.Fatalf("outcome = %s, want queued(dst) for the declared type", outcome)
	}

	fixture.sender.reset()
	rejected := fixture.send(t, dst)
	if rejected.Kind() != SendRejected {
		t.Fatalf("outcome = %s, want rejected for the undeclared type", rejected)
	}
}

// TestLastHopGateAppliesOnTransit pins "the gate applies ALWAYS": a relay
// forwarding to the destination is the last hop too, and a frame it cannot
// handle is not handed over. For a transit frame the refusal is a silent
// drop, not a caller-visible outcome.
func TestLastHopGateAppliesOnTransit(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	upstream := domaintest.ID("upstream")

	fixture.direct.set(dst, PeerConnection{
		ConnectedAt:             fixture.clock().Add(-time.Hour),
		Advertised:              advertising(CapabilityDatagramV1, CapabilityDatagramTransitV1),
		ReportedProtocolVersion: schedLocalVersion,
	})
	fixture.routes.setCached(dst, fixture.route(dst, 1))

	// The frame never reaches the scheduler at all: the early deliverability
	// sieve of §4.1 step 7 finds no viable candidate and drops it BEFORE the
	// signature is paid for. Either way the refusal is silent — that is the
	// asymmetry of §4.3 item 4 — and nothing is enqueued.
	result := fixture.transit(t, upstream, dst)
	if !result.Dropped() {
		t.Fatalf("outcome = %s, want a silent drop", result.Outcome())
	}
	if tried := fixture.sender.tried(); len(tried) != 0 {
		t.Fatalf("nothing may be enqueued, tried %v", tried)
	}
}

// TestQueuedCarriesTheActuallyChosenNextHop pins §4.3 item 3 together with
// the reason `queued` carries a hop at all: candidates are walked until one
// accepts, so the caller must learn where the frame really went — otherwise
// the next retry's avoid_next_hop would exclude the wrong peer.
func TestQueuedCarriesTheActuallyChosenNextHop(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	best := domaintest.ID("relay-best")
	second := domaintest.ID("relay-second")

	fixture.datagramPeer(best, 10*time.Hour)
	fixture.datagramPeer(second, time.Hour)
	fixture.routes.set(dst, fixture.route(best, 1), fixture.route(second, 2))
	fixture.sender.refuseHop(best)

	outcome := fixture.send(t, dst)
	hop, queued := outcome.NextHop()
	if !queued || hop != second {
		t.Fatalf("outcome = %s, want queued(relay-second)", outcome)
	}
	requireHops(t, fixture.sender.tried(), best, second)

	// The retry excludes the peer the frame ACTUALLY went to.
	fixture.sender.reset()
	retry := fixture.sendAvoiding(t, dst, AvoidNextHop(hop))
	// `best` is the only candidate left and its queue refuses: a local,
	// temporary failure — not "no route", which would tell the caller to wait
	// for a route it already has.
	if retry.Kind() != SendFailed {
		t.Fatalf("retry outcome = %s, want failed (best refuses, second excluded)", retry)
	}
	requireHops(t, fixture.sender.tried(), best)
}

// TestAvoidNextHopCoversTheDirectBranch pins the rule that makes the
// promise real for the most common case: the exclusion is applied BEFORE
// direct-first, so a retry to a destination with a live session does not
// go straight back into it. With no other candidate the send honestly
// answers no_route rather than pretending.
func TestAvoidNextHopCoversTheDirectBranch(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	fixture.direct.set(dst, fixture.datagramPeer(dst, time.Hour, schedDType.String()))

	outcome := fixture.sendAvoiding(t, dst, AvoidNextHop(dst))
	if outcome.Kind() != SendNoRoute {
		t.Fatalf("outcome = %s, want no_route", outcome)
	}
	if tried := fixture.sender.tried(); len(tried) != 0 {
		t.Fatalf("the excluded peer must not be attempted, tried %v", tried)
	}

	// Without the exclusion the same send goes straight to the direct
	// session — proof that the exclusion, not a missing session, caused
	// the refusal.
	if again := fixture.send(t, dst); again.Kind() != SendQueued {
		t.Fatalf("outcome without avoid = %s, want queued", again)
	}
}

// TestAvoidNextHopChangesTheFirstHop shows the guarantee it does make: a
// different FIRST hop, with the second candidate taking the frame.
func TestAvoidNextHopChangesTheFirstHop(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	first := domaintest.ID("relay-first")
	other := domaintest.ID("relay-other")

	fixture.datagramPeer(first, 10*time.Hour)
	fixture.datagramPeer(other, time.Hour)
	fixture.routes.set(dst, fixture.route(first, 1), fixture.route(other, 1))

	outcome := fixture.sendAvoiding(t, dst, AvoidNextHop(first))
	hop, queued := outcome.NextHop()
	if !queued || hop != other {
		t.Fatalf("outcome = %s, want queued(relay-other)", outcome)
	}
	requireHops(t, fixture.sender.tried(), other)
}

// TestNoCandidatesIsSynchronousForLocalAndSilentForTransit pins §4.3
// item 4: the same situation is a synchronous refusal to a local caller
// and a silent drop for a frame passing through.
func TestNoCandidatesIsSynchronousForLocalAndSilentForTransit(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")

	local := fixture.send(t, dst)
	if local.Kind() != SendNoRoute {
		t.Fatalf("local outcome = %s, want no_route", local)
	}
	if local.SilentDrop() {
		t.Fatal("a locally created frame must never be dropped silently")
	}

	transit := fixture.transit(t, domaintest.ID("upstream"), dst)
	if !transit.Dropped() {
		t.Fatalf("a transit frame with no candidates is a silent drop, got %s", transit.Outcome())
	}
	if tried := fixture.sender.tried(); len(tried) != 0 {
		t.Fatalf("nothing may be enqueued, tried %v", tried)
	}
}

// TestSplitHorizonExcludesTheIncomingNeighbour keeps a transit frame from
// bouncing back to the peer that handed it over.
func TestSplitHorizonExcludesTheIncomingNeighbour(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	upstream := domaintest.ID("upstream")
	onward := domaintest.ID("onward")

	fixture.datagramPeer(upstream, 10*time.Hour)
	fixture.datagramPeer(onward, time.Hour)
	fixture.routes.setCached(dst, fixture.route(upstream, 1), fixture.route(onward, 4))

	result := fixture.transit(t, upstream, dst)
	if result.Outcome() != InboundForwarded {
		t.Fatalf("outcome = %s (%s), want forwarded", result.Outcome(), result.Reason())
	}
	requireHops(t, fixture.sender.tried(), onward)
}

// TestFreshnessDependsOnWhoCreatedTheFrame pins the §4.3 freshness rule:
// a locally originated send reads the fresh lookup so a route accepted a
// moment ago is visible, a transit frame reads the coalesced snapshot.
func TestFreshnessDependsOnWhoCreatedTheFrame(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	freshOnly := domaintest.ID("relay-fresh-only")
	cachedOnly := domaintest.ID("relay-cached-only")

	fixture.datagramPeer(freshOnly, time.Hour)
	fixture.datagramPeer(cachedOnly, time.Hour)
	fixture.routes.setFresh(dst, fixture.route(freshOnly, 1))
	fixture.routes.setCached(dst, fixture.route(cachedOnly, 1))

	local := fixture.send(t, dst)
	if hop, _ := local.NextHop(); hop != freshOnly {
		t.Fatalf("a local send must read the fresh lookup, went to %s", hop)
	}
	if fresh, cached := fixture.routes.sourceCalls(); fresh != 1 || cached != 0 {
		t.Fatalf("a local send read fresh=%d cached=%d, want 1/0", fresh, cached)
	}
	fixture.sender.reset()
	transit := fixture.transit(t, domaintest.ID("upstream"), dst)
	if transit.Outcome() != InboundForwarded {
		t.Fatalf("transit outcome = %s (%s)", transit.Outcome(), transit.Reason())
	}
	requireHops(t, fixture.sender.tried(), cachedOnly)
	// The transit frame touches the cached source twice — once for the early
	// deliverability sieve of §4.1 step 7, once for the selection itself — and
	// never the fresh lookup. That the FRESH counter is still 1 is the
	// assertion; the cached count only has to stay away from it.
	if fresh, _ := fixture.routes.sourceCalls(); fresh != 1 {
		t.Fatalf("a transit frame read the fresh lookup %d times, want 1 (the local send's)", fresh)
	}
}

// TestOutcomeIsFinalFromTheEnqueue pins the §4.3 finality rule that
// protects the migration's transport fallback (§8): a refusal discovered
// AFTER the enqueue does not rewrite `queued` into `rejected`, so the same
// ciphertext never leaves a second time in a legacy envelope.
//
// The late refusal used here is the real one: Commit(forwarded) fails after the
// frame is already in the queue. The layer's memory answers `fail` for exactly
// one reason — the record is gone — and the cache reaches that state by itself
// when the abandoned-reservation watchdog reclaims a branch that outlived
// replay_until plus the whole hop budget. The fixture puts the cache in the same
// state at the same moment, from the emitter the publish runs through. §4.1
// answers with a Release and a log, and §4.3 forbids it from touching the
// outcome.
func TestOutcomeIsFinalFromTheEnqueue(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})

	dst := domaintest.ID("dst")
	relay := domaintest.ID("relay")
	fixture.datagramPeer(relay, time.Hour)
	fixture.routes.set(dst, fixture.route(relay, 1))

	var reclaimed bool
	fixture.sender.onEmit = func(frame protocol.DatagramFrame) {
		if reclaimed {
			return
		}
		reclaimed = forgetReplayRecord(fixture.replay, replayKeyOf(t, frame))
	}

	outcome := fixture.send(t, dst)
	if !reclaimed {
		t.Fatal("the fixture never reclaimed the reservation: the Commit below cannot have failed")
	}
	if outcome.Kind() != SendQueued {
		t.Fatalf("outcome = %s, want queued despite the failed Commit", outcome)
	}
	hop, queued := outcome.NextHop()
	if !queued || hop != relay {
		t.Fatalf("the chosen next hop must survive a late refusal, got %s", outcome)
	}
	requireHops(t, fixture.sender.tried(), relay)
	calls := replayCallsOf(fixture.replay)
	if calls.commits != 0 {
		t.Fatalf("the Commit landed after all (%d): the premise of this test never armed", calls.commits)
	}
	if calls.releases != 1 {
		t.Fatalf("Commit.fail must Release the key, releases = %d", calls.releases)
	}
}

// TestRejectionNamesTheMissingCapability keeps the refusal metric useful:
// "rejected" alone cannot tell an operator which name the network expects.
//
// It is also the regression the second review named: MissingCapability() was
// never populated on the real send path, because the public path threw the
// scheduler's refusal away and answered `no_route`.
func TestRejectionNamesTheMissingCapability(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	// The destination does not speak the envelope at all, so the role gate
	// refuses it and has a name to report.
	fixture.direct.set(dst, PeerConnection{
		ConnectedAt:             fixture.clock().Add(-time.Hour),
		Advertised:              advertising(CapabilityDatagramTransitV1),
		DTypes:                  declaredDTypesOf([]string{schedDType.String()}),
		ReportedProtocolVersion: schedLocalVersion,
	})

	outcome := fixture.send(t, dst)
	if outcome.Kind() != SendRejected {
		t.Fatalf("outcome = %s, want rejected", outcome)
	}
	missing, named := outcome.MissingCapability()
	if !named || missing != CapabilityDatagramV1 {
		t.Fatalf("missing capability = %q (named=%v), want %q", missing, named, CapabilityDatagramV1)
	}
}

// TestAllCandidatesRefusingIsALocalFailure pins the boundary between the walk
// and the outcome: when every ADMITTED candidate refuses the enqueue, nothing
// about policy failed, so the caller must not get `rejected` — and nothing
// about the route failed either, so it must not get `no_route` and be told to
// wait for a route that is already there. §4.3 has the third outcome for
// exactly this: a temporary local failure, provably before the enqueue, worth
// a retry with backoff.
func TestAllCandidatesRefusingIsALocalFailure(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	first := domaintest.ID("relay-first")
	second := domaintest.ID("relay-second")

	fixture.datagramPeer(first, 10*time.Hour)
	fixture.datagramPeer(second, time.Hour)
	fixture.routes.set(dst, fixture.route(first, 1), fixture.route(second, 2))
	fixture.sender.refuseHop(first)
	fixture.sender.refuseHop(second)

	outcome := fixture.send(t, dst)
	if outcome.Kind() != SendFailed {
		t.Fatalf("outcome = %s, want failed", outcome)
	}
	if outcome.Err() == nil {
		t.Fatal("a failed outcome must carry its cause")
	}
	requireHops(t, fixture.sender.tried(), first, second)
}

// A gate refusal of ONE peer must not become the outcome of a walk in which
// OTHER, admitted peers were tried and failed to take the frame. §4.3 defines
// `rejected` as "repeating is pointless", and reporting it here would stop the
// retries of a caller whose real problem is a temporary local one.
func TestEnqueueFailureIsNotReportedAsAForeignPeersRejection(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	gated := domaintest.ID("relay-gated")
	healthy := domaintest.ID("relay-healthy")

	// The gated peer speaks the envelope but will not forward somebody else's
	// frame; the healthy one passes every gate and refuses the enqueue.
	fixture.peers.set(gated, PeerConnection{
		ConnectedAt:             fixture.clock().Add(-time.Hour),
		Advertised:              advertising(CapabilityDatagramV1),
		ReportedProtocolVersion: schedLocalVersion,
	})
	fixture.datagramPeer(healthy, time.Hour)

	fixture.routes.set(dst, fixture.route(gated, 1), fixture.route(healthy, 2))
	fixture.sender.failHop(healthy)

	outcome := fixture.send(t, dst)
	if outcome.Kind() == SendRejected {
		t.Fatalf("outcome = %s, want the local failure, not a foreign peer's policy verdict", outcome)
	}
	if outcome.Kind() != SendFailed {
		t.Fatalf("outcome = %s, want failed", outcome)
	}
	if outcome.Err() == nil {
		t.Fatal("a failed outcome must carry its cause")
	}
}
