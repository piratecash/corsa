package datagram

import (
	"context"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// first_hop_preference_test.go covers PreferredFirstHops — the seam the node's
// guard set reaches the candidate walk through.
//
// The walk stops at the first candidate whose queue accepts the frame, so
// "preferred" has to mean "earlier in this slice" and nothing else. A
// preference that merely existed as a field would look identical from outside
// and would pin no first hop at all.

func prefTestCandidate(hop domain.PeerIdentity, hops int) RouteCandidate {
	return RouteCandidate{nextHop: hop, hops: hops}
}

func prefTestOrder(candidates []RouteCandidate) []domain.PeerIdentity {
	out := make([]domain.PeerIdentity, 0, len(candidates))
	for _, candidate := range candidates {
		out = append(out, candidate.nextHop)
	}
	return out
}

// TestPreferredHopsGoFirstInTheirOwnOrder: the head is the caller's order, not
// the ranking's. A preference that re-sorted itself by hop count would be a
// rotation whenever the topology moved, which is what the guard model forbids.
func TestPreferredHopsGoFirstInTheirOwnOrder(t *testing.T) {
	best := domaintest.ID("best-by-ranking")
	second := domaintest.ID("second-guard")
	first := domaintest.ID("first-guard")

	// Ranking order: best (1 hop), first (5 hops), second (9 hops).
	candidates := []RouteCandidate{
		prefTestCandidate(best, 1),
		prefTestCandidate(first, 5),
		prefTestCandidate(second, 9),
	}
	got := prefTestOrder(PreferFirstHops(first, second).hoist(candidates))

	want := []domain.PeerIdentity{first, second, best}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order %v, want %v: the preferred hops must lead, in the "+
				"caller's order, and the rest keep the ranking behind them", got, want)
		}
	}
}

// TestAPreferenceNeverRemovesACandidate. The layer's job is to place the frame.
// Turning a privacy preference into a filter would make a guard with no route
// to the destination into a delivery failure, which is not a trade the
// transport gets to make for the caller.
func TestAPreferenceNeverRemovesACandidate(t *testing.T) {
	reachable := domaintest.ID("reachable")
	unrelated := domaintest.ID("guard-with-no-route")

	candidates := []RouteCandidate{prefTestCandidate(reachable, 3)}
	got := PreferFirstHops(unrelated).hoist(candidates)
	if len(got) != 1 || got[0].nextHop != reachable {
		t.Fatalf("candidates after hoisting a guard that is not among them: %v, want "+
			"the original list untouched", prefTestOrder(got))
	}
}

// TestAnEmptyPreferenceLeavesTheRankingAlone: the zero value must be inert, or
// every send that does not opt in would silently acquire a policy.
func TestAnEmptyPreferenceLeavesTheRankingAlone(t *testing.T) {
	a := domaintest.ID("a")
	b := domaintest.ID("b")
	candidates := []RouteCandidate{prefTestCandidate(a, 1), prefTestCandidate(b, 2)}

	for name, preference := range map[string]PreferredFirstHops{
		"zero value":    {},
		"constructed":   NoFirstHopPreference(),
		"only a zero":   PreferFirstHops(domain.PeerIdentity{}),
		"nothing given": PreferFirstHops(),
	} {
		t.Run(name, func(t *testing.T) {
			if !preference.Empty() {
				t.Fatal("preference reports itself non-empty")
			}
			got := prefTestOrder(preference.hoist(candidates))
			if got[0] != a || got[1] != b {
				t.Fatalf("ranking order changed to %v with no preference set", got)
			}
		})
	}
}

// TestASendReallyLeavesThroughThePreferredHop is the wiring test, and the one
// that would catch the field being carried around and never read.
//
// Two relays reach the destination and the RANKING prefers one of them (fewer
// hops, older connection). A send that names the other must leave through the
// other — repeatedly, because a guard that only wins the first send is not a
// guard at all.
func TestASendReallyLeavesThroughThePreferredHop(t *testing.T) {
	t.Parallel()

	fixture := newSchedFixture(t, schedFixtureOpts{})
	dst := domaintest.ID("dst")
	favoured := domaintest.ID("relay-the-ranking-likes")
	guard := domaintest.ID("relay-the-guard-set-picked")

	// The ranking's own order puts `favoured` first: fewer hops AND the older
	// connection, which are the top two keys of routeCandidateLess.
	fixture.datagramPeer(favoured, 10*time.Hour)
	fixture.datagramPeer(guard, time.Minute)
	fixture.routes.set(dst, fixture.route(favoured, 1), fixture.route(guard, 4))

	if outcome := fixture.send(t, dst); mustNextHop(t, outcome) != favoured {
		t.Fatalf("without a preference the frame left through %s, want the "+
			"ranking's own choice %s — the fixture is not set up as intended",
			mustNextHop(t, outcome), favoured)
	}

	for i := 0; i < 5; i++ {
		outcome := fixture.pipeline.SendLocal(context.Background(), LocalSendOpts{
			Frame:    fixture.frame(t, fixture.signer, fixture.local, dst),
			FirstHop: PreferFirstHops(guard),
		})
		if hop := mustNextHop(t, outcome); hop != guard {
			t.Fatalf("send %d left through %s, want the preferred first hop %s: "+
				"a preference the walk does not read pins nothing", i, hop, guard)
		}
	}
}

func mustNextHop(t *testing.T, outcome SendOutcome) domain.PeerIdentity {
	t.Helper()
	hop, queued := outcome.NextHop()
	if !queued {
		t.Fatalf("outcome %s, want the frame queued somewhere", outcome)
	}
	return hop
}

// TestPeersIsACopy: the node reads the preference back to attribute a send
// outcome, and a shared backing array would let that reading mutate the
// preference a concurrent send is walking.
func TestPeersIsACopy(t *testing.T) {
	a := domaintest.ID("a")
	b := domaintest.ID("b")
	preference := PreferFirstHops(a, b)

	peers := preference.Peers()
	peers[0] = domaintest.ID("overwritten")

	if again := preference.Peers(); again[0] != a {
		t.Fatal("mutating the returned slice changed the preference itself")
	}
}
