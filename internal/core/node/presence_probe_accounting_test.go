package node

import (
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// presence_probe_accounting_test.go covers what a probe's OUTCOME is allowed to
// conclude — about the contact, and about the neighbour that carried it.
//
// Both rules come from the same place: an event has to be attributed to
// whoever actually caused it. Silence caused by our own queue is not evidence
// about a person, and a neighbour that was never offered a frame did not refuse
// it.

// probeAccountingService builds the smallest node these rules need: a guard
// set and a projector, no network.
func probeAccountingService(t *testing.T, seed ...guardEntry) *Service {
	t.Helper()
	clock := &guardTestClock{at: time.Unix(1780000000, 0).UTC()}
	svc := &Service{
		presenceProjector: newPresenceProjector(),
		presenceClock:     clock.now,
	}
	svc.firstHopGuards = newFirstHopGuards(clock.now, &recordingGuardPersister{}, seed)
	svc.firstHopGuards.fuzz = func(time.Duration) time.Duration { return 0 }
	return svc
}

// TestOnlyOfferedGuardsAreBlamed is the accounting fix.
//
// A guard with no route to THIS destination never enters the candidate list, so
// the walk never offers it the frame. Blaming it put a working neighbour into
// back-off; a guard in back-off is skipped; the set then topped itself up with
// somebody new. The policy widened its own exposure out of an accounting error.
func TestOnlyOfferedGuardsAreBlamed(t *testing.T) {
	offered := domaintest.ID("offered-and-passed-over")
	carried := domaintest.ID("carried-the-frame")
	unrelated := domaintest.ID("no-route-to-this-target")

	svc := probeAccountingService(t, []guardEntry{
		{Identity: offered, SampledAt: time.Unix(1779999000, 0).UTC(), ConfirmedSeq: 1},
		{Identity: carried, SampledAt: time.Unix(1779999001, 0).UTC(), ConfirmedSeq: 2},
		{Identity: unrelated, SampledAt: time.Unix(1779999002, 0).UTC(), ConfirmedSeq: 3},
	}...)

	// The walk offered the frame to `offered` first; `carried` took it.
	// `unrelated` was never a candidate.
	svc.noteFirstHopsPassedOver([]domain.PeerIdentity{offered, carried}, carried)

	for _, entry := range svc.firstHopGuards.Entries() {
		switch entry.Identity {
		case offered:
			if entry.Failures == 0 {
				t.Fatal("the guard that was offered the frame and passed it over " +
					"recorded no failure: a dead first hop would be retried forever")
			}
		case unrelated, carried:
			if entry.Failures != 0 {
				t.Fatalf("%s was blamed without being offered the frame: a working "+
					"neighbour goes into back-off, is skipped, and the set replaces "+
					"it with somebody new", entry.Identity)
			}
		}
	}
}

// TestNothingAfterTheWinnerIsBlamed: the walk stops at the first acceptance, so
// candidates behind it were never asked at all.
func TestNothingAfterTheWinnerIsBlamed(t *testing.T) {
	winner := domaintest.ID("took-it")
	behind := domaintest.ID("never-reached")
	svc := probeAccountingService(t, []guardEntry{
		{Identity: winner, SampledAt: time.Unix(1779999000, 0).UTC()},
		{Identity: behind, SampledAt: time.Unix(1779999001, 0).UTC()},
	}...)

	svc.noteFirstHopsPassedOver([]domain.PeerIdentity{winner, behind}, winner)

	for _, entry := range svc.firstHopGuards.Entries() {
		if entry.Identity == behind && entry.Failures != 0 {
			t.Fatal("a candidate behind the accepting hop was blamed: the walk " +
				"never offered it the frame")
		}
	}
}

// TestALocalDropIsNotAStrike is the presence half.
//
// `queued` means the class queue took the frame and nothing more. Below it a
// send deadline, a discarded session queue and the writer itself can each lose
// the frame without telling anyone. Counting that silence against the contact
// made three local drops turn a live person grey — which
// docs/protocol/presence.md §4 forbids in as many words: a probe that did not
// reach the network is not evidence about them.
//
// The witness is per-FRAME, and both halves of that matter. A layer-wide
// counter missed losses below the class queue, so this failure survived it; and
// being layer-wide it also let ONE unrelated datagram dying anywhere suppress
// this probe's timeout, which under sustained backpressure meant no contact
// could ever be called absent.
func TestALocalDropIsNotAStrike(t *testing.T) {
	svc := probeAccountingService(t)
	prober := newPresenceProber(svc)
	target := domaintest.ID("alive-but-unreachable-locally")
	now := svc.presenceNow()

	// Three attempts whose frames never reached a socket. An attempt whose
	// witness was never closed is exactly that: the class queue dropped it on
	// its deadline, the session queue was discarded, the writer skipped an
	// expired ticket — none of which closes the channel.
	for i := 0; i < presenceDetectMult; i++ {
		label := domaintest.ID(string(rune('a'+i)) + "-attempt")
		prober.mu.Lock()
		prober.attempts[label] = presenceProbeAttempt{
			target: target,
			onWire: []<-chan struct{}{make(chan struct{})},
			sentAt: now.Add(-2 * presenceProbeTimeout),
		}
		prober.mu.Unlock()
		prober.expireStaleAttempts(now)
	}

	set := svc.presenceProjector.project(presenceTestInputs(now,
		map[domain.PeerIdentity]presenceRouteState{target: presenceRoutePresent}, nil))
	if got := set.Get(target); got.State == domain.PresenceOffline {
		t.Fatal("three probes lost inside our OWN layer made a contact offline: " +
			"silence we caused is not evidence about them")
	}
}

// TestARealTimeoutIsStillAStrike is the other half: the suppression above must
// not become a blanket excuse, or a contact who is genuinely gone never turns
// grey at all.
func TestARealTimeoutIsStillAStrike(t *testing.T) {
	svc := probeAccountingService(t)
	prober := newPresenceProber(svc)
	target := domaintest.ID("really-gone")
	now := svc.presenceNow()

	for i := 0; i < presenceDetectMult; i++ {
		label := domaintest.ID(string(rune('a'+i)) + "-attempt")
		onWire := make(chan struct{})
		// The writer closed it: these bytes really left the process, so the
		// silence that followed is the contact's.
		close(onWire)
		prober.mu.Lock()
		prober.attempts[label] = presenceProbeAttempt{
			target: target,
			onWire: []<-chan struct{}{onWire},
			sentAt: now.Add(-2 * presenceProbeTimeout),
		}
		prober.mu.Unlock()
		prober.expireStaleAttempts(now)
	}

	set := svc.presenceProjector.project(presenceTestInputs(now,
		map[domain.PeerIdentity]presenceRouteState{target: presenceRoutePresent}, nil))
	got := set.Get(target)
	if got.State != domain.PresenceOffline {
		t.Fatalf("after %d clean timeouts: got %s, want offline", presenceDetectMult, got)
	}
	if got.Source != domain.PresenceSourceProbeTimeout {
		t.Fatalf("source %s, want probe_timeout", got.Source)
	}
}

// TestRenewalHasRoomForARetry is the 7.5-minute bug.
//
// The prober skips a contact whose proof still has more than the renew lead
// left, so the first probe after a successful one goes out at
// validity − lead. With a 45 s lead that was ONE attempt before the proof
// expired: a single lost packet dropped a live contact out of `online` at
// exactly the validity, and nothing brought it back until the next cadence
// slot. The three-strike hysteresis guards the way INTO offline; nothing
// guarded the way out of online.
//
// It also meant the stated cadence never ran: probes went out every 405 s, not
// every 150 s, so `offline` took about twelve minutes instead of the 450 s the
// validity is derived from.
func TestRenewalHasRoomForARetry(t *testing.T) {
	// Worst case: every interval stretched by the jitter.
	worstInterval := time.Duration(float64(presenceProbeInterval) * (1 + presenceProbeJitter))
	attempts := 0
	for spent := time.Duration(0); spent+presenceProbeTimeout <= presenceProbeRenewLead; spent += worstInterval {
		attempts++
	}
	if attempts < 2 {
		t.Fatalf("only %d renewal attempt(s) fit inside a %v lead at the worst-case "+
			"%v cadence: one lost probe drops a live contact out of online at "+
			"exactly presenceAliveValidity", attempts, presenceProbeRenewLead, worstInterval)
	}
	if presenceProbeRenewLead >= presenceAliveValidity {
		t.Fatalf("the renew lead (%v) is not shorter than the validity (%v): demand "+
			"mode would never suppress a probe and an active conversation would "+
			"stop being free", presenceProbeRenewLead, presenceAliveValidity)
	}
}

// TestSilenceNeedsTheFrameToHaveLeft is the per-frame half of the strike rule,
// and it names the two ways a cumulative counter got it wrong.
//
// The witness is closed by the netcore writer and by nothing else, so it
// answers about THIS frame:
//
//   - a frame lost anywhere below the class queue — an expired ticket, a
//     session queue discarded on close, a link drained after a failed write —
//     leaves it open. A layer-wide "frames lost after queued" counter never saw
//     those, so three of them could still walk a live contact to `offline`;
//   - an unrelated datagram dying elsewhere does not touch it. With a
//     layer-wide counter one such loss suppressed this probe's timeout, so
//     under sustained backpressure no contact could ever be called absent.
func TestSilenceNeedsTheFrameToHaveLeft(t *testing.T) {
	for name, tc := range map[string]struct {
		witness func() []<-chan struct{}
		strike  bool
	}{
		"never reached the transport": {
			// The class queue refused or dropped it: no witness was ever made.
			witness: func() []<-chan struct{} { return nil },
			strike:  false,
		},
		"reached a writer that did not write it": {
			// Expired ticket, dead link, discarded session queue.
			witness: func() []<-chan struct{} { return []<-chan struct{}{make(chan struct{})} },
			strike:  false,
		},
		"the bytes left the process": {
			witness: func() []<-chan struct{} {
				written := make(chan struct{})
				close(written)
				return []<-chan struct{}{written}
			},
			strike: true,
		},
		"one offer refused, a later one wrote it": {
			// The walk can hand the frame to more than one socket. One witness
			// closing is the frame being on the wire, however many others
			// stayed open.
			witness: func() []<-chan struct{} {
				refused := make(chan struct{})
				written := make(chan struct{})
				close(written)
				return []<-chan struct{}{refused, written}
			},
			strike: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			svc := probeAccountingService(t)
			prober := newPresenceProber(svc)
			target := domaintest.ID("the-contact")
			now := svc.presenceNow()

			label := domaintest.ID("one-attempt")
			prober.mu.Lock()
			prober.attempts[label] = presenceProbeAttempt{
				target: target,
				onWire: tc.witness(),
				sentAt: now.Add(-2 * presenceProbeTimeout),
			}
			prober.mu.Unlock()
			prober.expireStaleAttempts(now)

			strikes := 0
			for _, entry := range svc.presenceProjector.records {
				strikes += entry.missedProbes
			}
			if tc.strike && strikes != 1 {
				t.Fatalf("recorded %d strikes, want 1: this frame demonstrably left the "+
					"process, so the silence that followed is the contact's", strikes)
			}
			if !tc.strike && strikes != 0 {
				t.Fatalf("recorded %d strikes for a frame that never reached a socket: "+
					"three of these turn a live person grey for a failure of ours", strikes)
			}
		})
	}
}

// TestOnlyOurOwnProbesAreWitnessed keeps the witness off the hot path: identity
// resolution sends the SAME dtype and mode and keeps no per-send record, so a
// gate on the frame's shape would put a channel on every lookup in the network.
func TestOnlyOurOwnProbesAreWitnessed(t *testing.T) {
	svc := probeAccountingService(t)
	svc.presenceProber = newPresenceProber(svc)
	mine := domaintest.ID("my-attempt-label")

	unwatched, mayOffer := svc.watchDatagramWrite(protocol.DatagramFrame{Src: mine}).mint()
	if unwatched != nil {
		t.Fatal("a label this node never issued got a write witness")
	}
	if !mayOffer {
		t.Fatal("an UNWATCHED frame was refused the walk: nobody is waiting to hear " +
			"about it, so nothing should stop it being offered")
	}

	svc.presenceProber.mu.Lock()
	svc.presenceProber.attempts[mine] = presenceProbeAttempt{target: domaintest.ID("t")}
	svc.presenceProber.mu.Unlock()

	witness := svc.watchDatagramWrite(protocol.DatagramFrame{Src: mine})
	if ack, _ := witness.mint(); ack == nil {
		t.Fatal("a probe of ours got no witness: its timeout could never tell " +
			"'they did not answer' from 'we never managed to ask'")
	}
	svc.presenceProber.mu.Lock()
	attached := svc.presenceProber.attempts[mine].onWire
	svc.presenceProber.mu.Unlock()
	if len(attached) != 1 {
		t.Fatalf("the attempt collected %d witnesses, want the one that was minted", len(attached))
	}
}

// TestOneChannelPerOfferNotPerWalk is the crash regression.
//
// A queue can ACCEPT an item and then answer a refusal: the gate it reads is
// checked after the offer, so a socket shut in between produces exactly that.
// Its writer nevertheless keeps draining what it already holds and closes the
// ack. The walk meanwhile moves on and a second socket accepts the same frame.
//
// With one channel shared across the walk the second writer's close panicked
// and took the process down. Per-offer channels make two closes land on two
// channels, and the frame counts as written if ANY of them closed.
func TestOneChannelPerOfferNotPerWalk(t *testing.T) {
	svc := probeAccountingService(t)
	svc.presenceProber = newPresenceProber(svc)
	label := domaintest.ID("one-probe")
	svc.presenceProber.mu.Lock()
	svc.presenceProber.attempts[label] = presenceProbeAttempt{target: domaintest.ID("t")}
	svc.presenceProber.mu.Unlock()

	witness := svc.watchDatagramWrite(protocol.DatagramFrame{Src: label})
	first, firstOK := witness.mint()
	second, secondOK := witness.mint()
	if first == nil || second == nil || !firstOK || !secondOK {
		t.Fatal("the walk could not mint a witness per offer")
	}
	if first == second {
		t.Fatal("two offers of one frame share a channel: whichever writers accept " +
			"them both close it, and the second close panics")
	}

	// Both writers really do close theirs, which is the scenario. Neither
	// panics, and the frame is on the wire.
	close(first)
	close(second)

	svc.presenceProber.mu.Lock()
	attached := svc.presenceProber.attempts[label].onWire
	svc.presenceProber.mu.Unlock()
	if len(attached) != 2 {
		t.Fatalf("the attempt collected %d witnesses, want one per offer", len(attached))
	}
	if !probeReachedTheWire(attached) {
		t.Fatal("a frame two writers wrote does not count as having reached the wire")
	}
}

// TestAnExhaustedWitnessStopsTheWalk closes the gap where a probe could reach
// the wire unobserved.
//
// The per-attempt witness slice is bounded, and its bound is NOT an upper bound
// on the candidate walk: one identity can hold several inbound connections per
// IP across several IPs, plus an outbound session. Once the budget is spent the
// walk used to carry on and could still write through the next socket — and a
// probe that really went out but was not watched can never produce a strike, so
// a contact who had genuinely gone would stay `probing` for good.
//
// The rule: a WATCHED frame that can no longer be watched is not offered again.
// An UNWATCHED frame is unaffected — nobody is waiting to hear about it.
func TestAnExhaustedWitnessStopsTheWalk(t *testing.T) {
	svc := probeAccountingService(t)
	svc.presenceProber = newPresenceProber(svc)
	label := domaintest.ID("a-probe")
	svc.presenceProber.mu.Lock()
	svc.presenceProber.attempts[label] = presenceProbeAttempt{target: domaintest.ID("t")}
	svc.presenceProber.mu.Unlock()

	witness := svc.watchDatagramWrite(protocol.DatagramFrame{Src: label})
	for i := 0; i < maxProbeWriteWitnesses; i++ {
		ack, mayOffer := witness.mint()
		if ack == nil || !mayOffer {
			t.Fatalf("offer %d was refused a witness while the budget still had room", i)
		}
	}

	ack, mayOffer := witness.mint()
	if ack != nil {
		t.Fatal("a witness was minted past the budget: the per-attempt slice is unbounded")
	}
	if mayOffer {
		t.Fatal("the walk was allowed to continue without a witness: the frame would " +
			"reach the wire unobserved, its silence could never be attributed, and a " +
			"contact who really left would stay probing for good")
	}
}
