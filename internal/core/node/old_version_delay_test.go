package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
)

// old_version_delay_test.go pins the old-version delay, which keeps a node
// that really is old from being redialled every few seconds: an endpoint whose
// CORRECT answer — the probe's, a v1 welcome — claims a version below this
// node's minimum is delayed on its own, 30 s doubling to 6 h. Nothing else
// is touched: no ban of the IP or the identity, no score, no delay of other
// addresses. A timeout, a broken answer, a TLS error or a full pin store is
// not this reason. Only a successful outbound connection to the same
// endpoint — over a protocol it may use, the pin respected — resets it; an
// inbound session of the same identity does not. The operator may retry one
// endpoint by hand, which lifts neither a pin nor the ban of a downgrade.
// The claim proves nothing — a forged one can hold the endpoint off for up
// to 6 h, the accepted price.

const oldVersionEndpoint = domain.PeerAddress("198.51.100.40:64646")

// oldReleaseBelowFloor is an old release's correct answer whose version is
// below this build's minimum.
func oldReleaseBelowFloor(t *testing.T) protocol.Frame {
	t.Helper()
	return oldReleaseWelcome(t, generateProbeIdentity(t), config.MinimumProtocolVersion-1)
}

// The delay grows per endpoint — 30 s doubling, at most 6 h — and one step
// per attempt; another endpoint is not held by it.
func TestTheOldVersionDelayGrowsToSixHoursPerEndpoint(t *testing.T) {
	t.Parallel()
	clock := newStoreClock()
	marks := newSessionAddressMarks(clock.read)
	other := domain.PeerAddress("198.51.100.41:64646")
	want := 30 * time.Second
	for step := 1; step <= 12; step++ {
		got := marks.noteClaimedOldVersion(oldVersionEndpoint, config.MinimumProtocolVersion-1)
		if got != want {
			t.Fatalf("step %d: wait %v, want %v", step, got, want)
		}
		if again := marks.noteClaimedOldVersion(oldVersionEndpoint, config.MinimumProtocolVersion-1); again != got {
			t.Fatalf("step %d: a second end inside the wait stepped it to %v", step, again)
		}
		if marks.oldVersionHeld(other) {
			t.Fatal("another endpoint is held by this one's delay")
		}
		clock.now = clock.now.Add(got - time.Second)
		if !marks.oldVersionHeld(oldVersionEndpoint) {
			t.Fatalf("step %d: the delay ended a second early", step)
		}
		clock.now = clock.now.Add(time.Second)
		want = min(2*want, oldVersionRetryMax)
	}
	if oldVersionRetryBase != 30*time.Second || oldVersionRetryMax != 6*time.Hour {
		t.Fatalf("delay = %v…%v, want 30s…6h", oldVersionRetryBase, oldVersionRetryMax)
	}
}

// A dialler that tries every ten seconds for a day reaches an old release
// below the floor a bounded number of times: the delay caps the redials, not
// the connection manager's luck.
func TestAnOldReleaseBelowTheFloorIsDialledBoundedTimesADay(t *testing.T) {
	t.Parallel()
	svc := newDialChoiceService(t)
	clock := newStoreClock()
	svc.secureSessions.marks = newSessionAddressMarks(clock.read)
	answer := oldReleaseBelowFloor(t)
	sockets := 0
	for elapsed := time.Duration(0); elapsed < 24*time.Hour; elapsed += 10 * time.Second {
		clock.now = clock.now.Add(10 * time.Second)
		peer := newScriptedPeer(answerClientHello(v1ErrorLine), oldNodeSocket(answer, answer))
		_, err := dialThrough(t, svc, oldVersionEndpoint, domain.DialToAddress(), peer)
		sockets += len(peer.sockets(t))
		svc.onCMDialFailed(oldVersionEndpoint, err, errors.Is(err, errIncompatibleProtocol))
	}
	// 13 attempts of two sockets each: 30 s doubling up to 6 h.
	if sockets > 30 || sockets < 20 {
		t.Fatalf("%d sockets in a day, want a bounded handful (20–30)", sockets)
	}
	assertNoVersionHarm(t, svc, oldVersionEndpoint)
}

// Not this reason: a probe nobody answers, a broken answer below the floor,
// a TLS failure, a full pin store. None of them starts the delay.
func TestOnlyACorrectOldAnswerStartsTheOldVersionDelay(t *testing.T) {
	t.Parallel()
	for name, scripts := range map[string][]socketScript{
		"a probe timeout":          {answerClientHello(v1ErrorLine), silentPeer},
		"a broken answer":          {answerClientHello(v1ErrorLine), oldNodeSocket(forgedLowVersionAnswer(), forgedLowVersionAnswer())},
		"a TLS failure":            {answerClientHello(tlsHandshakeGarbage)},
		"a TLS alert":              {answerClientHello(sessionv2.RefusalAlert())},
		"a v1 line, then an error": {answerClientHello(v1ErrorLine), answerClientHello(v1ErrorLine)},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			svc := newDialChoiceService(t)
			peer := newScriptedPeer(scripts...)
			_, err := dialThrough(t, svc, oldVersionEndpoint, domain.DialToAddress(), peer)
			svc.onCMDialFailed(oldVersionEndpoint, err, errors.Is(err, errIncompatibleProtocol))
			if svc.secureSessions.marks.oldVersionHeld(oldVersionEndpoint) {
				t.Fatalf("%s started the old-version delay", name)
			}
		})
	}
	t.Run("a full pin store", func(t *testing.T) {
		t.Parallel()
		svc := newDialChoiceService(t)
		svc.onCMDialFailed(oldVersionEndpoint, errPinStoreFull, false)
		if svc.secureSessions.marks.oldVersionHeld(oldVersionEndpoint) {
			t.Fatal("pin_store_full started the old-version delay")
		}
	})
}

// A successful outbound connection to the endpoint resets the delay; the
// count starts over.
func TestAnOutboundSuccessResetsTheOldVersionDelay(t *testing.T) {
	t.Parallel()
	svc := newDialChoiceService(t)
	marks := svc.secureSessions.marks
	marks.noteClaimedOldVersion(oldVersionEndpoint, config.MinimumProtocolVersion-1)
	marks.clearOldVersionWait(oldVersionEndpoint)
	marks.noteClaimedOldVersion(oldVersionEndpoint, config.MinimumProtocolVersion-1)
	svc.recordOutboundAuthSuccess(oldVersionEndpoint, string(oldVersionEndpoint))
	if marks.oldVersionHeld(oldVersionEndpoint) {
		t.Fatal("an outbound success left the delay in place")
	}
	if got := marks.noteClaimedOldVersion(oldVersionEndpoint, config.MinimumProtocolVersion-1); got != oldVersionRetryBase {
		t.Fatalf("the next delay = %v, want the first step", got)
	}
}

// A node that fell back to v1-only (its identity or certificate source was
// unusable) is held by the delay all the same: the dial is refused before
// any socket, and the address is not a candidate meanwhile.
func TestTheOldVersionDelayHoldsAV1OnlyNodeToo(t *testing.T) {
	t.Parallel()
	const endpoint = "1.2.3.4:64646"
	svc := newServiceWithBootstrapPeers(t, publicCandidatesOnly, endpoint)
	asOldNode(svc)
	refusal := incompatibleVersionOf(oldReleaseBelowFloor(t), versionClaimed, errors.New("below the floor"))
	svc.onCMDialFailed(endpoint, refusal, true)
	if !svc.dialHeld(endpoint) {
		t.Fatal("a v1-only node does not hold an endpoint that claimed an old version")
	}
	if candidatesContain(svc, endpoint) {
		t.Fatal("the delayed endpoint stayed a candidate on a v1-only node")
	}
	peer := newScriptedPeer(answerClientHello(v1ErrorLine))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := svc.dialPeerTransport(ctx, endpoint, domain.DialToAddress(), peer.dialer()); !errors.Is(err, errOldVersionAwaited) {
		t.Fatalf("err = %v, want errOldVersionAwaited", err)
	}
	if sockets := peer.sockets(t); len(sockets) != 0 {
		t.Fatalf("%d sockets opened inside the delay, want none", len(sockets))
	}
}

// The operator's diagnostics name the reason, the claimed version and the
// next attempt; a retry by hand lifts the delay of that endpoint alone and
// nothing else — not a pin, not a binding — so a downgrade stays refused.
func TestTheOperatorSeesAndRetriesAnOldVersionDelay(t *testing.T) {
	t.Parallel()
	svc := newDialChoiceService(t)
	clock := newStoreClock()
	svc.secureSessions.marks = newSessionAddressMarks(clock.read)
	other := domain.PeerAddress("198.51.100.41:64646")
	claimed := domain.ProtocolVersion(config.MinimumProtocolVersion - 1)
	svc.secureSessions.marks.noteClaimedOldVersion(oldVersionEndpoint, claimed)
	svc.secureSessions.marks.noteClaimedOldVersion(other, claimed)
	if _, err := svc.secureSessions.store.noteProvenOutbound(dialledAt(oldVersionEndpoint), domain.PeerIdentity{7}, endpointProtectionNone()); err != nil {
		t.Fatalf("bind: %v", err)
	}

	delays, err := svc.ListEndpointDialDelays(context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	found := false
	for _, delay := range delays {
		if delay.Endpoint != oldVersionEndpoint {
			continue
		}
		found = true
		if delay.Reason != domain.EndpointDialDelayClaimedOldVersion || delay.ClaimedVersion != claimed || !delay.NextAttemptAt.Equal(clock.now.Add(oldVersionRetryBase)) {
			t.Fatalf("delay = %+v, want the claimed old version and its next attempt", delay)
		}
	}
	if !found {
		t.Fatalf("delays = %+v, want %s listed", delays, oldVersionEndpoint)
	}

	if err := svc.RetryEndpointDial(context.Background(), oldVersionEndpoint); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if svc.secureSessions.marks.oldVersionHeld(oldVersionEndpoint) {
		t.Fatal("the retry left the dial held")
	}
	if !svc.secureSessions.marks.oldVersionHeld(other) {
		t.Fatal("the retry of one endpoint lifted another's delay")
	}
	if !svc.secureSessions.store.endpointRequiresV2(oldVersionEndpoint) {
		t.Fatal("the retry lifted the endpoint's binding")
	}
	peer := newScriptedPeer(answerClientHello(v1ErrorLine))
	if _, err := dialThrough(t, svc, oldVersionEndpoint, domain.DialToAddress(), peer); !errors.Is(err, errLegacyRefusedV2Bound) {
		t.Fatalf("the dial after the retry: %v, want the downgrade still refused", err)
	}
	if err := svc.RetryEndpointDial(context.Background(), "198.51.100.42:64646"); !errors.Is(err, domain.ErrEndpointDialDelayNotFound) {
		t.Fatalf("a retry of an endpoint without a delay: %v, want ErrEndpointDialDelayNotFound", err)
	}
}

// A retry by hand is one attempt, not a reset: the next dial may go at once,
// but if the peer still claims an old version, the delay that follows grows
// on from where it was — only an outbound success starts it over.
func TestARetryByHandKeepsTheOldVersionCount(t *testing.T) {
	t.Parallel()
	svc := newDialChoiceService(t)
	clock := newStoreClock()
	svc.secureSessions.marks = newSessionAddressMarks(clock.read)
	marks := svc.secureSessions.marks
	claimed := domain.ProtocolVersion(config.MinimumProtocolVersion - 1)
	marks.noteClaimedOldVersion(oldVersionEndpoint, claimed)
	clock.now = clock.now.Add(oldVersionRetryBase)
	marks.noteClaimedOldVersion(oldVersionEndpoint, claimed)
	if err := svc.RetryEndpointDial(context.Background(), oldVersionEndpoint); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if marks.oldVersionHeld(oldVersionEndpoint) {
		t.Fatal("the retry left the dial held")
	}
	if got := marks.noteClaimedOldVersion(oldVersionEndpoint, claimed); got != 4*oldVersionRetryBase {
		t.Fatalf("the delay after a retry to a still-old peer = %v, want %v: the retry reset the count", got, 4*oldVersionRetryBase)
	}
}

// Only a correct answer starts the delay: a welcome whose identity binds but
// whose version is missing, or that names another network, is no claim of an
// old version — on a v1-only node too, where no probe validated it first.
func TestOnlyACorrectWelcomeClaimsAnOldVersion(t *testing.T) {
	t.Parallel()
	for name, broken := range map[string]func(*protocol.Frame){
		"no version":      func(f *protocol.Frame) { f.Version = 0 },
		"another network": func(f *protocol.Frame) { f.Network = "another-network" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			svc := newDialChoiceService(t)
			asOldNode(svc)
			welcome := oldReleaseBelowFloor(t)
			welcome.Challenge = "a challenge"
			broken(&welcome)
			svc.onCMDialFailed(oldVersionEndpoint, incompatibleVersionOf(welcome, versionClaimed, errors.New("refused")), true)
			if svc.secureSessions.marks.oldVersionHeld(oldVersionEndpoint) {
				t.Fatalf("a welcome with %s started the old-version delay", name)
			}
		})
	}
}
