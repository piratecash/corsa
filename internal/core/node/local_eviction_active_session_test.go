package node

import (
	"bufio"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// local_eviction_active_session_test.go extends local_eviction_session_test.go
// past setup.
//
//   - A session that is already served when the ConnectionManager evicts its
//     slot on a local decision: the serve loop sees a read error on the socket
//     the CM closed, and the peer is published as disconnected but not charged
//     — no failed disconnect on its health record, no disconnect_storm
//     evidence — while its routing registration is undone exactly once.
//   - The opposite guard: a genuine peer failure closes the session first, and
//     a CM eviction that follows does not excuse it.
//   - A session whose setup succeeded but which the CM evicts before it is
//     registered: it is never published.

// answeringFarEnd plays a peer that answers every setup request at once, so
// initPeerSession succeeds and the session reaches its serve loop. A close of
// hangUp, when given, makes it drop the connection, as a peer that goes away
// does.
func answeringFarEnd(hangUp <-chan struct{}) farEndFunc {
	return func(remote net.Conn, inbox chan<- protocol.Frame, errs chan<- error) {
		if hangUp != nil {
			go func() {
				<-hangUp
				_ = remote.Close()
			}()
		}
		replies := map[string]string{"get_peers": "peers", "fetch_contacts": "contacts"}
		reader := bufio.NewReader(remote)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				select {
				case errs <- err:
				default:
				}
				return
			}
			request, err := protocol.ParseFrameLine(line)
			if err != nil {
				continue
			}
			if replyType, ok := replies[request.Type]; ok {
				inbox <- protocol.Frame{Type: replyType}
			}
		}
	}
}

// healthCharge is the part of a peer's health record a disconnect charges.
type healthCharge struct {
	consecutiveFailures int
	score               int
	lastError           string
}

func (rig *evictionRig) healthOf(t *testing.T) (healthCharge, bool) {
	t.Helper()
	rig.svc.peerMu.RLock()
	defer rig.svc.peerMu.RUnlock()
	health := rig.svc.health[rig.svc.resolveHealthAddress(evictionSlotAddress)]
	if health == nil {
		t.Fatalf("no health record for %s", evictionSlotAddress)
	}
	return healthCharge{
		consecutiveFailures: health.ConsecutiveFailures,
		score:               health.Score,
		lastError:           health.LastError,
	}, health.Connected
}

// sessionCounters is the routing registration of the rig's identity.
type sessionCounters struct {
	total, relay int
	stormHistory int
	registered   bool
}

func (rig *evictionRig) counters() sessionCounters {
	rig.svc.peerMu.RLock()
	defer rig.svc.peerMu.RUnlock()
	_, registered := rig.svc.sessions[evictionSlotAddress]
	return sessionCounters{
		total:        rig.svc.identitySessions[rig.session.peerIdentity],
		relay:        rig.svc.identityRelaySessions[rig.session.peerIdentity],
		stormHistory: len(rig.svc.peerDisconnectHistory[rig.session.penaltySubject()]),
		registered:   registered,
	}
}

// newServedRig returns a rig whose session is relay-capable — so its close
// reaches the disconnect_storm accounting, which counts only the last relay
// session — and is being served. A second, non-relay session for the same
// identity is registered alongside, the way an inbound connection would be:
// it makes an over-deregistration visible (the identity's total would drop to
// zero) while leaving the served session the last relay one.
func newServedRig(t *testing.T, hangUp <-chan struct{}) *evictionRig {
	t.Helper()
	rig := newEvictionRig(t, evictionRigOptions{
		farEnd: answeringFarEnd(hangUp),
		beforeEstablished: func(info SessionInfo) {
			info.Session.capabilities = []domain.Capability{domain.CapMeshRelayV1}
		},
	})
	waitFor(t, 2*time.Second, "the session to be served and its peer marked connected", func() bool {
		rig.svc.peerMu.RLock()
		health := rig.svc.health[rig.svc.resolveHealthAddress(evictionSlotAddress)]
		connected := health != nil && health.Connected
		rig.svc.peerMu.RUnlock()
		return connected && rig.counters().relay == 1
	})
	rig.svc.onPeerSessionEstablished(rig.session.peerIdentity, nil)
	t.Cleanup(func() { rig.svc.onPeerSessionClosed(rig.session.peerIdentity, rig.session.penaltySubject(), nil) })
	if got := rig.counters(); got.total != 2 || got.relay != 1 {
		t.Fatalf("precondition: identity sessions = %+v, want 2 total / 1 relay", got)
	}
	return rig
}

// assertServedSessionUnwoundOnce checks that the served session's routing
// registration was undone exactly once: the other session of the identity
// still counts, the served one no longer does.
func (rig *evictionRig) assertServedSessionUnwoundOnce(t *testing.T) sessionCounters {
	t.Helper()
	got := rig.counters()
	if got.total != 1 || got.relay != 0 {
		t.Errorf("identity sessions after the served session ended = %d total / %d relay, want 1 / 0: "+
			"its routing registration was undone %s", got.total, got.relay, unwoundTimes(got.total))
	}
	if got.registered {
		t.Errorf("%s is still registered in s.sessions after its session ended", evictionSlotAddress)
	}
	return got
}

func unwoundTimes(total int) string {
	switch {
	case total > 1:
		return "never"
	case total < 1:
		return "more than once"
	default:
		return "once"
	}
}

func TestLocalEvictionOfAServedSession_DoesNotChargeThePeer(t *testing.T) {
	cases := []struct {
		name  string
		evict func(t *testing.T, rig *evictionRig)
	}{
		{name: "RetainOnly", evict: func(t *testing.T, rig *evictionRig) {
			if !rig.cm.RetainOnly(context.Background(), evictionPinAddress) {
				t.Fatal("RetainOnly = false on a running event loop")
			}
		}},
		{name: "shrinkToLimit", evict: func(_ *testing.T, rig *evictionRig) {
			rig.maxSlots.Store(0)
			rig.cm.EmitHint(NewPeersDiscovered{Count: 1})
		}},
		{name: "handleManualPeer eviction", evict: func(t *testing.T, rig *evictionRig) {
			if !rig.cm.EmitSlot(ManualPeerRequested{Address: evictionOtherAddress}) {
				t.Fatal("the manager refused the manual peer request")
			}
		}},
		{name: "ConnectionManager shutdown", evict: func(_ *testing.T, rig *evictionRig) {
			rig.cancelCM()
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newServedRig(t, nil)
			before, _ := rig.healthOf(t)

			tc.evict(t, rig)
			rig.awaitSessionGoroutine(t)
			rig.assertSlotEvicted(t)

			after, connected := rig.healthOf(t)
			if connected {
				t.Errorf("%s is still published as connected after its session was evicted", evictionSlotAddress)
			}
			if after != before {
				t.Errorf("health charge after a local %s = %+v, want it unchanged from %+v", tc.name, after, before)
			}
			if got := rig.assertServedSessionUnwoundOnce(t); got.stormHistory != 0 {
				t.Errorf("disconnect_storm history against %s = %d entries after a local %s, want none",
					evictionSlotAddress, got.stormHistory, tc.name)
			}
		})
	}
}

// The peer hangs up first; its session's owner closes the session and only
// then does the CM evict the slot. The first close was the owner's, so the
// eviction that follows must not turn a genuine failure into a local decision.
//
// peerTeardownBarrier runs in retirePeerSession after the owner's close and
// before the disconnect is charged — the latest point at which an eviction can
// still land before the charge is decided.
func TestPeerFailureFollowedByALocalEviction_IsStillCharged(t *testing.T) {
	hangUp := make(chan struct{})
	rig := newServedRig(t, hangUp)
	before, _ := rig.healthOf(t)

	var evictOnce sync.Once
	rig.svc.peerTeardownBarrier = func() {
		evictOnce.Do(func() {
			if !rig.cm.RetainOnly(context.Background(), evictionPinAddress) {
				t.Error("RetainOnly = false on a running event loop")
			}
		})
	}
	close(hangUp)
	rig.awaitSessionGoroutine(t)
	rig.assertSlotEvicted(t)

	after, connected := rig.healthOf(t)
	if connected {
		t.Errorf("%s is still published as connected after its peer hung up", evictionSlotAddress)
	}
	if after.consecutiveFailures != before.consecutiveFailures+1 {
		t.Errorf("consecutive failures after the peer hung up = %d, want %d: the later eviction excused a genuine failure",
			after.consecutiveFailures, before.consecutiveFailures+1)
	}
	if got := rig.assertServedSessionUnwoundOnce(t); got.stormHistory != 1 {
		t.Errorf("disconnect_storm history against %s = %d entries, want the one for the peer's hang-up",
			evictionSlotAddress, got.stormHistory)
	}
}

// Setup succeeded, and the CM evicts the slot before the session is
// registered: the registration must see the eviction and publish nothing —
// not in s.sessions, not marked connected, not counted as an identity session,
// the setup-failure counter neither moved nor cleared — and the session's
// control-frame bucket must not outlive it.
func TestEvictionBetweenSetupAndRegistration_IsNotPublished(t *testing.T) {
	gate := make(chan struct{})
	reached := make(chan struct{})
	var releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(gate) }) }

	// The barrier is installed on the event loop before the session goroutine
	// starts, so the goroutine is ordered after it. The loop waits for the
	// rig to exist first: the dial runs while newEvictionRig is still
	// returning.
	var rig *evictionRig
	rigBuilt := make(chan struct{})
	rig = newEvictionRig(t, evictionRigOptions{
		farEnd: answeringFarEnd(nil),
		beforeEstablished: func(SessionInfo) {
			<-rigBuilt
			rig.svc.cmdLimiter.allowCommand(outboundControlFrameLimitKey(rig.session.connID))
			rig.svc.cmSessionRegisterBarrier = func() {
				close(reached)
				<-gate
			}
		},
	})
	close(rigBuilt)
	t.Cleanup(releaseGate)
	bucketKey := outboundControlFrameLimitKey(rig.session.connID)

	awaitClosed(t, reached, "the session goroutine to reach registration")
	if !rig.cm.RetainOnly(context.Background(), evictionPinAddress) {
		t.Fatal("RetainOnly = false on a running event loop")
	}
	releaseGate()
	rig.awaitSessionGoroutine(t)
	rig.assertSlotEvicted(t)

	if got := rig.counters(); got.registered || got.total != 0 {
		t.Errorf("a session the CM evicted before registration was published: registered = %v, identity sessions = %d",
			got.registered, got.total)
	}
	rig.svc.peerMu.RLock()
	health := rig.svc.health[rig.svc.resolveHealthAddress(evictionSlotAddress)]
	consecutive := 0
	if entry := rig.svc.setupFailures[evictionSlotAddress]; entry != nil {
		consecutive = entry.Consecutive
	}
	rig.svc.peerMu.RUnlock()
	if health != nil && !health.LastConnectedAt.IsZero() {
		t.Errorf("%s was marked connected by a session the CM evicted before registration", evictionSlotAddress)
	}
	if consecutive != evictionSeededSetupFailures {
		t.Errorf("setup failures = %d, want the seeded %d untouched: an evicted session proves nothing either way",
			consecutive, evictionSeededSetupFailures)
	}
	rig.svc.cmdLimiter.mu.Lock()
	_, bucketLeft := rig.svc.cmdLimiter.buckets[bucketKey]
	rig.svc.cmdLimiter.mu.Unlock()
	if bucketLeft {
		t.Error("the control-frame bucket of a session that was never published outlived it")
	}
}
