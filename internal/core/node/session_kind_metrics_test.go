package node

import (
	"bufio"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
)

// session_kind_metrics_test.go pins the counts the switch-off of the legacy
// path is decided by: cumulative sessions by kind and direction, the legacy
// sessions whose peer claimed v2, and the live gauge with distinct identities.
// Outbound is counted where the ConnectionManager publishes a session, so the
// tests drive real CM sessions (bootstrap), not the lower-level dial harness.

// waitKinds waits until the node's cumulative session-kind counts satisfy
// pred and returns them.
func waitKinds(t *testing.T, svc *Service, what string, pred func(domain.SessionKindStats) bool) domain.SessionKindStats {
	t.Helper()
	waitForConditionMsg(t, 15*time.Second, what, func() bool {
		return pred(svc.SessionKindStats())
	})
	return svc.SessionKindStats()
}

func TestSessionKindsCountAV2SessionOnBothSides(t *testing.T) {
	t.Parallel()
	listener, address, stopListener := listenerNode(t, nil)
	defer stopListener()
	dialer, stopDialer := dialerNode(t, address, nil)
	defer stopDialer()

	out := waitKinds(t, dialer, "no outbound v2 session counted", func(s domain.SessionKindStats) bool {
		return s.OutboundV2 > 0
	})
	if out.OutboundLegacy != 0 {
		t.Fatalf("dialler counted %d legacy outbound sessions with a v2 peer", out.OutboundLegacy)
	}
	in := waitKinds(t, listener, "no inbound v2 session counted", func(s domain.SessionKindStats) bool {
		return s.InboundV2 > 0
	})
	if in.InboundLegacy != 0 {
		t.Fatalf("listener counted %d legacy inbound sessions with a v2 peer", in.InboundLegacy)
	}
	if out.LegacyAtClaimedV2 != 0 || in.LegacyAtClaimedV2 != 0 {
		t.Fatalf("a v2 session was counted as legacy-at-claimed-v2: dialler %d, listener %d", out.LegacyAtClaimedV2, in.LegacyAtClaimedV2)
	}
}

// A node of this build and an emulated old peer (version rewritten below 31)
// count a legacy session each way, with the old version seen as pre-v2 and
// never as legacy-at-claimed-v2.
func TestSessionKindsCountLegacyWithAnOldPeerBothWays(t *testing.T) {
	t.Parallel()
	for _, version := range releasedLegacyVersions() {
		version := version
		t.Run(versionName(version), func(t *testing.T) {
			t.Parallel()
			pair := startLegacyPair(t, version)

			out := waitKinds(t, pair.new, "no legacy outbound session with the old peer", func(s domain.SessionKindStats) bool {
				return s.OutboundLegacy > 0 && s.InboundLegacy > 0
			})
			if out.OutboundV2 != 0 || out.InboundV2 != 0 {
				t.Fatalf("v%d: a v2 session was counted with a v1-only peer: %+v", version, out)
			}
			if out.WelcomesBelowSecureVersion == 0 || out.HellosBelowSecureVersion == 0 {
				t.Fatalf("v%d: the old peer's announced version was not seen as pre-v2: %+v", version, out)
			}
			if out.LegacyAtClaimedV2 != 0 {
				t.Fatalf("v%d: a pre-v2 legacy session was counted as legacy-at-claimed-v2: %d", version, out.LegacyAtClaimedV2)
			}
		})
	}
}

// A legacy session with a peer that announces this build's version — reached
// over v1 but claiming v2 (a stripped ALPN, or a man in the middle) — is
// counted as legacy-at-claimed-v2, not as an old peer below the v2 version.
// These are the sessions raising the minimum would cut off despite the claim.
func TestSessionKindsCountALegacyPeerClaimingV2(t *testing.T) {
	t.Parallel()
	_, address, stopOld := listenerNode(t, withoutOwnDials(asOldNode))
	defer stopOld()
	dialer, stopDialer := dialerNode(t, address, nil)
	defer stopDialer()

	out := waitKinds(t, dialer, "no legacy session with the v2-claiming old node", func(s domain.SessionKindStats) bool {
		return s.OutboundLegacy > 0
	})
	if out.OutboundV2 != 0 {
		t.Fatalf("a v2 session was counted with a v1-only node: %+v", out)
	}
	if out.WelcomesBelowSecureVersion != 0 {
		t.Fatalf("a peer announcing this build's version was counted as pre-v2: %d", out.WelcomesBelowSecureVersion)
	}
	if out.LegacyAtClaimedV2 == 0 {
		t.Fatal("a legacy session with a peer claiming v2 was not counted as legacy-at-claimed-v2")
	}
}

// The cumulative counters are not reset by reading: a second read never shows
// fewer than the first, and the period starts with the node and moves forward.
func TestSessionKindsAreNotResetByReading(t *testing.T) {
	t.Parallel()
	_, address, stopListener := listenerNode(t, nil)
	defer stopListener()
	dialer, stopDialer := dialerNode(t, address, nil)
	defer stopDialer()

	waitKinds(t, dialer, "no outbound v2 session counted", func(s domain.SessionKindStats) bool {
		return s.OutboundV2 > 0
	})
	first := dialer.SessionKindStats()
	second := dialer.SessionKindStats()
	if second.OutboundV2 < first.OutboundV2 {
		t.Fatalf("reading lowered the count: %d then %d", first.OutboundV2, second.OutboundV2)
	}
	if !second.StartedAt.Equal(dialer.startedAt) || second.ReadAt.Before(first.ReadAt) {
		t.Fatalf("period %v..%v after %v..%v, want it to start with the node and move forward", second.StartedAt, second.ReadAt, first.StartedAt, first.ReadAt)
	}
}

// The live gauge counts the sessions live now and the distinct identities
// behind them: for one v2 peer, one live v2 session and one identity each
// way, and no legacy. The gauge resists the inflation the cumulative counts
// allow — a second session to the same identity adds a session, never an
// identity.
func TestLiveSessionKindsGaugeCountsDistinctIdentities(t *testing.T) {
	t.Parallel()
	listener, address, stopListener := listenerNode(t, nil)
	defer stopListener()
	dialer, stopDialer := dialerNode(t, address, nil)
	defer stopDialer()

	waitForConditionMsg(t, 15*time.Second, "no v2 session between the two nodes", func() bool {
		v2, _ := authenticatedInbound(listener)
		return v2 > 0
	})
	var dialled *peerSession
	waitForConditionMsg(t, 15*time.Second, "the dialler never published its v2 session", func() bool {
		dialled = publishedProvenSession(dialer)
		return dialled != nil
	})

	dialer.refreshLiveSessionKinds()
	listener.refreshLiveSessionKinds()

	dialerLive := dialer.SessionKindStats().Live
	if !dialerLive.Ready {
		t.Fatal("the dialler's live gauge never became ready")
	}
	assertLiveBucket(t, "dialler outbound v2", dialerLive.OutboundV2)
	if dialerLive.OutboundLegacy.Sessions != 0 {
		t.Fatalf("the dialler holds %d live legacy outbound sessions with a v2 peer", dialerLive.OutboundLegacy.Sessions)
	}

	listenerLive := listener.SessionKindStats().Live
	assertLiveBucket(t, "listener inbound v2", listenerLive.InboundV2)
	if listenerLive.InboundLegacy.Sessions != 0 {
		t.Fatalf("the listener holds %d live legacy inbound sessions with a v2 peer", listenerLive.InboundLegacy.Sessions)
	}

	// A second session proven for the same identity, at another address: the
	// shape of a peer that opens more sessions to run the counts up.
	second := domain.PeerAddress("198.51.100.77:64646")
	registerSessionForGauge(dialer, &peerSession{address: second, peerIdentity: dialled.peerIdentity, proven: dialled.proven})
	defer unregisterSessionForGauge(dialer, second)

	dialer.refreshLiveSessionKinds()
	twice := dialer.SessionKindStats().Live.OutboundV2
	if twice.Sessions != dialerLive.OutboundV2.Sessions+1 {
		t.Fatalf("live outbound v2 sessions = %d after a second session, want %d", twice.Sessions, dialerLive.OutboundV2.Sessions+1)
	}
	if twice.Identities != dialerLive.OutboundV2.Identities || twice.Identities >= twice.Sessions {
		t.Fatalf("live outbound v2 identities = %d over %d sessions, want %d: a second session to one identity added an identity",
			twice.Identities, twice.Sessions, dialerLive.OutboundV2.Identities)
	}
}

// publishedProvenSession is a session svc published whose identity v2 proved,
// or nil while there is none.
func publishedProvenSession(svc *Service) *peerSession {
	svc.peerMu.RLock()
	defer svc.peerMu.RUnlock()
	for _, session := range svc.sessions {
		if _, proven := session.provenIdentity(); proven {
			return session
		}
	}
	return nil
}

// registerSessionForGauge enters a session the gauge walks; it carries no
// transport and must be unregistered before the node stops.
func registerSessionForGauge(svc *Service, session *peerSession) {
	svc.peerMu.Lock()
	svc.deliveryMu.Lock()
	svc.sessions[session.address] = session
	svc.deliveryMu.Unlock()
	svc.peerMu.Unlock()
}

func unregisterSessionForGauge(svc *Service, address domain.PeerAddress) {
	svc.peerMu.Lock()
	svc.deliveryMu.Lock()
	delete(svc.sessions, address)
	svc.deliveryMu.Unlock()
	svc.peerMu.Unlock()
}

// assertLiveBucket checks a non-empty bucket: at least one session, at least
// one identity, and never more identities than sessions (the bound that makes
// the gauge game-resistant).
func assertLiveBucket(t *testing.T, what string, bucket domain.LiveSessionKindCount) {
	t.Helper()
	if bucket.Sessions < 1 {
		t.Fatalf("%s: %d live sessions, want at least 1", what, bucket.Sessions)
	}
	if bucket.Identities < 1 {
		t.Fatalf("%s: %d distinct identities, want at least 1", what, bucket.Identities)
	}
	if bucket.Identities > bucket.Sessions {
		t.Fatalf("%s: %d identities over %d sessions — identities must not exceed sessions", what, bucket.Identities, bucket.Sessions)
	}
}

// assertNoOutboundSessionCounted fails when any outbound kind count moved:
// the session under test was never published, so it must not be in any of
// them.
func assertNoOutboundSessionCounted(t *testing.T, svc *Service, why string) {
	t.Helper()
	got := svc.SessionKindStats()
	if got.OutboundV2 != 0 || got.OutboundLegacy != 0 || got.WelcomesBelowSecureVersion != 0 || got.LegacyAtClaimedV2 != 0 {
		t.Fatalf("%s, yet it was counted: outbound v2 %d, outbound legacy %d, welcomes below v2 %d, legacy at claimed v2 %d",
			why, got.OutboundV2, got.OutboundLegacy, got.WelcomesBelowSecureVersion, got.LegacyAtClaimedV2)
	}
}

// answerSetupOnceReleased plays a peer that answers every setup request of
// initPeerSession, but only once release is closed, so the test can change
// what the node knows while the setup waits. done ends it early.
func answerSetupOnceReleased(release, done <-chan struct{}) farEndFunc {
	replies := map[string]string{"get_peers": "peers", "fetch_contacts": "contacts"}
	return func(remote net.Conn, inbox chan<- protocol.Frame, errs chan<- error) {
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
			replyType, ok := replies[request.Type]
			if !ok {
				continue
			}
			select {
			case <-release:
			case <-done:
				return
			}
			select {
			case inbox <- protocol.Frame{Type: replyType}:
			case <-done:
				return
			}
		}
	}
}

// dropOnFirstSetupRequest plays a peer that hangs up as soon as the first
// setup request arrives, so initPeerSession fails on the peer's account.
func dropOnFirstSetupRequest(remote net.Conn, _ chan<- protocol.Frame, errs chan<- error) {
	_, _ = bufio.NewReader(remote).ReadString('\n')
	_ = remote.Close()
	select {
	case errs <- io.ErrUnexpectedEOF:
	default:
	}
}

// A dialled legacy session whose identity proves v2 while its setup runs is
// refused when it is about to be published and withdrawn. It never served
// the peer, so it is not a legacy session of this node: counting it would
// report a session the pin had already closed as legacy traffic.
func TestSessionKindsSkipADialledSessionRefusedBeforePublication(t *testing.T) {
	release := make(chan struct{})
	farEndDone := make(chan struct{})
	rig := newEvictionRig(t, evictionRigOptions{farEnd: answerSetupOnceReleased(release, farEndDone)})
	t.Cleanup(func() { close(farEndDone) })

	if err := rig.svc.secureSessions.store.noteProvenInbound(rig.session.peerIdentity); err != nil {
		t.Fatalf("pin: %v", err)
	}
	close(release)
	rig.awaitSessionGoroutine(t)

	if got := rig.svc.SecureSessionStoreStats().V1RefusalsIdentityPinned; got != 1 {
		t.Fatalf("precondition: identity refusals = %d, want 1 — the setup did not reach the pin check before publication", got)
	}
	assertNoOutboundSessionCounted(t, rig.svc, "the dialled session was refused by the pin before publication")
}

// A dialled session whose application-level setup fails on the peer's account
// is reported to the ConnectionManager as a failed setup and never published.
func TestSessionKindsSkipADialledSessionWhoseSetupFailed(t *testing.T) {
	rig := newEvictionRig(t, evictionRigOptions{farEnd: dropOnFirstSetupRequest})
	rig.awaitSessionGoroutine(t)

	rig.svc.peerMu.RLock()
	consecutive := 0
	if entry := rig.svc.setupFailures[evictionSlotAddress]; entry != nil {
		consecutive = entry.Consecutive
	}
	rig.svc.peerMu.RUnlock()
	if consecutive != evictionSeededSetupFailures+1 {
		t.Fatalf("precondition: setup failures = %d, want %d — the setup did not fail on the peer's account", consecutive, evictionSeededSetupFailures+1)
	}
	assertNoOutboundSessionCounted(t, rig.svc, "the dialled session's setup failed")
}

// A dialled session loses the ConnectionManager's generation race after it
// was established: its slot is evicted while the setup runs, and the setup's
// last reply still arrives. The session is aborted, not published, and is not
// counted either.
func TestSessionKindsSkipADialledSessionWhoseSlotWasEvictedDuringSetup(t *testing.T) {
	farEndDone := make(chan struct{})
	farEnd := &replyAfterEvictionFarEnd{
		lastRequestSeen: make(chan struct{}),
		evicted:         make(chan struct{}),
		lastReplyTaken:  make(chan struct{}),
		done:            farEndDone,
	}
	rig := newEvictionRig(t, evictionRigOptions{farEnd: farEnd.play})
	t.Cleanup(func() { close(farEndDone) })

	awaitClosed(t, farEnd.lastRequestSeen, "initPeerSession to send fetch_contacts")
	rig.cm.RetainOnly(context.Background(), evictionPinAddress)
	rig.assertSlotEvicted(t)
	close(farEnd.evicted)
	rig.awaitSessionGoroutine(t)
	awaitClosed(t, farEnd.lastReplyTaken, "initPeerSession to take its last reply")

	assertNoOutboundSessionCounted(t, rig.svc, "the dialled session's slot was evicted before publication")
}
