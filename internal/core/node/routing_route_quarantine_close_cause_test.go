package node

import (
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/domain/domaintest"
)

// ---------------------------------------------------------------------------
// sessionCloseCause tests: only PEER-initiated teardowns may feed the
// disconnect_storm quarantine window. Teardowns the local node chose
// itself (inbox-overflow slow-consumer eviction, CM slot replacement)
// say nothing about the peer's stability; counting them used to let a
// busy local node quarantine — and transit-tombstone — perfectly
// healthy neighbours, amplifying route churn across the mesh.
// ---------------------------------------------------------------------------

// TestQuarantine_LocalEvictionClosesDoNotFeedDisconnectStorm pins that
// session closes classified as local evictions are excluded from
// disconnect_storm accounting entirely: no history entries, no arm.
func TestQuarantine_LocalEvictionClosesDoNotFeedDisconnectStorm(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	caps := []domain.Capability{domain.CapMeshRelayV1}

	for i := 0; i < quarantineDisconnectThreshold+1; i++ {
		svc.onPeerSessionEstablished(idPeerB, caps)
		svc.onPeerSessionClosedWithCause(idPeerB, caps, sessionCloseLocalEviction)
	}

	if svc.IsPeerInRouteQuarantine(idPeerB) {
		t.Fatal("local-eviction closes must not arm disconnect_storm quarantine")
	}
	svc.peerMu.RLock()
	histLen := len(svc.peerDisconnectHistory[idPeerB])
	svc.peerMu.RUnlock()
	if histLen != 0 {
		t.Fatalf("disconnect history has %d entries, want 0 for local evictions", histLen)
	}
}

// TestQuarantine_PeerInitiatedClosesStillArm guards the other side of
// the classification: genuine peer-side flaps (the default cause used
// by onPeerSessionClosed) must keep arming the quarantine exactly as
// before the cause split.
func TestQuarantine_PeerInitiatedClosesStillArm(t *testing.T) {
	svc := newTestServiceWithRouting(t, idNodeA)
	caps := []domain.Capability{domain.CapMeshRelayV1}

	for i := 0; i < quarantineDisconnectThreshold; i++ {
		svc.onPeerSessionEstablished(idPeerB, caps)
		svc.onPeerSessionClosed(idPeerB, caps)
	}

	if !svc.IsPeerInRouteQuarantine(idPeerB) {
		t.Fatalf("peer-initiated close storm (%d closes) must arm quarantine",
			quarantineDisconnectThreshold)
	}
}

// TestSessionCloseCauseFromError pins the error→cause mapping used by
// the session cleanup paths. Local evictions are: errors wrapping
// errPeerSessionInboxOverflow (slow-consumer eviction) and nil — the
// ONLY nil-returning exits of servePeerSession are local context
// cancellation (ctx.Done, and writerDone re-checked against ctx.Err),
// so a nil teardown is a clean local shutdown, never a peer flap.
// Without the nil→local mapping, a node shutdown that raced the
// session goroutine cleanup ahead of onCMSessionTeardown recorded a
// disconnect_storm event against every connected peer. Everything
// else is peer-initiated.
func TestSessionCloseCauseFromError(t *testing.T) {
	t.Parallel()

	overflow := fmt.Errorf("%w for 10.0.0.9:64646", errPeerSessionInboxOverflow)
	if got := sessionCloseCauseFromError(overflow); got != sessionCloseLocalEviction {
		t.Fatalf("inbox overflow error: got cause %v, want sessionCloseLocalEviction", got)
	}

	reset := errors.New("read tcp 10.0.0.1:1->10.0.0.2:2: read: connection reset by peer")
	if got := sessionCloseCauseFromError(reset); got != sessionClosePeerInitiated {
		t.Fatalf("connection reset: got cause %v, want sessionClosePeerInitiated", got)
	}
	if sessionCloseProvidesPeerOfflineEvidence(reset) {
		t.Fatal("connection reset is ambiguous and must not provide identity-scoped presence evidence")
	}
	if sessionCloseProvidesPeerOfflineEvidence(io.ErrUnexpectedEOF) {
		t.Fatal("truncated-frame unexpected EOF must not be treated as a clean remote FIN")
	}

	timeout := errors.New("read tcp 10.0.0.1:1->10.0.0.2:2: i/o timeout")
	if got := sessionCloseCauseFromError(timeout); got != sessionClosePeerInitiated {
		t.Fatalf("timeout: got cause %v, want sessionClosePeerInitiated", got)
	}
	if sessionCloseProvidesPeerOfflineEvidence(timeout) {
		t.Fatal("ambiguous timeout must not provide identity-scoped presence evidence")
	}

	if got := sessionCloseCauseFromError(nil); got != sessionCloseLocalEviction {
		t.Fatalf("nil error (local ctx cancellation): got cause %v, want sessionCloseLocalEviction", got)
	}
	if sessionCloseProvidesPeerOfflineEvidence(nil) {
		t.Fatal("local cancellation must not provide direct presence evidence")
	}
}

func TestPendingWithdrawalPresenceEvidenceCanBeUpgraded(t *testing.T) {
	t.Parallel()

	svc := &Service{
		pendingWithdrawals:             make(map[domain.PeerIdentity]*pendingWithdrawal),
		routeWithdrawalGracePeriodTest: time.Hour,
	}
	peer := domaintest.ID("presence-evidence-upgrade")
	svc.maybeScheduleDeferredWithdrawalWithAttribution(peer, nil, nil)
	t.Cleanup(svc.cancelAllPendingWithdrawalsForShutdown)
	evidence := &peerOfflineEvidence{observedAt: presenceInstantAt(time.Now())}
	svc.maybeScheduleDeferredWithdrawalWithAttribution(peer, nil, evidence)

	svc.peerMu.Lock()
	entry := svc.pendingWithdrawals[peer]
	svc.peerMu.Unlock()
	if entry == nil || entry.presenceEvidence != evidence {
		t.Fatal("later direct EOF did not upgrade pending withdrawal presence evidence")
	}
}
