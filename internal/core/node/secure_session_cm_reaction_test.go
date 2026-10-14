package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/sessionv2"
	"github.com/piratecash/corsa/internal/core/sessionv2/sessionv2test"
)

// secure_session_cm_reaction_test.go pins that the node's reaction to a
// failed outbound attempt never undoes downgrade protection — not the endpoint
// binding, not the identity pin — and never marks the endpoint as a v1 node.
//
// The tests of each refusal (session_secure_test.go, pin_store_full_test.go)
// stop after the second refusal; a reaction that gave way only on a later
// failure would slip past them. This contract runs each kind of failure
// through the manager's own retries and replacement and then through
// reactionSeries more reactions — well past reconnectMaxRetries (3), the
// setup-failure ban threshold (3) and the two refusals the per-refusal tests
// wait for — and checks the protection afterwards.
//
// What the reaction is, and what covers each part.
//
//   - A failed dial (DialFailed → handleDialFailed): retry or replace in the
//     manager's own slot table, a refill through PeerProvider.Candidates
//     (read-only), and the one callback that writes into the Service,
//     OnDialFailed (NewService wires it to onCMDialFailed). Phase 1 runs it
//     through a live manager whose DialFn fails; phase 2 calls the wired
//     OnDialFailed directly.
//   - A session that opened and failed its setup: the session goroutine's
//     failCMSessionSetup, then ActiveSessionLost → handleActiveSessionLost,
//     which reads IsSetupFailureBannedFn and calls the same OnDialFailed
//     before it retries or replaces. Phase 1 dials real sessions whose far
//     end is already closed, so every step of that path runs; phase 2 calls
//     failCMSessionSetup and the wired OnDialFailed directly. The sessions
//     are what a bound endpoint can actually carry: v2, proven by the very
//     identity the endpoint is bound to, so a reaction that acts on the
//     session's identity or on its proven penalty subject is exercised too.
//   - Deactivating a slot calls OnSessionTeardown (onCMSessionTeardown). In
//     phase 1 it runs for Initializing slots whose session never reached
//     s.sessions, so it finds nothing to remove; on a published session it
//     touches the session maps only, never the secure-session store or the
//     address marks. No session of this test is published.

// reactionSeries is how many reactions phase 2 adds to the manager's own.
const reactionSeries = 30

// reactionFixture is the node under test, the endpoint it is bound to, and
// the v2 peer that endpoint is bound to — whose identity is pinned.
type reactionFixture struct {
	svc   *Service
	bound domain.PeerAddress
	peer  sessionv2.Peer
}

// cmFailureKind is one way an outbound attempt can fail. dial returns the
// attempt's outcome, doing the bookkeeping the real failure does before it
// returns; react repeats the node's reaction to that failure once.
type cmFailureKind struct {
	dial  func(t *testing.T, f reactionFixture) (DialResult, error)
	react func(t *testing.T, f reactionFixture)
	// done reports that the manager has finished its own reactions to the
	// failures phase 1 produced: the slot is replaced and not redialled.
	done func(f reactionFixture, attempts int64) bool
}

// failedDial is a kind whose attempts fail inside DialFn with err, after
// count has done the bookkeeping the real refusal does.
func failedDial(err error, count func(*secureSessionStore)) cmFailureKind {
	return cmFailureKind{
		dial: func(_ *testing.T, f reactionFixture) (DialResult, error) {
			count(f.svc.secureSessions.store)
			return DialResult{}, err
		},
		react: func(_ *testing.T, f reactionFixture) {
			count(f.svc.secureSessions.store)
			f.svc.connManager.config.OnDialFailed(f.bound, err, errors.Is(err, errIncompatibleProtocol))
		},
		done: func(f reactionFixture, attempts int64) bool {
			// One attempt plus reconnectMaxRetries retries, then the slot is
			// replaced and the peer is cooled down out of the candidates.
			return attempts >= reconnectMaxRetries+1 && f.svc.connManager.SlotCount() == 0
		},
	}
}

// failedSetup is the kind whose attempts open a session that then fails its
// setup: the far end of each session is closed before the manager hands it
// to the node, so initPeerSession fails on its first request.
func failedSetup() cmFailureKind {
	setupErr := errors.New("setup failed: the peer closed the session")
	return cmFailureKind{
		dial: func(t *testing.T, f reactionFixture) (DialResult, error) {
			return DialResult{Session: deadOutboundSession(t, f), ConnectedAddress: f.bound}, nil
		},
		react: func(t *testing.T, f reactionFixture) {
			f.svc.failCMSessionSetup(deadOutboundSession(t, f), f.bound, 0, setupErr)
			f.svc.connManager.config.OnDialFailed(f.bound, setupErr, false)
		},
		done: func(f reactionFixture, _ int64) bool {
			// Three failed setups ban the address: the manager replaces the
			// slot instead of retrying, and Candidates leaves it out.
			return f.svc.IsSetupFailureBanned(f.bound) && f.svc.connManager.SlotCount() == 0
		},
	}
}

// deadOutboundSession builds the v2 session openPeerSessionForCM hands the
// manager for the bound endpoint — a NetCore over its socket, proven by the
// peer the endpoint is bound to — whose peer has already hung up.
func deadOutboundSession(t *testing.T, f reactionFixture) *peerSession {
	t.Helper()
	local, remote := net.Pipe()
	_ = remote.Close()
	f.svc.peerMu.Lock()
	connID := f.svc.nextConnIDLocked()
	f.svc.peerMu.Unlock()
	peer := f.peer
	session := &peerSession{
		address:      f.bound,
		peerIdentity: peer.Identity,
		connID:       connID,
		proven:       &peer,
		authOK:       true,
		conn:         local,
		inboxCh:      make(chan protocol.Frame),
		errCh:        make(chan error, 1),
	}
	if _, proven := session.provenIdentity(); !proven {
		t.Fatal("precondition: the session carries no v2 proof")
	}
	if err := f.svc.attachOutboundNetCore(session); err != nil {
		t.Fatalf("fixture: attachOutboundNetCore: %v", err)
	}
	t.Cleanup(func() { _ = session.Close() })
	return session
}

func cmFailureKinds() map[string]cmFailureKind {
	nothing := func(*secureSessionStore) {}
	return map[string]cmFailureKind{
		"downgrade refused":       failedDial(fmt.Errorf("%w: contract", errLegacyRefusedV2Bound), (*secureSessionStore).noteEndpointRefusal),
		"identity pinned to v2":   failedDial(fmt.Errorf("%w: contract", errLegacyRefusedPinned), (*secureSessionStore).noteIdentityRefusal),
		"pin store full":          failedDial(errPinStoreFull, nothing),
		"binding not written":     failedDial(fmt.Errorf("secure session store: %w", errors.New("disk full")), nothing),
		"incompatible protocol":   failedDial(fmt.Errorf("%w: contract", errIncompatibleProtocol), nothing),
		"transport":               failedDial(fmt.Errorf("%w: contract", errPeerDialTransport), nothing),
		"unclassified v2 failure": failedDial(errors.New("v2 handshake timeout"), nothing),
		"session setup failed":    failedSetup(),
	}
}

func TestTheConnectionManagersReactionToAFailedDialNeverUndoesProtection(t *testing.T) {
	t.Parallel()

	const endpoint = "1.2.3.4:64646"
	for name, kind := range cmFailureKinds() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			svc := newServiceWithBootstrapPeers(t, publicCandidatesOnly, endpoint)
			store := svc.secureSessions.store
			peer, _ := sessionv2test.NewProvenPeer(t)
			f := reactionFixture{svc: svc, bound: domain.PeerAddress(endpoint), peer: peer}
			bound, id := f.bound, peer.Identity
			if err := store.noteProvenOutbound(bound, id, false); err != nil {
				t.Fatalf("bind: %v", err)
			}

			// Phase 1: the real manager, with its own retries and its
			// replacement of the slot, every attempt failing the same way.
			var attempts atomic.Int64
			cm := svc.connManager
			cm.config.BackoffFn = func(int) time.Duration { return 0 }
			cm.config.DialFn = func(context.Context, []domain.PeerAddress) (DialResult, error) {
				attempts.Add(1)
				return kind.dial(t, f)
			}
			stopCM := runCMUntilStopped(t, cm)
			cm.NotifyBootstrapReady()
			waitForConditionMsg(t, 10*time.Second, "the manager never finished reacting to the failing slot", func() bool {
				return kind.done(f, attempts.Load())
			})
			stopCM()
			// Session goroutines run on runLoopsWg (no Run joins it here),
			// fire-and-forget work on backgroundWg; both must be done before
			// the reactions are counted and the protection read.
			svc.runLoopsWg.Wait()
			svc.WaitBackground()

			// Phase 2: a long run of the same reaction, through the real
			// functions the manager and the session goroutine call.
			for range reactionSeries {
				kind.react(t, f)
			}

			total := attempts.Load() + reactionSeries
			if !store.endpointRequiresV2(bound) {
				t.Fatalf("after %d failures the reaction had dropped the endpoint binding", total)
			}
			if !store.identityPinned(id) {
				t.Fatalf("after %d failures the reaction had dropped the identity pin", total)
			}
			if svc.secureSessions.marks.v1Seen(bound) {
				t.Fatalf("after %d failures the reaction had marked the endpoint as a v1 node", total)
			}
		})
	}
}
