package node

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/stretchr/testify/mock"
)

func selectedCMFixture(t *testing.T) (*Service, *Service, *peerSession, *selectedTestClock) {
	t.Helper()
	listener, address, stopListener := listenerNode(t, withoutOwnDials(nil))
	t.Cleanup(stopListener)
	clock := &selectedTestClock{base: time.Now()}
	dialer, stopDialer := dialerNode(t, address, func(s *Service) { s.selectedPins.clock = clock.Now })
	t.Cleanup(stopDialer)
	var session *peerSession
	waitForConditionMsg(t, 10*time.Second, "no published real proven CM session", func() bool {
		dialer.peerMu.RLock()
		defer dialer.peerMu.RUnlock()
		for _, candidate := range dialer.sessions {
			if candidate.selectedProof != nil && candidate.selectedGeneration != 0 && candidate.closedBy.Load() == nil {
				session = candidate
				return true
			}
		}
		return false
	})
	if _, ok := session.provenIdentity(); !ok {
		t.Fatal("fixture did not run a genuine v2 proof")
	}
	return dialer, listener, session, clock
}
func TestSelectedRealCMSessionPromotesOnlyAtContinuousTenMinutes(t *testing.T) {
	dialer, listener, session, clock := selectedCMFixture(t)
	binding := session.selectedProof.binding
	if session.selectedProof.group != domain.SourceGroupLoopback {
		t.Fatalf("actual loopback attributed to %s", session.selectedProof.group)
	}
	clock.advance(selectedSessionAge - time.Nanosecond)
	if err := dialer.selectedPins.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	if dialer.secureSessions.store.selectedPinStatus(binding).allocated {
		t.Fatal("class allocated before ten minutes")
	}
	// Another legitimate metadata change and actual fresh v2 proof must not
	// turn the same entry/CM session into a new ten-minute probation.
	stageMetadata(t, dialer.secureSessions.store, binding.identity, pinClassMetadataFixture(t))
	if err := dialWithIntent(t, dialer, session.address, domain.DialToAddress()); err != nil {
		t.Fatal(err)
	}
	clock.advance(time.Nanosecond)
	if err := dialer.selectedPins.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !dialer.secureSessions.store.selectedPinStatus(binding).confirmed {
		t.Fatal("real CM proof did not yield strictly confirmed N-sel")
	}
	if stats := listener.secureSessions.store.stats(); stats.SelectedAllocations != 0 {
		t.Fatal("inbound peer was misclassified as selected")
	}
	store := dialer.secureSessions.store
	restarted := loadSecureSessionStore(filepath.Join(store.dir, store.name), newStoreClock().trusted())
	entry := restarted.identities[binding.identity]
	if entry.classMetadata().Selected.State != domain.IdentityPinAllocationEligible || entry.selectedConfirmed() {
		t.Fatal("restart lost historical eligibility or invented strict runtime acknowledgment")
	}
}
func TestSelectedReconnectDoesNotAggregateShortSessions(t *testing.T) {
	dialer, _, first, clock := selectedCMFixture(t)
	clock.advance(6 * time.Minute)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	var second *peerSession
	waitForConditionMsg(t, 10*time.Second, "CM did not replace closed connection", func() bool {
		dialer.peerMu.RLock()
		defer dialer.peerMu.RUnlock()
		for _, candidate := range dialer.sessions {
			if candidate != first && candidate.selectedProof != nil && candidate.connID != first.connID && candidate.closedBy.Load() == nil {
				second = candidate
				return true
			}
		}
		return false
	})
	clock.advance(6 * time.Minute)
	if err := dialer.selectedPins.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	if dialer.secureSessions.store.selectedPinStatus(second.selectedProof.binding).allocated {
		t.Fatal("two short sessions accumulated age")
	}
	clock.advance(4 * time.Minute)
	if err := dialer.selectedPins.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !dialer.secureSessions.store.selectedPinStatus(second.selectedProof.binding).confirmed {
		t.Fatal("replacement session's own ten minutes did not count")
	}
}
func TestSelectedTerminalReaderExitRevokesContinuityBeforePublicationCleanup(t *testing.T) {
	dialer, _, session, clock := selectedCMFixture(t)
	clock.advance(selectedSessionAge - time.Second)
	dialer.peerMu.Lock()
	defer dialer.peerMu.Unlock()
	if err := session.conn.Close(); err != nil {
		t.Fatal(err)
	}
	waitForConditionMsg(t, 5*time.Second, "actual reader did not revoke eligibility", func() bool { return session.eligibilityEnded.Load() })
	// Isolate the terminal-reader predicate from both owner Close and cleanup.
	// Immutable proof/incarnation/generation came from this real v2 CM session.
	projected := &peerSession{selectedProof: session.selectedProof, selectedGeneration: session.selectedGeneration, connID: session.connID, proven: session.proven}
	projection := &Service{sessions: map[domain.PeerAddress]*peerSession{session.address: projected}}
	producer := newSelectedPinProducer(selectedPinProducerOptions{service: projection, clock: clock.Now})
	clock.advance(time.Second)
	if len(producer.capture(clock.Now())) != 1 {
		t.Fatal("positive stale-map entry was not eligible before terminal flag")
	}
	projected.eligibilityEnded.Store(session.eligibilityEnded.Load())
	if projected.closedBy.Load() != nil {
		t.Fatal("Close masked terminal flag")
	}
	if len(producer.capture(clock.Now())) != 0 {
		t.Fatal("terminal EOF flag did not exclude retained stale-map entry before cleanup")
	}
}

func TestSelectedUnsupportedBackendPrecedesNewAllocation(t *testing.T) {
	dialer, _, session, clock := selectedCMFixture(t)
	p := dialer.selectedPins
	p.passMu.Lock()
	defer p.passMu.Unlock()
	clock.advance(selectedSessionAge)
	facts := p.capture(clock.Now())
	if len(facts) != 1 {
		t.Fatalf("earned actual facts=%d", len(facts))
	}
	files := NewMockidentityPinDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(secretfile.ErrDurabilityUnsupported).Once()
	if err := p.promote(t.Context(), facts, files); !errors.Is(err, secretfile.ErrDurabilityUnsupported) {
		t.Fatal(err)
	}
	if dialer.secureSessions.store.selectedPinStatus(session.selectedProof.binding).allocated || session.eligibilityEnded.Load() {
		t.Fatal("unsupported backend changed class or ordinary session")
	}
}

func TestSelectedWitnessSurvivesCloseDuringBackendPreflight(t *testing.T) {
	dialer, _, session, clock := selectedCMFixture(t)
	p := dialer.selectedPins
	p.passMu.Lock()
	defer p.passMu.Unlock()
	clock.advance(selectedSessionAge)
	facts := p.capture(clock.Now())
	if len(facts) != 1 {
		t.Fatalf("earned actual facts=%d", len(facts))
	}
	real, err := dialer.secureSessions.store.openIdentityPinFiles()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := real.Close(); err != nil {
			t.Error(err)
		}
	}()
	files := NewMockidentityPinDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).RunAndReturn(func(ctx context.Context) error {
		if err := session.Close(); err != nil {
			t.Fatal(err)
		}
		return real.Preflight(ctx)
	}).Once()
	files.EXPECT().Preflight(mock.Anything).RunAndReturn(real.Preflight).Once()
	files.EXPECT().WriteMain(mock.Anything, mock.Anything).RunAndReturn(real.WriteMain).Once()
	if err := p.promote(t.Context(), facts, files); err != nil {
		t.Fatal(err)
	}
	if !dialer.secureSessions.store.selectedPinStatus(session.selectedProof.binding).confirmed {
		t.Fatal("close erased a fact already witnessed at live threshold")
	}
}

func TestSelectedActualSessionCannotRebindRecreatedPin(t *testing.T) {
	dialer, _, session, clock := selectedCMFixture(t)
	p := dialer.selectedPins
	p.passMu.Lock()
	defer p.passMu.Unlock()
	clock.advance(selectedSessionAge)
	facts := p.capture(clock.Now())
	store := dialer.secureSessions.store
	store.pinMutationGate.Lock()
	store.mu.Lock()
	store.deletePinEntryLocked(session.selectedProof.binding.identity)
	store.changes++
	store.mu.Unlock()
	store.pinMutationGate.Unlock()
	if err := store.noteProvenInbound(session.selectedProof.binding.identity); err != nil {
		t.Fatal(err)
	}
	files := NewMockidentityPinDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(nil).Once()
	if err := p.promote(t.Context(), facts, files); err != nil {
		t.Fatal(err)
	}
	if store.stats().SelectedAllocations != 0 {
		t.Fatal("old CM session classified a newly recreated pin")
	}
}

func TestSelectedOneShotProofHasNoSessionAge(t *testing.T) {
	listener, address, stopListener := listenerNode(t, withoutOwnDials(nil))
	t.Cleanup(stopListener)
	clock := &selectedTestClock{base: time.Now()}
	dialer, stopDialer := startQuietNode(t, func(s *Service) { s.selectedPins.clock = clock.Now })
	t.Cleanup(stopDialer)
	if err := dialWithIntent(t, dialer, domain.PeerAddress(address), domain.DialToAddress()); err != nil {
		t.Fatal(err)
	}
	if !dialer.secureSessions.store.identityPinned(peerIdentityOf(t, listener)) {
		t.Fatal("one-shot genuine proof did not pin")
	}
	clock.advance(2 * selectedSessionAge)
	if err := dialer.selectedPins.scan(t.Context()); err != nil {
		t.Fatal(err)
	}
	if dialer.secureSessions.store.stats().SelectedAllocations != 0 {
		t.Fatal("one-shot proof inferred selected session continuity")
	}
}
