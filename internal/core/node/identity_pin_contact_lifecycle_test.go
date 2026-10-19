package node

import (
	"bytes"
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/piratecash/corsa/internal/core/domain"
)

// Direct store fixtures explicitly simulate a local row application. Production
// has no identity-only fact-stage entry point: it requires the original stamp.
func (s *trustStore) stageLocalContact(ctx context.Context, id domain.PeerIdentity) (localContactStamp, error) {
	s.mu.Lock()
	row, ok := s.contacts[id.String()]
	if !ok {
		s.mu.Unlock()
		return localContactStamp{}, domain.ErrLocalContactFactStale
	}
	applied := s.bindLocalContactRowLocked(&row, contactApplyExplicitLocal)
	s.contacts[id.String()] = row
	s.mu.Unlock()
	return s.stageBoundLocalContact(ctx, applied)
}

func TestK1DeletedBeforePreflightCannotAuthorizeGenericReadd(t *testing.T) {
	s, peer := localContactFixture(t)
	original := s.trust.localFiles
	files := NewMocklocalContactDurableFiles(t)
	entered, release := make(chan struct{}), make(chan struct{})
	files.EXPECT().Preflight(mock.Anything).RunAndReturn(func(context.Context) error { close(entered); <-release; return nil }).Once()
	files.EXPECT().Close().Return(nil).Once()
	s.trust.localFiles = func() (localContactDurableFiles, error) { return files, nil }
	type completion struct {
		result domain.LocalContactIntentResult
		err    error
	}
	done := make(chan completion, 1)
	go func() {
		result, err := s.AddLocalContact(t.Context(), contactIntent(peer))
		done <- completion{result, err}
	}()
	<-entered
	before := s.trust.trustedContacts()[peer.Address].localRow
	if before == nil {
		t.Fatal("local lifecycle was not captured before preflight")
	}
	s.deleteTrustedContactFrame(domain.PeerIdentityFromWire(peer.Address))
	intent := contactIntent(peer)
	s.trustContact(peer.Address, string(intent.PubKey), string(intent.BoxKey), string(intent.BoxSignature), "generic_readd")
	row := s.trust.trustedContacts()[peer.Address]
	if row.LocalContact != nil || row.localRow != nil {
		t.Fatal("generic readd minted local intent")
	}
	close(release)
	completionResult := <-done
	if !errors.Is(completionResult.err, domain.ErrLocalContactFactStale) || completionResult.result.Contact != domain.ContactAppliedLive || completionResult.result.Fact != domain.LocalContactFactAbsent {
		t.Fatalf("dishonest stale result: %+v, %v", completionResult.result, completionResult.err)
	}
	if row := s.trust.trustedContacts()[peer.Address]; row.LocalContact != nil || s.trust.authorityObserved {
		t.Fatal("stale operation created durable authority")
	}
	s.trust.localFiles = original
	addLocalContact(t, s, peer)
	current := s.trust.trustedContacts()[peer.Address]
	if current.LocalContact == nil || current.localRow == nil || current.localRow == before {
		t.Fatal("fresh manual action did not bind the recreated row")
	}
}

func TestK1ConcurrentManualApplySurvivesGenericRefreshOnSameRow(t *testing.T) {
	s, peer := localContactFixture(t)
	original := s.trust.localFiles
	files := NewMocklocalContactDurableFiles(t)
	entered, release := make(chan struct{}, 2), make(chan struct{})
	files.EXPECT().Preflight(mock.Anything).RunAndReturn(func(context.Context) error { entered <- struct{}{}; <-release; return nil }).Twice()
	files.EXPECT().Close().Return(nil).Twice()
	var calls atomic.Uint32
	s.trust.localFiles = func() (localContactDurableFiles, error) {
		if calls.Add(1) <= 2 {
			return files, nil
		}
		return original()
	}
	type completion struct {
		result domain.LocalContactIntentResult
		err    error
	}
	done := make(chan completion, 2)
	for range 2 {
		go func() {
			result, err := s.AddLocalContact(t.Context(), contactIntent(peer))
			done <- completion{result, err}
		}()
	}
	<-entered
	<-entered
	before := s.trust.trustedContacts()[peer.Address].localRow
	intent := contactIntent(peer)
	s.trustContact(peer.Address, string(intent.PubKey), string(intent.BoxKey), string(intent.BoxSignature), "generic_refresh")
	refreshed := s.trust.trustedContacts()[peer.Address]
	if before == nil || refreshed.localRow != before || refreshed.LocalContact != nil {
		t.Fatal("generic refresh changed lifecycle or granted authority")
	}
	close(release)
	for range 2 {
		result := <-done
		if result.err != nil || result.result.Contact != domain.ContactAppliedLive || result.result.Fact != domain.LocalContactFactDurableConfirmed {
			t.Fatalf("same-row local retry failed: %+v %v", result.result, result.err)
		}
	}
	s.trust.localFiles = original
	fact := s.trust.trustedContacts()[peer.Address].LocalContact.fact
	addLocalContact(t, s, peer)
	if s.trust.trustedContacts()[peer.Address].LocalContact.fact != fact {
		t.Fatal("idempotent local action replaced durable fact")
	}
	loaded, err := loadTrustStore(s.trust.path, trustedContact{})
	if err != nil {
		t.Fatal(err)
	}
	restored := loaded.trustedContacts()[peer.Address]
	if restored.localRow != nil || restored.LocalContact == nil || restored.LocalContact.fact != fact {
		t.Fatal("restart restored transient lifecycle or lost durable fact")
	}
	raw, err := os.ReadFile(s.trust.path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("localRow")) || bytes.Contains(raw, []byte("local_row")) {
		t.Fatal("runtime marker was serialized")
	}
	if domain.SizeOfAll(localContactRowIncarnation{}) == 0 {
		t.Fatal("row marker must have nonzero size")
	}
	snapshot := s.trust.trustedContacts()[peer.Address]
	if snapshot.localRow != before {
		t.Fatal("immutable snapshot marker unexpectedly changed")
	}
}

func TestK1CancelledAfterLocalApplyDoesNotStageFact(t *testing.T) {
	s, peer := localContactFixture(t)
	files := NewMocklocalContactDurableFiles(t)
	entered, release := make(chan struct{}), make(chan struct{})
	files.EXPECT().Preflight(mock.Anything).RunAndReturn(func(context.Context) error { close(entered); <-release; return nil }).Once()
	files.EXPECT().Close().Return(nil).Once()
	s.trust.localFiles = func() (localContactDurableFiles, error) { return files, nil }
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	type completion struct {
		result domain.LocalContactIntentResult
		err    error
	}
	done := make(chan completion, 1)
	go func() { result, err := s.AddLocalContact(ctx, contactIntent(peer)); done <- completion{result, err} }()
	<-entered
	cancel()
	close(release)
	result := <-done
	if !errors.Is(result.err, context.Canceled) || result.result.Contact != domain.ContactAppliedLive {
		t.Fatalf("cancelled result %+v %v", result.result, result.err)
	}
	row := s.trust.trustedContacts()[peer.Address]
	if row.Address == "" || row.localRow == nil || row.LocalContact != nil || s.trust.authorityObserved {
		t.Fatal("cancel converted transient application to authority")
	}
}
