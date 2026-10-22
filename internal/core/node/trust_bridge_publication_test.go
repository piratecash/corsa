package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/secretfile"
	"github.com/piratecash/corsa/internal/core/sessionv2/sessionv2test"
	"github.com/stretchr/testify/mock"
)

// Real trust-store constructor/writers with a private FixtureDomain, not a
// production Service/domain/Ready composition or a live intake guarantee.
func trustPublicationFixture(t *testing.T) (*trustStore, findnodebridge.FixtureDomain, *atomic.Int64, chan struct{}) {
	t.Helper()
	clock, done := new(atomic.Int64), make(chan struct{})
	d, err := findnodebridge.NewFixtureDomain(done, clock)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "trust.json")
	s, err := loadTrustStoreWithPublisher(path, trustedContact{}, func() (localContactDurableFiles, error) { return openLocalContactFiles(path) }, d.Publisher())
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Publisher().SealConstruction(); err != nil {
		t.Fatal(err)
	}
	return s, d, clock, done
}
func trustPublicationPeer(t *testing.T) (*identity.Identity, trustedContact) {
	t.Helper()
	peer, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	intent := contactIntent(peer)
	return peer, trustedContact{Address: peer.Address, PubKey: string(intent.PubKey), BoxKey: string(intent.BoxKey), BoxSignature: string(intent.BoxSignature), Source: "trust-publication"}
}
func trustPublicationApply(t *testing.T, s *trustStore, row trustedContact) localContactStamp {
	t.Helper()
	stored, applied, err := s.rememberContactOrigin(t.Context(), row, contactApplyExplicitLocal)
	if err != nil || !stored || applied.incarnation == nil {
		t.Fatal("explicit trust row not applied", stored, err)
	}
	stamp, err := s.stageBoundLocalContact(t.Context(), applied)
	if err != nil {
		t.Fatal(err)
	}
	return stamp
}
func trustPublicationRequireStrict(t *testing.T, s *trustStore) {
	t.Helper()
	files, err := s.localFiles()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = files.Close() }()
	if err := files.Preflight(t.Context()); errors.Is(err, secretfile.ErrDurabilityUnsupported) {
		t.Skip("strict local-contact backend unavailable")
	} else if err != nil {
		t.Fatal(err)
	}
}
func TestTrustBridgeOrdinaryZeroAndUnescapedConstructorLoad(t *testing.T) {
	plain, err := loadTrustStore("", trustedContact{})
	if err != nil {
		t.Fatal(err)
	}
	if plain.trustPublisher != (findnodebridge.Publisher{}) || plain.beginTrustPublication() != nil {
		t.Fatal("ordinary trust constructor attached or allocated publication")
	}
	prior, _, _, _ := trustPublicationFixture(t)
	_, row := trustPublicationPeer(t)
	if stored, err := prior.remember(row); err != nil || !stored {
		t.Fatal(err)
	}
	d, err := findnodebridge.NewFixtureDomain(make(chan struct{}), new(atomic.Int64))
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := loadTrustStoreWithPublisher(prior.path, trustedContact{}, func() (localContactDurableFiles, error) { return openLocalContactFiles(prior.path) }, d.Publisher())
	if err != nil || loaded.trustPublisher == (findnodebridge.Publisher{}) {
		t.Fatal("constructor failed immutable pre-load dependency", err)
	}
	if _, err := d.Observer().BeginCapture(); err == nil {
		t.Fatal("partial trust constructor sealed Ready")
	}
	got := loaded.trustedContacts()[row.Address]
	if got.PubKey != row.PubKey || got.LocalContact != nil || loaded.authorityObserved {
		t.Fatal("ordinary load invented local authority or lost row")
	}
	if err := d.Publisher().SealConstruction(); err != nil {
		t.Fatal(err)
	}
	_ = pinPublicationCapture(t, d)
	if err := os.WriteFile(prior.path, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	bad, err := loadTrustStoreWithPublisher(prior.path, trustedContact{}, func() (localContactDurableFiles, error) { return openLocalContactFiles(prior.path) }, d.Publisher())
	if err == nil || bad != nil {
		t.Fatal("malformed constructor load was accepted")
	}
}
func TestTrustBridgeContactWriterFamiliesInvalidateGeneration(t *testing.T) {
	for _, kind := range []string{"add", "refresh", "forget", "record", "last_online"} {
		t.Run(kind, func(t *testing.T) {
			s, d, _, _ := trustPublicationFixture(t)
			peer, row := trustPublicationPeer(t)
			id := domain.PeerIdentityFromWire(peer.Address)
			var previous trustedContact
			if kind != "add" {
				stored, _, err := s.rememberContactOrigin(t.Context(), row, contactApplyExplicitLocal)
				if err != nil || !stored {
					t.Fatal(err)
				}
				previous = s.trustedContacts()[row.Address]
			}
			stamp := pinPublicationCapture(t, d)
			switch kind {
			case "add", "refresh":
				row.Source = "generic refresh"
				if stored, _, err := s.rememberContactOrigin(t.Context(), row, contactApplyExplicitLocal); err != nil || !stored {
					t.Fatal(err)
				}
				current := s.trustedContacts()[row.Address]
				if current.localRow == nil || (kind == "refresh" && current.localRow != previous.localRow) {
					t.Fatal("contact refresh changed original row incarnation")
				}
			case "forget":
				if removed, err := s.forget(id); err != nil || !removed {
					t.Fatal(err)
				}
				if _, ok := s.trustedContacts()[row.Address]; ok {
					t.Fatal("forget retained live contact")
				}
			case "record":
				record, body := issueTestRecord(t, peer, 7, true)
				if outcome, err := s.rememberRecord(testRecordStoreNetwork, record, body); err != nil || !outcome.Accepted() {
					t.Fatal("record merge", outcome, err)
				}
				current := s.trustedContacts()[row.Address]
				if current.Source != trustContactSourceRecord || current.localRow != previous.localRow || current.BoxKey != string(body.BoxKey) {
					t.Fatal("record replacement changed original row or keys")
				}
			case "last_online":
				at := time.Date(2026, time.October, 20, 9, 0, 0, 0, time.UTC)
				if n, err := s.recordLastOnlineAt([]domain.PeerIdentity{id}, at, 0); err != nil || n != 1 {
					t.Fatal("last online", n, err)
				}
				current := s.trustedContacts()[row.Address]
				if !current.LastOnlineAt.Equal(at) || current.localRow != previous.localRow {
					t.Fatal("last-online replacement changed original row or time")
				}
			}
			if !pinPublicationRefused(d, stamp) {
				t.Fatal("trust memory writer preserved a stale capture", kind)
			}
		})
	}
}
func TestTrustBridgeFailedSaveKeepsLiveRememberAndForget(t *testing.T) {
	for _, kind := range []string{"remember", "forget", "record"} {
		t.Run(kind, func(t *testing.T) {
			s, d, _, _ := trustPublicationFixture(t)
			peer, row := trustPublicationPeer(t)
			if kind != "remember" {
				if stored, err := s.remember(row); err != nil || !stored {
					t.Fatal(err)
				}
			}
			blocker := filepath.Join(t.TempDir(), "file")
			if err := os.WriteFile(blocker, []byte("blocker"), 0600); err != nil {
				t.Fatal(err)
			}
			s.path = filepath.Join(blocker, "trust.json") // existing failure seam; no added production hook
			stamp := pinPublicationCapture(t, d)
			switch kind {
			case "remember":
				stored, err := s.remember(row)
				if !stored || err == nil {
					t.Fatal("failed save lost live remember result", stored, err)
				}
				if _, present := s.trustedContacts()[row.Address]; !present {
					t.Fatal("failed save rolled back live remember")
				}
			case "forget":
				removed, err := s.forget(domain.PeerIdentityFromWire(peer.Address))
				if !removed || err == nil {
					t.Fatal("failed save lost live forget result", removed, err)
				}
				if _, present := s.trustedContacts()[row.Address]; present {
					t.Fatal("failed save rolled back live forget")
				}
			default:
				record, body := issueTestRecord(t, peer, 8, true)
				outcome, err := s.rememberRecord(testRecordStoreNetwork, record, body)
				if !outcome.Accepted() || err == nil {
					t.Fatal("failed save lost accepted live record result", outcome, err)
				}
				if s.trustedContacts()[row.Address].Source != trustContactSourceRecord {
					t.Fatal("failed save rolled back live record row")
				}
			}
			if !pinPublicationRefused(d, stamp) {
				t.Fatal("failed save erased memory publication", kind)
			}
		})
	}
}
func TestTrustBridgeConflictAndOlderOnlineKeepAuthorityCut(t *testing.T) {
	s, d, _, _ := trustPublicationFixture(t)
	peer, row := trustPublicationPeer(t)
	if stored, err := s.remember(row); err != nil || !stored {
		t.Fatal(err)
	}
	id := domain.PeerIdentityFromWire(peer.Address)
	at := time.Now().UTC()
	if n, err := s.recordLastOnlineAt([]domain.PeerIdentity{id}, at, 0); err != nil || n != 1 {
		t.Fatal(err)
	}
	stamp := pinPublicationCapture(t, d)
	if n, err := s.recordLastOnlineAt([]domain.PeerIdentity{id}, at.Add(-time.Hour), 0); err != nil || n != 0 {
		t.Fatal("older observation changed contact", n, err)
	}
	conflict := row
	conflict.PubKey = "different"
	if stored, err := s.remember(conflict); stored || !errors.Is(err, errTrustConflict) {
		t.Fatal("conflict lost original refusal", stored, err)
	}
	if s.trustedContacts()[row.Address].PubKey != row.PubKey || pinPublicationRefused(d, stamp) {
		t.Fatal("non-authority conflict or rejected observation published contact")
	}
}
func TestTrustBridgeFactStagePreparesClockBeforeFinalW(t *testing.T) {
	s, d, _, _ := trustPublicationFixture(t)
	_, row := trustPublicationPeer(t)
	stored, applied, err := s.rememberContactOrigin(t.Context(), row, contactApplyExplicitLocal)
	if err != nil || !stored {
		t.Fatal(err)
	}
	stamp := pinPublicationCapture(t, d)
	at := time.Date(2026, time.October, 20, 10, 0, 0, 0, time.UTC)
	var calls int
	s.clock = func() time.Time { // existing immutable-before-concurrency test clock seam
		calls++
		read, err := d.Consumer().TryReadClock(stamp)
		if err != nil {
			t.Fatal("existing trust clock callback ran under publication W", err)
		}
		read.Release()
		return at
	}
	fact, err := s.stageBoundLocalContact(t.Context(), applied)
	if err != nil {
		t.Fatal(err)
	}
	s.mu.RLock()
	current := s.contacts[row.Address]
	observed, revision := s.authorityObserved, s.factRevision
	s.mu.RUnlock()
	if calls != 1 || current.LocalContact == nil || !current.LocalContact.fact.At.Equal(at) || current.LocalContact.revision != revision || !fact.matches(current.LocalContact, s) || !observed || current.LocalContact.confirmed != 0 {
		t.Fatal("fact stage split revision, row, clock or authority latch")
	}
	if !pinPublicationRefused(d, stamp) {
		t.Fatal("fact stage preserved stale capture")
	}
}
func TestTrustBridgeActualStrictACKCOWAndReload(t *testing.T) {
	s, d, _, _ := trustPublicationFixture(t)
	_, row := trustPublicationPeer(t)
	fact := trustPublicationApply(t, s, row)
	trustPublicationRequireStrict(t, s)
	s.mu.RLock()
	old := s.contacts[row.Address].LocalContact
	s.mu.RUnlock()
	stamp := pinPublicationCapture(t, d)
	if err := s.confirmLocalContacts(t.Context(), fact); err != nil {
		t.Fatal(err)
	}
	s.mu.RLock()
	current := s.contacts[row.Address].LocalContact
	s.mu.RUnlock()
	if old == current || old.confirmed != 0 || current.confirmed != current.revision || old.fact != current.fact || old.revision != current.revision {
		t.Fatal("strict trust ACK mutated old state or changed original fact")
	}
	if !pinPublicationRefused(d, stamp) {
		t.Fatal("strict trust ACK preserved stale capture")
	}
	loaded, err := loadTrustStore(s.path, trustedContact{})
	if err != nil {
		t.Fatal(err)
	}
	got := loaded.trustedContacts()[row.Address].LocalContact
	if got == nil || got.fact != old.fact || got.confirmed != 0 || loaded.localContactStatus(fact) != domain.LocalContactFactAbsent {
		t.Fatal("reload dropped fact or invented cross-owner strict ACK")
	}
}
func TestTrustBridgeUnconfirmedWritesNeverPublishACK(t *testing.T) {
	for _, tc := range []struct {
		name    string
		outcome secretfile.DurableCommit
		fail    bool
	}{
		{"not_committed", secretfile.DurableNotCommitted, false}, {"unknown", secretfile.DurableIndeterminate, false}, {"write_error", secretfile.DurableNotCommitted, true}, {"committed_error", secretfile.DurableCommitted, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, d, _, _ := trustPublicationFixture(t)
			_, row := trustPublicationPeer(t)
			fact := trustPublicationApply(t, s, row)
			files := NewMocklocalContactDurableFiles(t)
			files.EXPECT().Preflight(mock.Anything).Return(nil).Once()
			files.EXPECT().Close().Return(nil).Once()
			var injected error
			if tc.fail {
				injected = errors.New("strict trust write failure")
			}
			files.EXPECT().Write(mock.Anything, mock.Anything).Return(tc.outcome, injected).Once()
			s.localFiles = func() (localContactDurableFiles, error) { return files, nil } // existing durable test backend only
			stamp := pinPublicationCapture(t, d)
			if err := s.confirmLocalContacts(t.Context(), fact); !errors.Is(err, domain.ErrLocalContactFactUnconfirmed) {
				t.Fatal("unconfirmed trust write lost classification", err)
			}
			if s.localContactStatus(fact) != domain.LocalContactFactUnconfirmed || pinPublicationRefused(d, stamp) {
				t.Fatal("failed or unknown write published false ACK")
			}
		})
	}
}
func TestTrustBridgeStaleFactRevisionCannotAcknowledgeReplacement(t *testing.T) {
	s, d, _, _ := trustPublicationFixture(t)
	peer, row := trustPublicationPeer(t)
	old := trustPublicationApply(t, s, row)
	trustPublicationRequireStrict(t, s)
	if err := s.confirmLocalContacts(t.Context(), old); err != nil {
		t.Fatal(err)
	}
	if removed, err := s.forget(domain.PeerIdentityFromWire(peer.Address)); err != nil || !removed {
		t.Fatal(err)
	}
	fresh := trustPublicationApply(t, s, row)
	if fresh.id == old.id || fresh.revision <= old.revision {
		t.Fatal("re-added fact reused original id or revision")
	}
	stamp := pinPublicationCapture(t, d)
	if err := s.confirmLocalContacts(t.Context(), old); err != nil {
		t.Fatal(err)
	}
	if s.localContactStatus(fresh) != domain.LocalContactFactUnconfirmed || pinPublicationRefused(d, stamp) {
		t.Fatal("stale target confirmed or published replacement fact")
	}
	if err := s.confirmLocalContacts(t.Context(), fresh); err != nil {
		t.Fatal(err)
	}
	if s.localContactStatus(fresh) != domain.LocalContactFactDurableConfirmed || !pinPublicationRefused(d, stamp) {
		t.Fatal("fresh replacement fact lacked exact ACK publication")
	}
}
func TestTrustBridgeGenuineProofCacheAndPromotionClearPublish(t *testing.T) {
	s, d, _, _ := trustPublicationFixture(t)
	peer, row := trustPublicationPeer(t)
	fact := trustPublicationApply(t, s, row)
	trustPublicationRequireStrict(t, s)
	if err := s.confirmLocalContacts(t.Context(), fact); err != nil {
		t.Fatal(err)
	}
	pins := loadSecureSessionStore("", newStoreClock().trusted())
	binding, err := pins.noteProvenInboundBinding(domain.PeerIdentityFromWire(peer.Address))
	if err != nil {
		t.Fatal(err)
	}
	proven := sessionv2test.ProvenPeer(t, peer)
	proof, ok := proven.Proof()
	if !ok {
		t.Fatal("missing genuine TLS proof")
	}
	s.mu.RLock()
	old := s.contacts[row.Address].LocalContact
	s.mu.RUnlock()
	stamp := pinPublicationCapture(t, d)
	if !s.captureLocalContactProof(proof, binding) {
		t.Fatal("genuine proof cache was not captured")
	}
	s.mu.RLock()
	captured := s.contacts[row.Address].LocalContact
	candidate := captured.proof
	s.mu.RUnlock()
	if old == captured || old.proof != nil || candidate == nil || !pinPublicationRefused(d, stamp) {
		t.Fatal("proof cache changed old value or missed publication")
	}
	// A direct method fixture, not a live Service/domain bridge or active worker.
	svc := &Service{trust: s, secureSessions: &secureSessions{store: pins}}
	producer := newContactPinProducer(svc)
	stamp = pinPublicationCapture(t, d)
	if err := producer.promote(t.Context(), candidate); err != nil {
		t.Fatal(err)
	}
	s.mu.RLock()
	current := s.contacts[row.Address].LocalContact
	s.mu.RUnlock()
	if current == captured || captured.proof != candidate || current.proof != nil || current.fact != captured.fact || !pinPublicationRefused(d, stamp) {
		t.Fatal("promotion proof-clear changed fact, old cache or publication")
	}
}
func trustPublicationRememberWorker(s *trustStore, row trustedContact, finished chan error) {
	_, err := s.remember(row)
	finished <- err
}
func trustPublicationConfirmWorker(s *trustStore, fact localContactStamp, finished chan error) {
	finished <- s.confirmLocalContacts(context.Background(), fact)
}
func trustPublicationWaitStack(t *testing.T, owner, needle string) {
	t.Helper()
	waitForConditionMsg(t, 2*time.Second, "trust writer did not reach final awaited lock", func() bool {
		raw := make([]byte, 1<<20)
		n := runtime.Stack(raw, true)
		for _, stack := range strings.Split(string(raw[:n]), "\n\n") {
			if strings.Contains(stack, owner) && strings.Contains(stack, needle) {
				return true
			}
		}
		return false
	})
}
func TestTrustBridgeTrustLockWaitPrecedesPublication(t *testing.T) {
	s, d, _, _ := trustPublicationFixture(t)
	_, row := trustPublicationPeer(t)
	stamp := pinPublicationCapture(t, d)
	finished := make(chan error, 1)
	s.mu.Lock()
	unlocked, joined := false, false
	defer func() {
		if !unlocked {
			s.mu.Unlock()
		}
		if !joined {
			select {
			case <-finished:
			case <-time.After(2 * time.Second):
				t.Error("trust remember worker cleanup did not join")
			}
		}
	}()
	go trustPublicationRememberWorker(s, row, finished)
	trustPublicationWaitStack(t, "rememberContactOrigin", "sync.(*RWMutex).Lock")
	read, err := d.Consumer().TryReadClock(stamp)
	if err == nil {
		read.Release()
	}
	s.mu.Unlock()
	unlocked = true
	select {
	case e := <-finished:
		joined = true
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("trust remember worker did not join")
	}
	if err != nil {
		t.Fatal("writer held W before awaited trust.mu", err)
	}
}
func TestTrustBridgeReadReservationDelaysOnlyPostIOACK(t *testing.T) {
	s, d, _, _ := trustPublicationFixture(t)
	_, row := trustPublicationPeer(t)
	fact := trustPublicationApply(t, s, row)
	trustPublicationRequireStrict(t, s)
	factory := s.localFiles
	files := NewMocklocalContactDurableFiles(t)
	files.EXPECT().Preflight(mock.Anything).Return(nil).Once()
	files.EXPECT().Close().Return(nil).Once()
	wrote, finished := make(chan struct{}), make(chan error, 1)
	files.EXPECT().Write(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, raw []byte) (secretfile.DurableCommit, error) {
		real, err := factory()
		if err != nil {
			return secretfile.DurableNotCommitted, err
		}
		defer func() { _ = real.Close() }()
		out, err := real.Write(ctx, raw)
		close(wrote)
		return out, err
	}).Once()
	s.localFiles = func() (localContactDurableFiles, error) { return files, nil }
	stamp := pinPublicationCapture(t, d)
	read, err := d.Consumer().TryReadClock(stamp)
	if err != nil {
		t.Fatal(err)
	}
	released, joined := false, false
	defer func() {
		if !released {
			read.Release()
		}
		if !joined {
			select {
			case <-finished:
			case <-time.After(2 * time.Second):
				t.Error("trust ACK worker cleanup did not join")
			}
		}
	}()
	go trustPublicationConfirmWorker(s, fact, finished)
	select {
	case <-wrote:
	case <-time.After(2 * time.Second):
		t.Fatal("strict IO waited under publication W")
	}
	trustPublicationWaitStack(t, "confirmLocalContacts", "findnodebridge.Publisher.BeginPublication")
	select {
	case e := <-finished:
		joined = true
		t.Fatal("ACK completed while final R held", e)
	default:
	}
	// No trust/store wait or lookup occurs while the reservation R is held.
	read.Release()
	released = true
	select {
	case e := <-finished:
		joined = true
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("trust ACK writer did not join after R release")
	}
	if s.localContactStatus(fact) != domain.LocalContactFactDurableConfirmed || !pinPublicationRefused(d, stamp) {
		t.Fatal("post-IO ACK failed final memory publication")
	}
}
func TestTrustBridgeMandatoryForgetAfterPoisonAndClosedDone(t *testing.T) {
	s, d, clock, done := trustPublicationFixture(t)
	peer, row := trustPublicationPeer(t)
	if stored, err := s.remember(row); err != nil || !stored {
		t.Fatal(err)
	}
	clock.Store(5)
	observation, err := d.Observer().ReserveClock()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := observation.Sample(); err != nil {
		t.Fatal(err)
	}
	observation.Release()
	close(done)
	clock.Store(4)
	observation, err = d.Observer().ReserveClock()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := observation.Sample(); err == nil {
		t.Fatal("fixture failed poison")
	}
	observation.Release()
	if removed, err := s.forget(domain.PeerIdentityFromWire(peer.Address)); err != nil || !removed {
		t.Fatal("poison or Done vetoed mandatory forget", removed, err)
	}
	if _, present := s.trustedContacts()[row.Address]; present {
		t.Fatal("poison preserved forgotten row")
	}
	if _, err := d.Observer().BeginCapture(); err == nil {
		t.Fatal("trust cleanup revived poisoned observation")
	}
}
