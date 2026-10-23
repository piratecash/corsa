package node

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/piratecash/corsa/internal/core/config"
	"github.com/piratecash/corsa/internal/core/datagram"
	"github.com/piratecash/corsa/internal/core/domain"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/identity"
	"github.com/piratecash/corsa/internal/core/protocol"
	"github.com/piratecash/corsa/internal/core/secretfile"
)

// The journal is installed by the existing test-only strict writer, then the
// candidate independently loads its actual format3 bytes into its own store.
func readyInboundInputs(t *testing.T) (config.Node, *identity.Identity, *ninFixtureClock) {
	t.Helper()
	cfg, id := readyTestInputs(t)
	cfg.EnableDatagramV1 = true
	cfg.ListenerSet, cfg.ListenerEnabled = true, true
	clock := new(ninFixtureClock)
	seed := ninOpenFixture(t, cfg.SecureSessionStorePath, clock)
	ninInstallVirginFixture(t, seed, ninFilesFixture(t, seed))
	return cfg, id, clock
}

func readyInboundBuild(t *testing.T, own bool) (*findNodeReadyConstruction, *Service, *ninFixtureClock) {
	t.Helper()
	cfg, id, clock := readyInboundInputs(t)
	r := readyTestPrepare(t, cfg, id)
	options := findNodeReadyOptions{loadedInbound: true}
	if own {
		options.ownDM = &ownDMRuntimeConstruction{profile: OwnDMResidenceProfile{MaxPrestage: time.Minute}}
	}
	s, err := r.buildWithOptions(cfg, id, nil, productionDatagramConstructor{}, options, inboundPinClock{elapsed: clock.read})
	if err != nil || s == nil {
		t.Fatal("loaded original journal did not compose Ready", err)
	}
	s.routeWithdrawalGracePeriodTest = -1
	if !r.bindings(s) || !r.finalized || !r.checked || !r.bridge.statisticInstalled || s.inboundPins == nil || s.inboundPins != r.originalInbound || s.inboundPins.service != s || s.inboundPins.store != r.store || s.inboundPins.wake != r.originalInboundWake {
		t.Fatal("composed producer/Service/store/wake differs from original")
	}
	store := s.localContactPinStore()
	store.mu.Lock()
	loaded, pending := store.ninFormat3 && store.ninJournal != nil, !store.ninJournal.strictConfirmed()
	store.mu.Unlock()
	if !loaded || !pending {
		t.Fatal("load was not actual format3 or falsely acknowledged startup obligation")
	}
	if own && (!options.ownDM.port.ValidFor(s, id) || options.ownDM.owner != s.r2OwnDMOwner || options.ownDM.resolver != s.identityResolver) {
		t.Fatal("own-DM did not retain same original Service/resolver")
	}
	// The original loaded-only worker first confirms the pending strict state.
	if err := s.inboundPins.pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	return r, s, clock
}

func TestOwnedReadyLoadedInboundOriginalSessionClassAndLocalAnswer(t *testing.T) {
	for _, own := range []bool{false, true} {
		name := "Ready-NIN"
		if own {
			name = "Ready-ownDM-NIN"
		}
		t.Run(name, func(t *testing.T) {
			_, s, clock := readyInboundBuild(t, own)
			cfg, id := readyTestInputs(t)
			remote, err := newFindNodeReadyCandidateService(cfg, id, nil, ownedCutProfile(), findnode.SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 4})
			if err != nil {
				t.Fatal(err)
			}
			remote.routeWithdrawalGracePeriodTest = -1
			t.Cleanup(func() { remote.runCancel(); remote.closeBackgroundAdmission(); remote.WaitBackground() })
			wire, original := ownedRawAccepted(t, s, remote)
			facts, err := s.collectOwnedFindNodeRawFacts(t.Context(), s.bridgeCapture.priority)
			if err != nil || len(facts.allOriginal) != 1 || facts.allOriginal[0] != original || facts.classes[0] != findnode.ClassOrdinary {
				t.Fatal("premature N-in class or different TLS original", facts, err)
			}
			// This changes only the existing private N-in elapsed fixture, not Runtime
			// clock/cut timestamps or proof. No ten-minute wall-clock sleep/evidence.
			clock.advance(10 * time.Minute)
			if err := s.inboundPins.pass(t.Context()); err != nil {
				t.Fatal(err)
			}
			facts, err = s.collectOwnedFindNodeRawFacts(t.Context(), s.bridgeCapture.priority)
			if err != nil || len(facts.allOriginal) != 1 || facts.allOriginal[0] != original || facts.classes[0] != findnode.ClassIncomingPinned {
				t.Fatal("genuine mature N-in did not classify same original", facts, err)
			}
			store := s.localContactPinStore()
			store.mu.Lock()
			count, confirmed := store.inboundAllocations, store.inboundConfirmed
			store.mu.Unlock()
			if count != 1 || confirmed != 1 {
				t.Fatal("N-in did not earn and strictly confirm exactly once", count, confirmed)
			}
			if _, err := s.captureOwnedFindNodeCut(context.Background()); err != nil {
				t.Fatal("original Runtime cut refused composed N-in", err)
			}
			request := ownedLocalNodeRequest(t, s)
			if err := s.dispatchOwnedFindNodeReceive(ownedLocalNodeReadRequest(t, s, wire, request)); err != nil {
				t.Fatal(err)
			}
			answer := ownedLocalNodeReadAnswer(t, wire)
			if err := protocol.VerifyFindNodeAnswer(answer, request, domain.NetworkID(networkName)); err != nil || answer.ID != request.ID {
				t.Fatal("original local T/sign/writer lost exact N-in request", err)
			}
			waitFor(t, 2*time.Second, "composed original local slot released", func() bool { return len(s.bridgeCapture.localLoop.slots) == 0 })
		})
	}
}

func TestOwnedReadyLoadedInboundIsExplicitAndProductionClockDefault(t *testing.T) {
	cfg, id, clock := readyInboundInputs(t)
	s, err := newFindNodeReadyLoadedInboundCandidateService(cfg, id, nil, ownedCutProfile(), findnode.SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 4})
	if err != nil || s == nil || s.inboundPins == nil {
		t.Fatal("private loaded-only production-clock factory refused", err)
	}
	t.Cleanup(func() { s.runCancel(); s.closeBackgroundAdmission(); s.WaitBackground() })
	store := s.localContactPinStore()
	if store.ninElapsed == nil {
		t.Fatal("original default trusted elapsed clock missing")
	}
	if _, err := store.ninElapsed(); err != nil {
		t.Fatal("original trusted elapsed source unavailable", err)
	}
	// The fixture clock is never supplied to this factory's store constructor.
	reads := clock.reads.Load()
	if err := s.inboundPins.pass(t.Context()); err != nil {
		t.Fatal(err)
	}
	if clock.reads.Load() != reads {
		t.Fatal("production factory retained fixture clock")
	}
	ordinary := NewService(cfg, id, nil)
	t.Cleanup(func() { ordinary.runCancel(); ordinary.closeBackgroundAdmission(); ordinary.WaitBackground() })
	if ordinary.inboundPins != nil {
		t.Fatal("ordinary construction activated loaded N-in")
	}
	plain := readyTestPrepare(t, cfg, id)
	ps, err := plain.build(cfg, id, nil, productionDatagramConstructor{})
	if err != nil || ps.inboundPins != nil {
		t.Fatal("old Ready branch activated N-in", err)
	}
	paired, _, cancel, err := NewInactiveOwnDMRuntimeCandidate(cfg, id, nil, ownDMRuntimeTestProfile())
	if err != nil || paired.inboundPins != nil {
		t.Fatal("old own-DM branch activated N-in", err)
	}
	t.Cleanup(cancel)
}

func TestOwnedReadyLoadedInboundRefusalDoesNotPublishOrMigrate(t *testing.T) {
	for _, kind := range []string{"missing", "format2", "invalid", "unsafe"} {
		t.Run(kind, func(t *testing.T) {
			cfg, id := readyTestInputs(t)
			var before []byte
			if kind == "invalid" {
				before = []byte("{invalid format3 journal")
			}
			if kind == "format2" {
				seed := ninOpenFixture(t, cfg.SecureSessionStorePath, new(ninFixtureClock))
				seed.mu.Lock()
				image := seed.snapshotLocked()
				seed.mu.Unlock()
				image.Format = 2
				var err error
				before, err = json.Marshal(image)
				if err != nil {
					t.Fatal(err)
				}
			}
			if before != nil {
				if err := os.WriteFile(cfg.SecureSessionStorePath, before, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "unsafe" {
				seed := ninOpenFixture(t, cfg.SecureSessionStorePath, new(ninFixtureClock))
				ninInstallVirginFixture(t, seed, ninFilesFixture(t, seed))
				var err error
				before, err = os.ReadFile(cfg.SecureSessionStorePath)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(cfg.SecureSessionStorePath, 0644); err != nil {
					t.Fatal(err)
				}
			}
			r := readyTestPrepare(t, cfg, id)
			escaped, buildErr := r.buildWithOptions(cfg, id, nil, productionDatagramConstructor{}, findNodeReadyOptions{loadedInbound: true})
			if escaped != nil || buildErr == nil || r.failure != buildErr || r.checked || r.finalized || r.bridge.statisticInstalled || r.originalContext.Err() == nil {
				t.Fatal("refused loaded journal escaped or lost exact failure", buildErr, r.failure)
			}
			switch kind {
			case "missing", "format2":
				if !errors.Is(buildErr, domain.InboundQuotaNotInstalled) {
					t.Fatal("loaded-only refusal lost original reason", buildErr)
				}
			case "invalid", "unsafe":
				var refusal *PinStoreRefusalError
				want := domain.PinStoreRefusalStoreUnreadable
				if kind == "unsafe" {
					want = domain.PinStoreRefusalLocationUnsafe
				}
				if !errors.Is(buildErr, domain.ErrPinStoreRefused) || !errors.As(buildErr, &refusal) || refusal.Reason != want {
					t.Fatal("original unsafe/unreadable startup reason masked", buildErr)
				}
			}
			if r.service != nil && r.service.goBackground(func() {}) {
				t.Fatal("refused construction left admission open")
			}
			after, err := os.ReadFile(cfg.SecureSessionStorePath)
			if kind == "missing" {
				if !errors.Is(err, os.ErrNotExist) {
					t.Fatal("refused candidate installed a journal", err)
				}
			} else if err != nil || string(after) != string(before) {
				t.Fatal("refused journal was migrated/replaced", err)
			}
		})
	}
}

func TestOwnedReadyLoadedInboundFinalizationRejectsTupleReplacement(t *testing.T) {
	cfg, id, clock := readyInboundInputs(t)
	r := readyTestPrepare(t, cfg, id)
	r.options = findNodeReadyOptions{loadedInbound: true}
	r.originalOptions = r.options
	s := newServiceWithConstruction(cfg, id, nil, nil, productionDatagramConstructor{}, serviceConstruction{bridge: r.bridge, ready: r}, inboundPinClock{elapsed: clock.read})
	if r.failure != nil || !r.checked || !r.bindings(s) || r.finalized {
		t.Fatal("original pre-finalize tuple missing", r.failure)
	}
	p := s.inboundPins
	service, store, wake := p.service, p.store, p.wake
	for _, kind := range []string{"producer", "service", "store", "wake", "request", "unrequested"} {
		switch kind {
		case "producer":
			s.inboundPins = &inboundPinProducer{service: s, store: store, wake: wake}
		case "service":
			p.service = &Service{}
		case "store":
			p.store = &secureSessionStore{}
		case "wake":
			p.wake = make(chan struct{}, 1)
		case "request":
			r.options.loadedInbound = false
		case "unrequested":
			r.options.loadedInbound = false
			r.originalOptions.loadedInbound = false
		}
		if r.bindings(s) || r.finalize(s) == nil || r.finalized {
			t.Fatal("replaced N-in tuple/request became Ready", kind)
		}
		s.inboundPins = p
		p.service, p.store, p.wake = service, store, wake
		r.options.loadedInbound = true
		r.originalOptions.loadedInbound = true
	}
	if err := r.finalize(s); err != nil || !r.bindings(s) {
		t.Fatal("original restored tuple refused", err)
	}
}

type readyInboundInspectConstructor struct {
	readyInspectConstructor
	replace bool
}

func (c *readyInboundInspectConstructor) Build(s *Service, m *datagram.Metrics) (*datagramLayer, error) {
	layer, err := c.readyInspectConstructor.Build(s, m)
	if c.replace {
		s.inboundPins = &inboundPinProducer{service: s, wake: make(chan struct{}, 1)}
	} // negative only, before original store exists
	return layer, err
}
func TestOwnedReadyLoadedInboundCallbackRefusalJoinsOriginal(t *testing.T) {
	for _, mode := range []string{"cancel", "panic", "replace"} {
		t.Run(mode, func(t *testing.T) {
			cfg, id, clock := readyInboundInputs(t)
			r := readyTestPrepare(t, cfg, id)
			joined := make(chan struct{})
			callback := &readyInboundInspectConstructor{readyInspectConstructor: readyInspectConstructor{t: t, r: r, mode: mode, joined: joined}, replace: mode == "replace"}
			var s *Service
			var err error
			var panicked any
			func() {
				defer func() { panicked = recover() }()
				s, err = r.buildWithOptions(cfg, id, nil, callback, findNodeReadyOptions{loadedInbound: true}, inboundPinClock{elapsed: clock.read})
			}()
			if s != nil || err == nil && panicked == nil || r.finalized || r.bridge.statisticInstalled || r.originalContext.Err() == nil {
				t.Fatal("failed N-in constructor escaped", err, panicked)
			}
			if mode == "panic" {
				if panicked != "ready constructor callback panic" {
					t.Fatal("wrong constructor panic cause", panicked)
				}
			} else if panicked != nil {
				t.Fatal("unexpected constructor panic", panicked)
			}
			select {
			case <-joined:
			default:
				t.Fatal("original callback worker not joined")
			}
			if r.service == nil || r.service.goBackground(func() {}) {
				t.Fatal("failed N-in constructor admission open")
			}
			if mode == "replace" && (!errors.Is(err, errNinJournal) || r.failure != err || r.checked) {
				t.Fatal("replacement attach refusal lost before statistic", err)
			}
			if mode == "cancel" && (!errors.Is(err, context.Canceled) || r.failure != err || r.checked) {
				t.Fatal("original cancellation was masked at loaded attach", err)
			}
		})
	}
}

func TestOwnedReadyLoadedInboundOriginalRunWorkerJoins(t *testing.T) {
	_, s, clock := readyInboundBuild(t, true)
	cfg, id := readyTestInputs(t)
	dialer := NewService(cfg, id, nil)
	t.Cleanup(func() { dialer.runCancel(); dialer.closeBackgroundAdmission(); dialer.WaitBackground() })
	f := n1AcceptedOnService(t, n1ServiceFixture{service: s, dialer: dialer, clock: clock})
	n1AssertOriginalRunProducerJoin(t, f)
}

// Same real format3 journal and original guard writer as ordinary recovery.
// No in-memory recovery flag, offline recovery operation or Unknown installer.
func TestOwnedReadyLoadedInboundRecoveryGuardRefusesBeforeSeal(t *testing.T) {
	cfg, id, clock := readyInboundInputs(t)
	seed := ninOpenFixture(t, cfg.SecureSessionStorePath, clock)
	peer := domain.PeerIdentity{21}
	if err := seed.noteProvenInbound(peer); err != nil {
		t.Fatal(err)
	}
	preview := pinRemovalPreview(t, seed, peer)
	seed.mu.Lock()
	entry := seed.identities[peer]
	format3 := seed.ninFormat3 && seed.ninJournal != nil
	seed.mu.Unlock()
	if !format3 {
		t.Fatal("recovery fixture lost actually loaded format3")
	}
	backup, err := domain.NewIdentityPinBackup(peer, entry.pinnedAt, entry.classMetadata())
	if err != nil {
		t.Fatal(err)
	}
	binding := domain.IdentityPinFenceBinding{Scope: domain.IdentityPinStoreIdentity{Node: domain.PeerIdentityFromWire(id.Address), Store: "ready-nin-guard"}, RuntimeEpoch: "ready-nin-process", Generation: 1, Identity: peer, ProofRevision: domain.IdentityPinProofRevision(entry.evidenceRevision), PreviousPinnedAt: entry.pinnedAt, Backup: backup}
	seed.mu.Lock()
	bindingErr := seed.checkIdentityPinRemovalLocked(binding, preview.Token)
	seed.mu.Unlock()
	if bindingErr != nil {
		t.Fatal("original mandatory pin binding/token invalid", bindingErr)
	}
	permit := domain.IdentityPinRemovalPermit{OperationID: "ready-nin-original-guard", Binding: binding, Generation: 2}
	raw, err := json.Marshal(identityPinGuardRecord{Format: identityPinGuardFormat, Permit: permit})
	if err != nil {
		t.Fatal(err)
	}
	files := ninFilesFixture(t, seed)
	if outcome, err := files.WriteGuard(t.Context(), raw); err != nil || outcome != secretfile.DurableCommitted {
		t.Fatal("original guard write not committed", outcome, err)
	}
	beforeGuard, err := files.ReadGuard(t.Context())
	if err != nil || string(beforeGuard) != string(raw) {
		t.Fatal("original guard bytes unavailable", err)
	}
	beforeMain, err := os.ReadFile(cfg.SecureSessionStorePath)
	if err != nil {
		t.Fatal(err)
	}
	r := readyTestPrepare(t, cfg, id)
	escaped, err := r.buildWithOptions(cfg, id, nil, productionDatagramConstructor{}, findNodeReadyOptions{loadedInbound: true}, inboundPinClock{elapsed: clock.read})
	var refusal *PinStoreRefusalError
	if escaped != nil || !errors.Is(err, domain.ErrIdentityPinRecoveryRequired) || !errors.Is(err, domain.ErrPinStoreRefused) || !errors.As(err, &refusal) || refusal.Reason != domain.PinStoreRefusalIdentityPinRecovery || r.failure != err {
		t.Fatal("original recovery refusal masked or usable Ready escaped", err, r.failure)
	}
	if r.checked || r.finalized || r.bridge.statisticInstalled || r.originalContext.Err() == nil {
		t.Fatal("recovery-required constructor reached Ready/statistic")
	}
	if _, err := r.originalCapture.observer.BeginCapture(); err == nil {
		t.Fatal("recovery-required constructor sealed domain")
	}
	if r.service == nil || r.service.goBackground(func() {}) {
		t.Fatal("recovery refusal did not close original admission")
	}
	afterGuard, guardErr := files.ReadGuard(t.Context())
	afterMain, mainErr := os.ReadFile(cfg.SecureSessionStorePath)
	if guardErr != nil || mainErr != nil || string(afterGuard) != string(beforeGuard) || string(afterMain) != string(beforeMain) {
		t.Fatal("refused Ready changed/cleared guard or main bytes", guardErr, mainErr)
	}
}
