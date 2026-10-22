package node

import (
	"context"
	"github.com/piratecash/corsa/internal/core/findnode"
	"github.com/piratecash/corsa/internal/core/findnodebridge"
	"testing"
	"time"
)

func ownDMRuntimeTestProfile() OwnDMRuntimeProfile {
	p := ownedCutProfile()
	return OwnDMRuntimeProfile{Residence: OwnDMResidenceProfile{MaxPrestage: 2 * time.Minute}, MaxActive: p.Capture.Degree.MaxActive, MaxMembers: p.Capture.Priority.MaxMembers, MaxRegistryVisits: p.Capture.Priority.MaxRegistryVisits, Intake: p.Intake, Signing: findnode.SigningProfile{NodeSpacing: 250 * time.Millisecond, MaxReservations: 4}}
}
func TestOwnDMRuntimeOneOriginalReadyServiceAndCancel(t *testing.T) {
	cfg, id := readyTestInputs(t)
	s, port, cancel, err := NewInactiveOwnDMRuntimeCandidate(cfg, id, nil, ownDMRuntimeTestProfile())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); s.WaitBackground() })
	if !port.ValidFor(s, id) || s.r2OwnDMOwner.r2Admission.resolver != s.identityResolver || s.r2OwnDMOwner.r2Admission.admission != s.identityResolver.recordAdmission || !s.identityResolver.recordAdmission.admitsR() {
		t.Fatal("R2 original binding differs")
	}
	if _, err := s.captureOwnedFindNodeCut(context.Background()); err != nil {
		t.Fatal("paired original Ready did not seal", err)
	}
	if s.runCtx.Done() != s.findNode.owned.done || s.secureSessions.store.pinPublisher != s.bridgeTerminal.publisher || s.trust.trustPublisher != s.bridgeTerminal.publisher {
		t.Fatal("second domain/store/lifecycle")
	}
	cancel()
	if s.runCtx.Err() == nil {
		t.Fatal("returned cancel is not original lifecycle")
	}
	if s.admitBackgroundJob() {
		s.backgroundWg.Done()
		t.Fatal("unstarted cleanup left original admission open")
	}
	// BeginCapture/ObservationClock may still supply inert historical evidence
	// after Done. New work is refused by the original commit Sample AFTER raw
	// clock/HWM; cancellation must not poison or revoke cleanup observation.
	stamp, err := s.bridgeCapture.observer.BeginCapture()
	if err != nil {
		t.Fatal("original historical observer unavailable", err)
	}
	admission, err := s.bridgeCapture.consumer.TryReadClock(stamp)
	if err != nil {
		t.Fatal("original commit role unavailable before Sample", err)
	}
	point, sampleErr := admission.Sample()
	admission.Release()
	if sampleErr == nil || point != (findnodebridge.OwnedInstant{}) {
		t.Fatal("canceled original admitted new work", sampleErr)
	}
	observation, err := s.bridgeCapture.observer.ReserveClock()
	if err != nil {
		t.Fatal("cleanup observation role unavailable", err)
	}
	_, observeErr := observation.Sample()
	observation.Release()
	if observeErr != nil {
		t.Fatal("Done refusal poisoned cleanup observation", observeErr)
	}
}
func TestOwnDMRuntimeCombinedFailureJoinsOriginalConstruction(t *testing.T) {
	for _, mode := range []string{"cancel", "panic", "foreign-consumer"} {
		t.Run(mode, func(t *testing.T) {
			cfg, id := readyTestInputs(t)
			r := readyTestPrepare(t, cfg, id)
			own := &ownDMRuntimeConstruction{profile: OwnDMResidenceProfile{MaxPrestage: time.Minute}}
			joined := make(chan struct{})
			callback := &readyInspectConstructor{t: t, r: r, mode: mode, joined: joined}
			panicked := false
			func() {
				defer func() {
					if v := recover(); v != nil {
						if v != "ready constructor callback panic" {
							panic(v)
						}
						panicked = true
					}
				}()
				s, err := r.buildWithOwnDM(cfg, id, nil, callback, own)
				if s != nil || err == nil {
					t.Fatal("failed paired construction escaped")
				}
			}()
			if panicked != (mode == "panic") || r.originalContext.Err() == nil || r.finalized {
				t.Fatal("paired failure lost original lifecycle")
			}
			select {
			case <-joined:
			default:
				t.Fatal("paired failure did not join callback owner")
			}
			if r.service == nil || r.service.goBackground(func() {}) {
				t.Fatal("paired failure left background admission open")
			}
		})
	}
}
func TestOwnDMRuntimeSnapshotRejectsReplacedPairedOwner(t *testing.T) {
	cfg, id := readyTestInputs(t)
	r := readyTestPrepare(t, cfg, id)
	own := &ownDMRuntimeConstruction{profile: OwnDMResidenceProfile{MaxPrestage: time.Minute}}
	s, err := r.buildWithOwnDM(cfg, id, nil, productionDatagramConstructor{}, own)
	if err != nil {
		t.Fatal(err)
	}
	if !r.bindings(s) {
		t.Fatal("original paired tuple refused")
	}
	original := *own
	own.port = OwnDMProvenancePort{}
	if r.bindings(s) {
		t.Fatal("replaced paired port accepted")
	}
	*own = original
	saved := s.r2OwnDMOwner
	s.r2OwnDMOwner = nil
	if r.bindings(s) {
		t.Fatal("replaced original owner accepted")
	}
	s.r2OwnDMOwner = saved
	if !r.bindings(s) {
		t.Fatal("restored same original tuple refused")
	}
}

func TestRuntimeUnstartedCleanupClosesOriginalGateBeforeWait(t *testing.T) {
	for _, kind := range []string{"normal", "paired"} {
		t.Run(kind, func(t *testing.T) {
			cfg, id := readyTestInputs(t)
			var s *Service
			var cleanup context.CancelFunc
			if kind == "normal" {
				s, cleanup = NewServiceForRuntime(cfg, id, nil)
			} else {
				var err error
				s, _, cleanup, err = NewInactiveOwnDMRuntimeCandidate(cfg, id, nil, ownDMRuntimeTestProfile())
				if err != nil {
					t.Fatal(err)
				}
			}
			defer cleanup()
			joined := make(chan struct{})
			if !s.goBackground(func() { <-s.runCtx.Done(); close(joined) }) {
				t.Fatal("original pre-start worker refused")
			}
			cleanup()
			// Inspect actual existing admission, not just context cancellation.
			if s.admitBackgroundJob() {
				s.backgroundWg.Done()
				t.Fatal("Add accepted after unstarted cleanup")
			}
			s.WaitBackground()
			select {
			case <-joined:
			default:
				t.Fatal("original callback not joined before store release")
			}
			if s.runCtx.Err() == nil {
				t.Fatal("cleanup canceled another context")
			}
			cleanup()
			if s.goBackground(func() { t.Error("post-cleanup callback executed") }) {
				t.Fatal("new callback admitted after Wait")
			}
		})
	}
}
